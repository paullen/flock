package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/elgris/sqrl"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"go.uber.org/zap"
	_ "gocloud.dev/blob/gcsblob" // Presuming this is the GCS blob driver
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	gob.Register(&time.Time{})
}

// Logger is the interface of the Error Logger, Most loggers satisfy the interface Printf including the standard library logger
type Logger interface {
	Info(string, ...zap.Field)
	Error(string, ...zap.Field)
	Sync() error
}

// DB ...
type DB interface {
	sqrl.ExecerContext
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryRow(string, ...interface{}) *sql.Row
}

var chunkMap = &sync.Map{}

type errorHandle struct {
	err  error
	lock *sync.Mutex
}

// Server ....
type Server struct {
	Logger Logger
}

// To check whether it conforms to the interface
var _ pb.FlockServer = (*Server)(nil)

// Health ...
func (s *Server) Health(ctx context.Context, in *pb.Ping) (*pb.Pong, error) {
	return &pb.Pong{}, nil
}

// DatabaseHealth ...
func (s *Server) DatabaseHealth(ctx context.Context, in *pb.DBPing) (*pb.DBPong, error) {
	db, err := flockSQL.ConnectDB(in.Url, in.Database)
	if err != nil {
		s.Logger.Error("failed to connect to database", zap.String("error", err.Error()))
		return nil, err
	}
	defer db.Close()

	info, err := flockSQL.GetSchema(ctx, db)
	if err != nil {
		s.Logger.Error("failed to generate base flock", zap.String("error", err.Error()))
		return nil, err
	}

	base, err := generateBase(info)
	if err != nil {
		s.Logger.Error("failed to generate base", zap.String("error", err.Error()))
		return nil, err
	}

	return &pb.DBPong{Schema: base}, nil
}

// Flock ...
func (s *Server) Flock(ch pb.Flock_FlockServer) error {
	var next pb.FlockRequest
	var inError = errorHandle{nil, &sync.Mutex{}}
	var db DB
	var p sqrl.PlaceholderFormat
	var tables map[string]flock.Table
	var params map[string]map[string]flock.Variable

	if err := ch.RecvMsg(&next); err != nil {
		return err
	}

	// Prepare the server for the Flock process
	switch v := next.Value.(type) {
	case *pb.FlockRequest_Start:
		// To resolve recreation of db in this scope
		var err error

		db, err = flockSQL.ConnectDB(v.Start.Url, v.Start.Database)
		if err != nil {
			s.Logger.Error("failed to connect to database", zap.String("error", err.Error()))
			return err
		}
		fl, err := flock.ParseSchema(bytes.NewBuffer(v.Start.Schema))
		if err != nil {
			s.Logger.Error("failed to build tables", zap.String("error", err.Error()))
			return err
		}

		tables, params = flock.BuildTables(fl)

		plugins, err := flock.PluginHandler(v.Start.Plugin)
		if err != nil {
			s.Logger.Error("failed to build plugin", zap.String("error", err.Error()))
			return err
		}

		flock.RegisterFunc(plugins)

		// TODO : Fill URL
		// url := ""

		// b, err := blob.OpenBucket(ch.Context(), url)
		// if err != nil {
		// 	s.Logger.Error("failed to open bucket for url", zap.String("url", url), zap.String("error", err.Error()))
		// 	return err
		// }

		// for name := range plugins {
		// 	wr, err := b.NewWriter(ch.Context(), name, nil)
		// 	if err != nil {
		// 		s.Logger.Error("failed to create a writer to the blob", zap.String("key", name), zap.String("error", err.Error()))
		// 		return err
		// 	}
		// 	if _, err := wr.Write(v.Start.Plugin); err != nil {
		// 		s.Logger.Error("failed to write to blob", zap.String("key", name), zap.String("error", err.Error()))
		// 		return err
		// 	}
		// 	if err := wr.Close(); err != nil {
		// 		s.Logger.Error("failed to write to blob", zap.String("key", name), zap.String("error", err.Error()))
		// 		return err
		// 	}
		// }

		if v.Start.Dollar == true {
			p = sqrl.Dollar
		} else {
			p = sqrl.Question
		}

		if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Pong{Pong: &pb.Pong{}}}); err != nil {
			s.Logger.Error("failed to send start response", zap.String("error", err.Error()))
			return err
		}

	default:
		s.Logger.Error("might be a version mis match unknown message type received", zap.String("type", fmt.Sprintf("%T", next.Value)))
		return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
	}

	// Implementation for a single user
	// To iterate over the multiple users wrap the below code in a for loop
	tx, err := db.BeginTx(ch.Context(), nil)
	if err != nil {
		s.Logger.Error("unable to create transaction", zap.String("error", err.Error()))
		return err
	}
	defer tx.Rollback()

	for {
		inError.lock.Lock()
		if inError.err != nil {
			s.Logger.Error("failed to process stream")
			return inError.err
		}
		inError.lock.Unlock()

		if err := ch.RecvMsg(&next); err != nil {
			return err
		}

		switch v := next.Value.(type) {
		case *pb.FlockRequest_Ping:
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Pong{}}); err != nil {
				s.Logger.Error("unable to send echo message", zap.String("error", err.Error()))
				return err
			}
		case *pb.FlockRequest_Batch:
			if v == nil || v.Batch == nil {
				return status.Errorf(codes.InvalidArgument, "nil batch request")
			}
			switch batch := v.Batch.Value.(type) {
			case *pb.Batch_Head:
				var tempChannel = make(chan *pb.DataStream)

				chunkMap.Store(batch.Head.BatchId, tempChannel)

				// Passing channel to ease access from locks and nextRequest to not store it
				go func(channel chan *pb.DataStream, nextRequest *pb.BatchInsertHead, inerror *errorHandle) {
					receivedChunks := make([]*pb.DataStream, 0)
					for v := range channel {
						receivedChunks = append(receivedChunks, v)
						if int64(len(receivedChunks)) == nextRequest.Chunks {
							close(channel)
							chunkMap.Delete(nextRequest.BatchId)
						}
					}
					sort.SliceStable(receivedChunks, func(i, j int) bool {
						return receivedChunks[i].Index < receivedChunks[j].Index
					})
					var data = make([]byte, 0)
					for _, v := range receivedChunks {
						data = append(data, v.GetData()...)
					}

					res, err := handleBatch(ch.Context(), tx, tables, nextRequest, data, p, params)
					if err != nil {
						s.Logger.Error("unable to handle batch insert request", zap.String("error", err.Error()))
						inerror.lock.Lock()
						inerror.err = err
						inerror.lock.Unlock()
						return
					}
					s.Logger.Info("successfully inserted chunk", zap.String("table", nextRequest.TableName), zap.String("batch", nextRequest.BatchId))
					if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: res}}); err != nil {
						s.Logger.Error("unable to send batch insert response", zap.String("error", err.Error()))
						inerror.lock.Lock()
						inerror.err = err
						inerror.lock.Unlock()
						return
					}
				}(tempChannel, batch.Head, &inError)
			case *pb.Batch_Chunk:

				value, ok := chunkMap.Load(batch.Chunk.BatchId)
				if !ok {
					s.Logger.Error("unidentified stream. Please send BatchInserHead before beginning a stream")
					return errors.New("stream not found")
				}
				value.(chan *pb.DataStream) <- batch.Chunk
			case *pb.Batch_Tail:
				// _, ok := chunkMap.Load(batch.Tail.BatchId)
				// if !ok {
				// 	s.Logger.Error("unidentified stream. Please send BatchInserHead before beginning a stream")
				// 	return errors.New("stream not found")
				// }

				// close(value.(chan *pb.DataStream))

				// chunkMap.Delete(batch.Tail.BatchId)

			default:
				s.Logger.Error("might be a version mis match unknown message type received", zap.String("type", fmt.Sprintf("%T", v.Batch.Value)))
				return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", v.Batch.Value)
			}
		case *pb.FlockRequest_End:
			if err := tx.Commit(); err != nil {
				s.Logger.Error("could not commit the transaction", zap.String("error", err.Error()))
				return err
			}
			ok, err := handleVerification(db, tables, v.End.Records)
			if err != nil {
				if ok {
					s.Logger.Error("number of inserted records don't match number of queried records", zap.String("info", err.Error()))
					return err
				}
				s.Logger.Error("inserted records could not be retreived", zap.String("error", err.Error()))
				return err
			}
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: &pb.BatchInsertResponse{Success: true}}}); err != nil {
				s.Logger.Error("COMMIT SUCCESSFUL but unable to send echo message", zap.String("error", err.Error()))
				return err
			}
			return nil
		default:
			s.Logger.Error("might be a version mis match unknown message type received", zap.String("type", fmt.Sprintf("%T", next.Value)))
			return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
		}

		// if err := s.Logger.Sync(); err != nil {
		// 	return fmt.Errorf("Failed to sync log: %v", err)
		// }
	}
}
