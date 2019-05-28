package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/elgris/sqrl"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	gob.Register(&time.Time{})
}

// Logger is the interface of the Error Logger, Most loggers satisfy the interface Printf including the standard library logger
type Logger interface {
	Printf(string, ...interface{})
}

// DB ...
type DB interface {
	sqrl.ExecerContext
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
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

//DatabaseHealth ...
func (s *Server) DatabaseHealth(ctx context.Context, in *pb.DBPing) (*pb.Pong, error) {
	db, err := flockSQL.ConnectDB(in.Url, in.Database)
	if err != nil {
		return nil, err
	}
	db.Close()
	return &pb.Pong{}, nil
}

// Flock ...
func (s *Server) Flock(ch pb.Flock_FlockServer) error {
	var next pb.FlockRequest
	var inError = errorHandle{nil, &sync.Mutex{}}
	var db DB
	var p sqrl.PlaceholderFormat
	var tables map[string]flock.Table

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
			s.Logger.Printf("failed to connect to database: %v", err)
			return err
		}

		fl, err := flock.ParseSchema(bytes.NewBuffer(v.Start.GetSchema()))
		if err != nil {
			s.Logger.Printf("failed to build tables: %v", err)
			return err
		}

		tables = flock.BuildTables(fl)

		if v.Start.Dollar == true {
			p = sqrl.Dollar
		} else {
			p = sqrl.Question
		}
	default:
		s.Logger.Printf("might be a version mis match unknown message type received: %T", next.Value)
		return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
	}

	// Implementation for a single user
	// To iterate over a database wrap the below code in another for loop
	tx, err := db.BeginTx(ch.Context(), nil)
	if err != nil {
		s.Logger.Printf("unable to create transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	for {
		inError.lock.Lock()
		if inError.err != nil {
			s.Logger.Printf("Failed to process stream")
			return inError.err
		}
		inError.lock.Unlock()

		if err := ch.RecvMsg(&next); err != nil {
			return err
		}

		switch v := next.Value.(type) {
		case *pb.FlockRequest_Ping:
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Pong{}}); err != nil {
				s.Logger.Printf("unable to send echo message: %T", err)
				return err
			}
		case *pb.FlockRequest_Batch:
			if v == nil || v.Batch == nil {
				return status.Errorf(codes.InvalidArgument, "nil batch request")
			}
			switch batch := v.Batch.Value.(type) {
			case *pb.Batch_Head:
				var tempChannel = make(chan *pb.DataStream, batch.Head.Chunks+1)

				chunkMap.Store(v.Batch.BatchId, tempChannel)

				// Passing channel to ease access from locks and nextRequest to not store it
				go func(channel chan *pb.DataStream, nextRequest *pb.BatchInsertHead, inerror *errorHandle) {
					var receivedChunks = make([]*pb.DataStream, 0)
					for v := range channel {
						receivedChunks = append(receivedChunks, v)
					}
					sort.SliceStable(receivedChunks, func(i, j int) bool {
						return receivedChunks[i].Index < receivedChunks[j].Index
					})
					var data = make([]byte, 0)
					for _, v := range receivedChunks {
						data = append(data, v.GetData()...)
					}
					// TODO : Pass placeholder in context
					res, err := handleBatch(ch.Context(), tx, tables, nextRequest, data, p)
					if err != nil {
						s.Logger.Printf("unable to handle batch insert request: %v", err)
						inerror.lock.Lock()
						inerror.err = err
						inerror.lock.Unlock()
						return
					}
					if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: res}}); err != nil {
						s.Logger.Printf("unable to send batch insert response: %v", err)
						inerror.lock.Lock()
						inerror.err = err
						inerror.lock.Unlock()
						return
					}
				}(tempChannel, batch.Head, &inError)
			case *pb.Batch_Chunk:

				value, ok := chunkMap.Load(v.Batch.BatchId)
				if !ok {
					s.Logger.Printf("unidentified stream. Please send BatchInserHead before beginning a stream")
					return errors.New("stream not found")
				}
				value.(chan *pb.DataStream) <- batch.Chunk
			case *pb.Batch_Tail:
				value, ok := chunkMap.Load(v.Batch.BatchId)
				if !ok {
					s.Logger.Printf("unidentified stream. Please send BatchInserHead before beginning a stream")
					return errors.New("stream not found")
				}
				// TODO : Close channel when all chunks are delivered
				close(value.(chan *pb.DataStream))

				chunkMap.Delete(v.Batch.BatchId)

			default:
				s.Logger.Printf("might be a version mis match unknown message type received: %T", v.Batch.Value)
				return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", v.Batch.Value)
			}
		case *pb.FlockRequest_End:
			if err := tx.Commit(); err != nil {
				s.Logger.Printf("could not commit the transaction: %v", err)
				return err
			}
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: &pb.BatchInsertResponse{Success: true}}}); err != nil {
				s.Logger.Printf("COMMIT SUCCESSFUL but unable to send echo message: %T", err)
				return err
			}
			return nil
		default:
			s.Logger.Printf("might be a version mis match unknown message type received: %T", next.Value)
			return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
		}
	}
}
