package server

import (
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"sort"
	"time"

	//"github.com/elgris/sqrl"

	"github.com/elgris/sqrl"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
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

//DB ...
type DB interface {
	sqrl.ExecerContext
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Server ....
type Server struct {
	DB     DB
	Logger Logger
	Tables map[string]flock.Table
}

// To check whether it conforms to the interface
var _ pb.FlockServer = (*Server)(nil)

// Flock ...
func (s *Server) Flock(ch pb.Flock_FlockServer) error {
	var next pb.FlockRequest
	var nextRequest *pb.BatchInsertHead
	var receivedChunks = make([]*pb.DataStream, 0)
	var chunks int64
	var streaming = false
	var endStream = false

	//Implementation for a single user
	//To iterate over a database wrap the below code in another for loop
	tx, err := s.DB.BeginTx(ch.Context(), nil)
	if err != nil {
		s.Logger.Printf("unable to create transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	for {
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
				if streaming {
					s.Logger.Printf("Error: Data streaming in progress. Can't handle new request.")
					return errors.New("unresolved data stream")
				}
				nextRequest = batch.Head
				chunks = int64(batch.Head.Chunks)
				streaming = true
			case *pb.Batch_Chunk:
				receivedChunks = append(receivedChunks, batch.Chunk)
				if int64(len(receivedChunks)) == chunks && endStream {
					streaming = false
				}
			case *pb.Batch_Tail:
				endStream = true
			default:
				s.Logger.Printf("might be a version mis match unknown message type received: %T", v.Batch.Value)
				return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", v.Batch.Value)
			}
		case *pb.FlockRequest_End:
			if streaming || endStream {
				s.Logger.Printf("End signal sent mid-stream")
				return errors.New("End request could not be processed")
			}
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: &pb.BatchInsertResponse{Success: true}}}); err != nil {
				s.Logger.Printf("unable to send echo message: %T", err)
				return err
			}
			if err := tx.Commit(); err != nil {
				s.Logger.Printf("could not commit the transaction: %v", err)
				return err
			}
			return nil
		default:
			s.Logger.Printf("might be a version mis match unknown message type received: %T", next.Value)
			return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
		}

		if !streaming && endStream {
			sort.SliceStable(receivedChunks, func(i, j int) bool {
				return receivedChunks[i].Index < receivedChunks[j].Index
			})
			var data = make([]byte, 0)
			for _, v := range receivedChunks {
				data = append(data, v.GetData()...)
			}
			res, err := handleBatch(ch.Context(), tx, s.Tables, nextRequest, data)
			if err != nil {
				s.Logger.Printf("unable to handle batch insert request: %v", err)
				return err
			}
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: res}}); err != nil {
				s.Logger.Printf("unable to send batch insert response: %v", err)
				return err
			}
			chunks = 0
			receivedChunks = receivedChunks[:0]
			nextRequest = nil
			endStream = false
		}
	}
}
