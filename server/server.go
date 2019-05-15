package server

import (
	"encoding/gob"
	"github.com/elgris/sqrl"
	"github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
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

			res, err := handleBatch(ch.Context(), s.DB, s.Tables, v.Batch)
			if err != nil {
				s.Logger.Printf("unable to handle batch insert request: %v", err)
				return err
			}
			if err := ch.Send(&pb.FlockResponse{Value: &pb.FlockResponse_Batch{Batch: res}}); err != nil {
				s.Logger.Printf("unable to send batch insert response: %v", err)
				return err
			}
		default:
			s.Logger.Printf("might be a version mis match unknown message type received: %T", next.Value)
			return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
		}
	}
}
