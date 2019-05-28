package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"

	pb "github.com/srikrsna/flock/cmd/client/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logger ...
type Logger interface {
	Printf(string, ...interface{})
}

// Server ...
type Server struct {
	Logger Logger
}

// Ping ...
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	switch v := req.Value.(type) {
	case *pb.PingRequest_Server:
		if err := pingServer(ctx, v.Server.Ip); err != nil {
			s.Logger.Printf("Failed to ping server: ", err)
			return nil, err
		}
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ClientDB:
		db, err := flockSQL.ConnectDB(v.ClientDB.Url, v.ClientDB.Database)
		if err != nil {
			s.Logger.Printf("Failed to ping the client database: %v", err)
			return nil, err
		}
		db.Close()
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ServerDB:
		if err := pingServerDatabase(ctx, v.ServerDB.Server.Ip, v.ServerDB.Url, v.ServerDB.Database); err != nil {
			s.Logger.Printf("Failed to ping server database: %v", err)
			return nil, err
		}
		return &pb.PingResponse{}, nil
	default:
		s.Logger.Printf("might be a version mis match unknown message type received: %T", req.Value)
		return nil, status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", req.Value)
	}
}

// SchemaTest ...
func (s *Server) SchemaTest(ctx context.Context, req *pb.SchemaFile) (*pb.SchemaResponse, error) {
	file := bytes.NewReader(req.File)
	params, err := testSchema(file)
	if err != nil {
		s.Logger.Printf("failed to parse schema: %v", err)
		return nil, err
	}
	return &pb.SchemaResponse{Params: params}, nil
}

// Report ...
func (s *Server) Report(req *pb.ReportRequest, srv pb.UI_ReportServer) error {

	progChan := make(chan progress)

	// TODO : Receive map from protobuf and send it to runFlockClient
	if err := runFlockClient(req.Server.Ip, req.ClientDB.Url, req.ClientDB.Database, req.ServerDB.Url, req.ServerDB.Database, req.Dollar, req.Flock, req.Plugin, progChan); err != nil {
		s.Logger.Printf("failed to transfer data: %v", err)
		return err
	}
	for v := range progChan {
		if err := srv.Send(&pb.ReportResponse{Chunks: int64(v.chunks), Tables: int64(v.tables), Percentage: int64(v.percentage * 100)}); err != nil {
			s.Logger.Printf("unable to send progress report to UI: %v", err)
			return err
		}
	}
	return nil
}

func runUIServer() error {
	l := log.New(os.Stderr, "", 0)

	srv := &Server{Logger: l}

	s := grpc.NewServer()
	pb.RegisterUIServer(s, srv)

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		return err
	}
	defer lis.Close()

	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}
