package main

import (
	"context"
	"encoding/json"
	"net"

	pb "github.com/srikrsna/flock/cmd/client/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logger ...
type Logger interface {
	Infof(string, ...interface{})
	Error(...interface{})
	Errorf(string, ...interface{})
	Info(...interface{})
	Sync() error
}

// Server ...
type Server struct {
	Logger Logger
}

// Ping ...
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	defer s.Logger.Sync()
	switch v := req.Value.(type) {
	case *pb.PingRequest_Server:
		if err := pingServer(ctx, v.Server.Ip); err != nil {
			s.Logger.Errorf("Failed to ping server: ", err)
			return nil, err
		}
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ClientDB:
		db, err := flockSQL.ConnectDB(v.ClientDB.Url, v.ClientDB.Database)
		if err != nil {
			s.Logger.Errorf("Failed to ping the client database: %v", err)
			return nil, err
		}
		db.Close()
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ServerDB:
		base, err := pingServerDatabase(ctx, v.ServerDB.Server.Ip, v.ServerDB.Url, v.ServerDB.Database)
		if err != nil {
			s.Logger.Errorf("Failed to ping server database: %v", err)
			return nil, err
		}
		return &pb.PingResponse{Schema: base}, nil
	default:
		s.Logger.Errorf("might be a version mis match unknown message type received: %T", req.Value)
		return nil, status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", req.Value)
	}
}

// SchemaTest ...
func (s *Server) SchemaTest(ctx context.Context, req *pb.SchemaFile) (*pb.SchemaResponse, error) {
	defer s.Logger.Sync()
	params, err := testSchema(req.File)
	if err != nil {
		s.Logger.Errorf("failed to parse schema: %v", err)
		return nil, err
	}
	return &pb.SchemaResponse{Params: params}, nil
}

// Plugin ...
func (s *Server) Plugin(ctx context.Context, req *pb.PluginRequest) (*pb.PluginResponse, error) {
	defer s.Logger.Sync()
	if err := testPlugin(req.Plugin); err != nil {
		s.Logger.Errorf("plugin test failed: %v", err)
		return nil, err
	}
	return &pb.PluginResponse{}, nil
}

// Report ...
func (s *Server) Report(req *pb.ReportRequest, srv pb.UI_ReportServer) error {
	defer s.Logger.Sync()
	progChan := make(chan progress)

	// Unpack the params from json to a map
	params := make(map[string]interface{})
	if err := json.Unmarshal(req.Params, &params); err != nil {
		s.Logger.Errorf("failed to parse params: %v", err)
		return err
	}

	if err := runFlockClient(req.Server.Ip, req.ClientDB.Url, req.ClientDB.Database, req.ServerDB.Url, req.ServerDB.Database, req.Dollar, req.Flock, req.Plugin, params, progChan); err != nil {
		s.Logger.Errorf("failed to transfer data: %v", err)
		return err
	}
	for v := range progChan {
		if err := srv.Send(&pb.ReportResponse{Chunks: int64(v.chunks), Tables: int64(v.tables), Percentage: int64(v.percentage * 100)}); err != nil {
			s.Logger.Errorf("unable to send progress report to UI: %v", err)
			return err
		}
	}
	return nil
}

func runUIServer() error {
	l, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	log := l.Sugar()
	// TODO : Add syncs and tweak the logger

	srv := &Server{Logger: log}

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
