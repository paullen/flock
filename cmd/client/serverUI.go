package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/rs/cors"
	pb "github.com/srikrsna/flock/cmd/client/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Logger ...
type Logger interface {
	Info(string, ...zap.Field)
	Error(string, ...zap.Field)
	Sync() error
}

// Server ...
type Server struct {
	Logger Logger
}

// Ping ...
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	// defer s.Logger.Sync()
	switch v := req.Value.(type) {
	case *pb.PingRequest_Server:
		if err := pingServer(ctx, v.Server.Ip); err != nil {
			s.Logger.Error("Failed to ping server", zap.String("error", err.Error()))
			return nil, err
		}
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ClientDB:
		db, err := flockSQL.ConnectDB(v.ClientDB.Url, v.ClientDB.Database)
		if err != nil {
			s.Logger.Error("Failed to ping the client database", zap.String("error", err.Error()))
			return nil, err
		}
		db.Close()
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ServerDB:
		base, err := pingServerDatabase(ctx, v.ServerDB.Server.Ip, v.ServerDB.Url, v.ServerDB.Database)
		if err != nil {
			s.Logger.Error("Failed to ping server database", zap.String("error", err.Error()))
			return nil, err
		}
		return &pb.PingResponse{Schema: base}, nil
	default:
		s.Logger.Error("might be a version mis match unknown message type received", zap.String("type", fmt.Sprintf("%T", req.Value)))
		return nil, status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", req.Value)
	}
}

// SchemaTest ...
func (s *Server) SchemaTest(ctx context.Context, req *pb.SchemaFile) (*pb.SchemaResponse, error) {
	// defer s.Logger.Sync()
	params, err := testSchema(req.File)
	if err != nil {
		s.Logger.Error("failed to parse schema", zap.String("error", err.Error()))
		return nil, err
	}
	return &pb.SchemaResponse{Params: params}, nil
}

// Plugin ...
func (s *Server) Plugin(ctx context.Context, req *pb.PluginRequest) (*pb.PluginResponse, error) {
	// defer s.Logger.Sync()
	if err := testPlugin(req.Plugin); err != nil {
		s.Logger.Error("plugin test failed", zap.String("error", err.Error()))
		return nil, err
	}
	return &pb.PluginResponse{}, nil
}

// Report ...
func (s *Server) Report(req *pb.ReportRequest, srv pb.UI_ReportServer) error {
	// defer s.Logger.Sync()
	progChan := make(chan progress)

	// Unpack the params from json to a map
	params := make(map[string]interface{})
	if err := json.Unmarshal(req.Params, &params); err != nil {
		s.Logger.Error("failed to parse params", zap.String("error", err.Error()))
		return err
	}
	go func() {
		for v := range progChan {
			if err := srv.Send(&pb.ReportResponse{Chunks: int64(v.chunks), Tables: int64(v.tables), Percentage: int64(v.percentage * 100)}); err != nil {
				s.Logger.Error("unable to send progress report to UI", zap.String("error", err.Error()))
				return
			}
			s.Logger.Info("Progress Update", zap.Duration("time", v.execTime), zap.Any("status", v.res))
		}
	}()

	if err := runFlockClient(req.Server.Ip, req.ClientDB.Url, req.ClientDB.Database, req.ServerDB.Url, req.ServerDB.Database, req.Dollar, req.Flock, req.Plugin, params, progChan); err != nil {
		s.Logger.Error("failed to transfer data", zap.String("error", err.Error()))
		return err
	}

	return nil
}

func runUIServer() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	// TODO : Add syncs and tweak the logger

	srv := &Server{Logger: log}

	s := grpc.NewServer()
	pb.RegisterUIServer(s, srv)

	ws := grpcweb.WrapServer(s, grpcweb.WithOriginFunc(func(string) bool { return true }))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if ws.IsGrpcWebRequest(r) {
			ws.HandleGrpcWebRequest(w, r)
			return
		}

		fmt.Println("not a grpcweb request")
	})

	fmt.Println("Server Running...")
	err = http.ListenAndServe(":8082", cors.AllowAll().Handler(http.DefaultServeMux))
	return err
}
