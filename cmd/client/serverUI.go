package main

import (
	"status"
	
	pb "github.com/srikrsna/flock/cmd/client/UIproto"
	flockSQL "github.com/srikrsna/flock/sql"
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
		if _, err := flockSQL.ConnectDB(v.ClientDB.Url, v.ClientDB.Database); err != nil {
			s.Logger.Printf("Failed to ping the client database: %v", err)
			return nil, err
		}
		return &pb.PingResponse{}, nil
	case *pb.PingRequest_ServerDB:
		if err := pingServerDatabase(ctx, v.ServerDB.Server.Ip, v.ServerDB.Url, v.ServerDB.Database); err != nil {
			s.Loggerf.Printf("Failed to ping server database: %v", err)
		}
		return &pb.PingResponse{}, nil
	default:
		s.Logger.Printf("might be a version mis match unknown message type received: %T", next.Value)
		return status.Errorf(codes.Unimplemented, "must be version mismatch unknown message type: %T", next.Value)
	}
}

// SchemaTest ... 
func SchemaTest(ctx context.Context, req *pb.SchemaFile) (*pb.SchemaResponse, error) {
	file := bytes.NewReader(req.file)
	params, err := testSchema(file)
	if err != nil {
		return nil, err
	}
	return &pb.SchemaResponse{Params: params}, nil
}

// Report ...
func (s *server) Report(ch *pb.UI_ReportServer) error {

}

func runUIServer() {
}