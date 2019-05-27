package main

import (
	pb "../protos"
)

type Logger interface {
	Printf(string, ...interface{})
}

type server struct {
	Logger Logger
}

func (s *server) Ping(ch *pb.UI_PingServer) error {
	var next *pb.PingRequest

	for {
		if err := ch.RecvMsg(&next); err != nil {
			s.Logger.Printf("Failed to receive message: ", err)
		}
	}
}

func (s *server) Report(ch *pb.UI_ReportServer) error {

}