package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"

	"github.com/elgris/sqrl"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
)

func handleBatch(ctx context.Context, db sqrl.ExecerContext, tables map[string]flock.Table, req *pb.BatchInsertHead, data []byte, format sqrl.PlaceholderFormat) (*pb.BatchInsertResponse, error) {
	var rows []map[string]interface{}

	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&rows); err != nil {
		return nil, err
	}

	table, ok := tables[req.GetTableName()]
	if !ok {
		return nil, errors.New("table not configured")
	}

	if err := flock.InsertBulk(ctx, db, rows, table, req.GetTableName(), format); err != nil {
		return nil, err
	}

	return &pb.BatchInsertResponse{Success: true}, nil
}
