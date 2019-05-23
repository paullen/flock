package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"database/sql"
	"errors"
	"github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
)


func handleBatch(ctx context.Context, db *sql.DB, tables map[string]flock.Table, req *pb.BatchInsertHead, data []byte) (*pb.BatchInsertResponse, error) {
	var rows []map[string]interface{}

	// if err := gob.NewDecoder(bytes.NewReader(req.GetData())).Decode(&rows); err != nil {
	// 	return nil, err
	// }

	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&rows); err != nil {
		return nil, err
	}

	table, ok := tables[req.GetTable()]
	if !ok {
		return nil, errors.New("table not configured")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := flock.InsertBulk(ctx, tx, rows, table, req.GetTableName()); err != nil {
		return nil, err
	}

	tx.Commit()

	return &pb.BatchInsertResponse{Success: true}, nil
}
