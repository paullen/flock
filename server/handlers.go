package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

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
		return nil, fmt.Errorf("failed to insert chunk. Table: %s\nData: %v\nError: %v", req.GetTableName(), rows, err)
	}

	return &pb.BatchInsertResponse{Success: true}, nil
}

func generateBase(info map[string]([]string)) ([]byte, error) {

	var buf bytes.Buffer

	for k, v := range info {
		if _, err := buf.WriteString(fmt.Sprintf("%s {\n\t``\n\t{\n", k)); err != nil {
			return nil, err
		}
		for _, v := range v {
			if _, err := buf.WriteString(fmt.Sprintf("\t\t%s = \n", v)); err != nil {
				return nil, err
			}
		}
		if _, err := buf.WriteString(fmt.Sprintf("\t}\n}\n")); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
