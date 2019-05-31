package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/google/uuid"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"google.golang.org/grpc"
)

func init() {
	gob.Register(&time.Time{})
}

type progress struct {
	chunks     int
	tables     int
	percentage float64
}

var gobLimit = 60000 // Gob data limit in bytes
var rowLimit = 100   // Number of rows that will be sent at a time

func main() {

	// FOR FUTURE REFERENCE
	// query := url.Values{}
	// query.Add("database", *database)

	// u := &url.URL{
	// 	Scheme:   *databaseServer,
	// 	User:     url.UserPassword(*user, *pass),
	// 	Host:     fmt.Sprintf("%s:%d", *host, port),
	// 	Path:     *path,
	// 	RawQuery: query.Encode(),
	// }

	// cli, err := ConnClient("23.251.141.168:50051")
	// if err ! nil {
	// 	return nil
	// }

	if err := runUIServer(); err != nil {
		fmt.Printf("UI server terminated: %v", err)
	}

}

// Functions implementing the relay functionality between the UI and the server

func runFlockClient(serverIP, clientURL, clientDB, serverURL, serverDB string, dollar bool, plugin, schema []byte, params map[string]interface{}, ch chan progress) error {

	// Connect to flock server
	conn, err := grpc.Dial(serverIP, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	// Connect to the client database
	db, err := flockSQL.ConnectDB(clientURL, clientDB)
	if err != nil {
		return err
	}
	defer db.Close()

	cli := pb.NewFlockClient(conn)

	// Receive the client-side stream of the Flock RPC
	fcli, err := cli.Flock(context.Background())
	if err != nil {
		return err
	}

	start := time.Now()

	if err := fcli.Send(&pb.FlockRequest{
		Value: &pb.FlockRequest_Start{
			Start: &pb.Start{
				Url:      serverURL,
				Database: serverDB,
				Dollar:   dollar,
				Schema:   schema,
				Plugin:   plugin,
			}}}); err != nil {
		return err
	}

	if _, err := fcli.Recv(); err != nil {
		return err
	}

	// Get User Specified Query
	fl, err := flock.ParseSchema(bytes.NewReader(schema))
	if err != nil {
		return err
	}

	// Get total number of tables to calculate percentage
	numTables := len(fl.Entries)

	// Iterating over all the tables
	for t, v := range fl.Entries {

		query, args := parseQuery(v.Query, params)

		data, err := flockSQL.GetData(context.Background(), db, query, args)
		if err != nil {
			return err
		}

		startRow := 0
		rowChunks := len(data) / rowLimit
		if len(data)%rowLimit > 0 {
			rowChunks++
		}
		// Iterating over all row chunks
		for i := 0; i < rowChunks; i++ {
			lenRow := rowLimit
			if startRow+lenRow > len(data) {
				lenRow = len(data) - startRow
			}
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(data[startRow:(startRow + lenRow)]); err != nil {
				return err
			}
			complete := buf.Bytes()
			startChunk := 0
			chunks := int64(len(complete) / gobLimit)
			if len(complete)%gobLimit > 0 {
				chunks++
			}

			// Generate UUID for the row chunk
			batchID := uuid.New()

			// Sending the head of a data stream
			if err := fcli.Send(&pb.FlockRequest{
				Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch{
						BatchId: batchID.String(),
						Value: &pb.Batch_Head{
							Head: &pb.BatchInsertHead{
								TableName: v.Name,
								Chunks:    chunks,
							},
						},
					},
				},
			}); err != nil {
				return err
			}

			// Sending chunks of data stream
			for i := int64(0); i < chunks; i++ {
				lenChunk := gobLimit
				if startChunk+lenChunk >= len(complete) {
					lenChunk = len(complete) - startChunk
				}

				if err := fcli.Send(&pb.FlockRequest{
					Value: &pb.FlockRequest_Batch{
						Batch: &pb.Batch{
							BatchId: batchID.String(),
							Value: &pb.Batch_Chunk{
								Chunk: &pb.DataStream{
									Data: complete[startChunk:(startChunk + lenChunk)],
								},
							},
						},
					},
				}); err != nil {
					return err
				}
				startChunk += lenChunk
			}

			// Sending the tail of a data stream
			if err := fcli.Send(&pb.FlockRequest{
				Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch{
						BatchId: batchID.String(),
						Value: &pb.Batch_Tail{
							Tail: &pb.BatchInsertTail{},
						},
					},
				},
			}); err != nil {
				return err
			}

			res, err := fcli.Recv()
			if err != nil {
				return err
			}

			log.Println(time.Since(start))
			log.Println(res)
			//Update the UI server of the progress
			ch <- progress{i + 1, t + 1, (float64(t+1) / float64(numTables))}
			startRow += lenRow
		}
	}
	if err = fcli.Send(&pb.FlockRequest{Value: &pb.FlockRequest_End{}}); err != nil {
		return err
	}
	res, err := fcli.Recv()
	if err != nil {
		return err
	}
	log.Println(res)
	return nil
}

func pingServer(ctx context.Context, serverIP string) error {
	conn, err := grpc.Dial(serverIP, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := pb.NewFlockClient(conn)

	_, err = cli.Health(ctx, &pb.Ping{})
	if err != nil {
		return err
	}
	return nil
}

func pingServerDatabase(ctx context.Context, serverIP, url, database string) ([]byte, error) {
	conn, err := grpc.Dial(serverIP, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := pb.NewFlockClient(conn)

	res, err := cli.DatabaseHealth(ctx, &pb.DBPing{Url: url, Database: database})
	if err != nil {
		return nil, err
	}
	return res.Schema, nil
}
