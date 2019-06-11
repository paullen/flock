package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"math"
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

var records = make(map[string]int)

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
		fmt.Printf("UI server terminated: %v\n", err)
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
		records[v.Name] = len(data)
		i := 0
		// Iterating over all row chunks
		for len(data) > 0 {
			var buf bytes.Buffer
			var tempData []map[string]interface{}
			if rowLimit <= len(data) {
				tempData = data[0:rowLimit]
				data = data[rowLimit:]
			} else {
				tempData = data
				data = data[len(data):]
			}

			if err := gob.NewEncoder(&buf).Encode(tempData); err != nil {
				return err
			}
			complete := buf.Bytes()
			chunks := int64(math.Ceil(float64(len(complete)) / float64(gobLimit)))

			// Generate UUID for the row chunk
			batchID := uuid.New()

			// Sending the head of a data stream
			if err := fcli.Send(&pb.FlockRequest{
				Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch{
						Value: &pb.Batch_Head{
							Head: &pb.BatchInsertHead{
								BatchId:   batchID.String(),
								TableName: v.Name,
								Chunks:    chunks,
							},
						},
					},
				},
			}); err != nil {
				return err
			}
			index := int64(1)
			// Sending chunks of data stream
			for len(complete) > 0 {
				var tempComplete []byte
				if gobLimit <= len(complete) {
					tempComplete = complete[0:gobLimit]
					complete = complete[gobLimit:]
				} else {
					tempComplete = complete
					complete = complete[len(complete):]
				}

				if err := fcli.Send(&pb.FlockRequest{
					Value: &pb.FlockRequest_Batch{
						Batch: &pb.Batch{
							Value: &pb.Batch_Chunk{
								Chunk: &pb.DataStream{
									BatchId: batchID.String(),
									Index:   index,
									Data:    tempComplete,
								},
							},
						},
					},
				}); err != nil {
					return err
				}
				index++
			}

			// Sending the tail of a data stream
			if err := fcli.Send(&pb.FlockRequest{
				Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch{
						Value: &pb.Batch_Tail{
							Tail: &pb.BatchInsertTail{BatchId: batchID.String()},
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
			i++
			ch <- progress{i + 1, t + 1, (float64(t+1) / float64(numTables))}
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(records); err != nil {
		return err
	}
	if err = fcli.Send(&pb.FlockRequest{Value: &pb.FlockRequest_End{End: &pb.EndStream{Records: buf.Bytes()}}}); err != nil {
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
