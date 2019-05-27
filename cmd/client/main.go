package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"google.golang.org/grpc"
)

func init() {
	gob.Register(&time.Time{})
}

var user = flag.String("u", "", "Username")
var pass = flag.String("p", "", "Password")
var host = flag.String("h", "", "Host")
var portString = flag.String("pn", "", "Port Number")
var database = flag.String("d", "", "Database")
var path = flag.String("r", "", "Path")
var databaseServer = flag.String("ds", "", "Database Server")
var schemaPath = flag.String("s", "", "path to your schema file")

var gobLimit = 60000 // Gob data limit in bytes
var rowLimit = 100   // Number of rows that will be sent at a time

func main() {
	log.SetFlags(0)
	flag.Parse()
	// queryPlaceholder := os.Args[1:]
	port, err := strconv.Atoi(*portString)
	if err != nil {
		log.Fatalln(err)
		return
	}

	//Get database connection string
	query := url.Values{}
	query.Add("database", *database)

	u := &url.URL{
		Scheme:   *databaseServer,
		User:     url.UserPassword(*user, *pass),
		Host:     fmt.Sprintf("%s:%d", *host, port),
		Path:     *path,
		RawQuery: query.Encode(),
	}

	//Connect to databsse
	db, err := flockSQL.ConnectDB(u, *databaseServer)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	if err := runClient(db); err != nil {
		log.Fatalln(err)
		return
	}

}

func runClient(db *sql.DB) error {

	conn, err := grpc.Dial("23.251.141.168:50051", grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := pb.NewFlockClient(conn)

	fcli, err := cli.Flock(context.Background())
	if err != nil {
		return err
	}
	start := time.Now()
	f, err := os.Open(*schemaPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get User Specified Query
	fl, err := flock.ParseSchema(f)
	if err != nil {
		return err
	}

	// TODO : Fill this with the named params passed by user
	var params = make(map[string]interface{})

	// Iterating over all the tables
	for _, v := range fl.Entries {

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
			offset := false
			chunks := int64(len(complete) / gobLimit)
			if len(complete)%gobLimit > 0 {
				chunks++
			}
			// Sending the head of a data stream
			if err := fcli.Send(&pb.FlockRequest{
				Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch{
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
			for !offset {
				lenChunk := gobLimit
				if startChunk+lenChunk >= len(complete) {
					lenChunk = len(complete) - startChunk
					offset = true
				}

				if err := fcli.Send(&pb.FlockRequest{
					Value: &pb.FlockRequest_Batch{
						Batch: &pb.Batch{
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
