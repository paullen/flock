package main

import (
	"bytes"
	"context"
	"strconv"
	"database/sql"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
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

var limit = 60000                 //Gob data limit in bytes

//var dbPath = flag.String("d", "", "path to your config file")

func main() {
	log.SetFlags(0)
	flag.Parse()

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

	//Get User Specified Query

	fl, err := flock.ParseSchema(f)
	if err != nil {
		return err
	}

	// TODO : Iterate over all the tables

	data, err := flockSQL.GetData(context.Background(), db, fl.Entries[0].Query)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return err
	}

	if err := fcli.Send(&pb.FlockRequest{
		Value: &pb.FlockRequest_Batch{
				Batch: &pb.Batch{
					Value: &pb.Batch_Head {
						Head: &pb.BatchInsertHead {
							Table: "",      //TODO: FILL
							TableName: "", 	//TODO: FILL
							Chunks: 0,      //TODO: FILL
							},
						},
				},
			},
		}); err != nil {
		return err
	}

	complete := buf.Bytes()
	startChunk := 0
	lenChunk := limit
	offset := false

	for !offset {
		if startChunk + lenChunk >= len(complete) {
			lenChunk = len(complete) - startChunk
			offset = true
		}

		if err := fcli.Send(&pb.FlockRequest{
			Value: &pb.FlockRequest_Batch{
					Batch: &pb.Batch {
						Value: &pb.Batch_Chunk {
							Chunk: &pb.DataStream {
								Data: complete[startChunk:startChunk + lenChunk],
							},
						},
					},
				},
			}); err != nil {
			return err
		}
		startChunk += lenChunk
	}

	if err := fcli.Send(&pb.FlockRequest{
		Value: &pb.FlockRequest_Batch{
			Batch: &pb.Batch {
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

	return nil
}

