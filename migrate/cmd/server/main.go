package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	"github.com/dgraph-io/badger"
	"github.com/elgris/sqrl"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	flockSQL "github.com/srikrsna/flock/sql"
	"github.com/srikrsna/flock/server"
	"google.golang.org/grpc"
)

var db *badger.DB

var schemaPath = flag.String("I", "", "path to your schema file")
var user = flag.String("u", "", "Username")
var pass = flag.String("p", "", "Password")
var host = flag.String("h", "", "Host")
var portString = flag.String("pn", "", "Port Number")
var database = flag.String("d", "", "Database")
var path = flag.String("r", "", "Path")
var databaseServer = flag.String("ds", "", "Database Server")

func main() {
	log.SetFlags(0)
	flag.Parse()
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	var err error
	db, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	srv, err := makeServer()
	if err != nil {
		log.Fatalln(err)
	}

	flock.RegisterFunc(flock.FuncMap{
		"ToGuid": inMemory(),
	})

	s := grpc.NewServer()
	pb.RegisterFlockServer(s, srv)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()

	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func makeServer() (*server.Server, error) {
	l := log.New(os.Stderr, "", 0)

	tables, err := makeTables(*schemaPath)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(*portString)
	if err != nil {
		return nil, err
	}

	fmt.Println(tables)
	
	u := &url.URL{
		Scheme:   *databaseServer,
		User:     url.UserPassword(*user, *pass),
		Host:     fmt.Sprintf("%s:%d", *host, port),
		Path:     *database,
		RawQuery: fmt.Sprintf("sslmode=%s&connect_timeout=%d", "disable", 3),
	}

	db, err := flockSQL.ConnectDB(u, *databaseServer)
	if err != nil {
		return nil, err
	}

	flock.RegisterFunc(flock.FuncMap{
		"Nil": Nil,
	})

	return &server.Server{
		Logger: l,
		Tables: tables,
		DB:     db,
	}, nil
}

func inMemory() func(table string, oldID int64) (string, error) {
	f := func(table string, oldID int64) (string, error) {
		key := table + strconv.FormatInt(oldID, 10)

		id, ok := keys[key]
		if ok {
			return id, nil
		}

		id = uuid.Must(uuid.NewUUID()).String()

		keys[key] = id

		return id, nil
	}

	return f
}

func bGuid(db *badger.DB) func(table string, oldID int64) (string, error) {
	f := func(table string, oldID int64) (string, error) {
		key := []byte(table + strconv.FormatInt(oldID, 10))
		tx := db.NewTransaction(true)
		defer tx.Commit(nil)

		i, err := tx.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				ui := uuid.Must(uuid.NewUUID())
				if err := tx.Set(key, ui[:]); err != nil {
					return "", err
				}

				return ui.String(), nil
			}
		}

		ui, err := i.Value()
		if err != nil {
			return "", err
		}

		return string(ui), nil
	}

	return f
}

var keys = map[string]string{}

func toGuid(db *sql.DB) func(table string, oldID int64) (string, error) {
	f := func(table string, oldID int64) (string, error) {
		key := table + strconv.FormatInt(oldID, 10)
		id, ok := keys[key]
		if ok {
			return id, nil
		}

		err := sqrl.Select("new").From("guid." + table).Where(sqrl.Eq{"old": oldID}).PlaceholderFormat(sqrl.Dollar).RunWith(db).QueryRow().Scan(&id)
		if err == nil {
			keys[key] = id
			return id, nil
		}

		if err != sql.ErrNoRows {
			return "", err
		}

		id = uuid.Must(uuid.NewUUID()).String()
		if _, err := sqrl.Insert("guid."+table).Columns("old", "new").Values(oldID, id).PlaceholderFormat(sqrl.Dollar).RunWith(db).Exec(); err != nil {
			return "", err
		}

		keys[key] = id

		return id, nil
	}

	return f
}

func Nil(a interface{}, b interface{}) interface{} {
	if b == nil {
		return a
	}

	return b
}

func makeTables(path string) (map[string]flock.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fl, err := flock.ParseSchema(f)
	if err != nil {
		return nil, err
	}

	return flock.BuildTables(fl), nil
}

// func connectDB(user, pass, host, database string) (*sql.DB, error) {
// 	connUrl := url.URL{
// 		Scheme:   "postgres",
// 		User:     url.UserPassword(user, pass),
// 		Host:     host,
// 		Path:     database,
// 		RawQuery: fmt.Sprintf("sslmode=%s&connect_timeout=%d", "disable", 3),
// 	}

// 	db, err := sql.Open("postgres", connUrl.String())
// 	if err != nil {
// 		return nil, err
// 	}

// 	if err := db.Ping(); err != nil {
// 		return nil, err
// 	}

// 	return db, nil
// }
