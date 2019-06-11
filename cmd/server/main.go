package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	"github.com/dgraph-io/badger"
	"github.com/elgris/sqrl"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	flock "github.com/srikrsna/flock/pkg"
	pb "github.com/srikrsna/flock/protos"
	"github.com/srikrsna/flock/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var db *badger.DB

func main() {
	log.SetFlags(0)
	flag.Parse()

	// opts := badger.DefaultOptions
	// opts.Dir = "/tmp/badger"
	// opts.ValueDir = "/tmp/badger"
	// var err error
	// db, err = badger.Open(opts)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer db.Close()

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

	fmt.Println("Running server...")
	if err := s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func makeServer() (*server.Server, error) {

	// FOR FUTURE REFERENCE
	// u := &url.URL{
	// 	Scheme:   *databaseServer,
	// 	User:     url.UserPassword(*user, *pass),
	// 	Host:     fmt.Sprintf("%s:%d", *host, port),
	// 	Path:     *database,
	// 	RawQuery: fmt.Sprintf("sslmode=%s&connect_timeout=%d", "disable", 3),
	// }

	flock.RegisterFunc(flock.FuncMap{
		"Nil": Nil,
	})

	log, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	// TODO : Add syncs and tweak the logger

	return &server.Server{
		Logger: log,
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

// Nil ...
func Nil(a interface{}, b interface{}) interface{} {
	if b == nil {
		return a
	}

	return b
}
