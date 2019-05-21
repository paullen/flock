package flock_test

import (
	"fmt"
	"testing"
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	flock "github.com/srikrsna/flock/pkg"
	//flockSQL "github.com/srikrsna/flock/sql"
)

func TestInsert(t * testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("Error in creating SQL mock")
	}
	defer db.Close()

	//mock.ExpectBegin()
	
	//mock.ExpectQuery()
	//mock.ExpectCommit()

	columns := map[string]flock.Column{"First":{"one", []flock.Func{}}, "Second":{"two", []flock.Func{}}, "Third":{"three", []flock.Func{}}}

	table := flock.Table{"Random", columns, []string{"First", "Second", "Third"}}


	rows := make([]map[string]interface{}, 4)

	rows[0] = map[string]interface{}{"one": 1, "two": 2, "three": 3}
	
	rows[1] = map[string]interface{}{"one": 12, "two": 22, "three": 32}
	rows[2] = map[string]interface{}{"one": 31, "two": 32, "three": 33}
	
	rows[3] = map[string]interface{}{"one": 123, "two": 223, "three": 323}

	mock.ExpectExec("INSERT INTO ").WillReturnResult(sqlmock.NewResult(1,1))
	//mock.ExpectQuery("SELECT * FROM Random").WillReturnRows(&rows)

	//fmt.Println(rows)
	
	flock.SetLimit(1)
	
	if err := flock.InsertBulk(context.Background(), db, rows, table, "Random"); err != nil {
		fmt.Println(err)
		t.Errorf("Couldn't insert data")
	}


	//fmt.Println(rows)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled Expectations")
	}
}