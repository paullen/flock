package flock_test

import (
	"fmt"
	"os"
	"reflect"
	"bytes"
	"encoding/gob"
	"testing"
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	flock "github.com/srikrsna/flock/pkg"
	flockSQL "github.com/srikrsna/flock/sql"
)

func TestGetData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("Error making a mock database.")
	}
	defer db.Close()

	rows := mock.NewRows([]string{"Rid", "AppointyID", "OrderId", "UserId", "FeedbackRating", "Comment", "feedbackDate", "adminReply", "Promote_Facebook", "AppointmentID"}).
		AddRow(24, 46, 45, 0, 0, 0, 0, 0, 0, 0).
		AddRow(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).
		AddRow(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

	mock.ExpectQuery("^SELECT (.+) FROM Feedbacks ORDER BY (.+) DESC;$").WillReturnRows(rows)

	f, err := os.Open("schema_test.fl")
	if err != nil {
		t.Errorf("Could not open given schema file.")
	}
	defer f.Close()

	fl, err := flock.ParseSchema(f)
	if err != nil {
		t.Errorf("Failed to parse given schema file")
	}

	data, err := flockSQL.GetData(context.Background(), db, fl.Entries[0].Query)

	fmt.Println("Fetched Data-----------------------------")
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		t.Errorf("Gob Encoding failed")
	}
	fmt.Println(reflect.TypeOf(buf.Bytes()[0]))
	fmt.Println("-----------------------------------------")
	//t.Logf(data)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetDataFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("Error making a mock database.")
	}
	defer db.Close()

	mock.ExpectQuery("^SELECT (.+) FROM Feedbacks ORDER BY (.+) DESC;$").WillReturnError(fmt.Errorf("Failed to fetch data"))

	f, err := os.Open("schema_test.fl")
	if err != nil {
		t.Errorf("Could not open given schema file.")
	}
	defer f.Close()

	fl, err := flock.ParseSchema(f)
	if err != nil {
		t.Errorf("Failed to parse given schema file")
	}

	data, err := flockSQL.GetData(context.Background(), db, fl.Entries[0].Query)

	fmt.Println("Fetched Data-----------------------------")
	fmt.Println(data)
	fmt.Println("-----------------------------------------")
	//t.Logf(data)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}