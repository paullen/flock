package flock_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/elgris/sqrl"
	flock "github.com/srikrsna/flock/pkg"
)

func TestInsertBulk(t *testing.T) {

	columns := map[string]flock.Column{"First": {"one", []flock.Func{}}, "Second": {"two", []flock.Func{}}, "Third": {"three", []flock.Func{}}}

	table := flock.Table{"Random", columns, []string{"First", "Second", "Third"}}

	rows := make([]map[string]interface{}, 4)

	rows[0] = map[string]interface{}{"one": 1, "two": 2, "three": 3}
	rows[1] = map[string]interface{}{"one": 12, "two": 22, "three": 32}
	rows[2] = map[string]interface{}{"one": 31, "two": 32, "three": 33}
	rows[3] = map[string]interface{}{"one": 123, "two": 223, "three": 323}

	tests := []struct {
		name  string
		limit int
	}{
		{"Test-0", 0},
		{"Test-1", 1},
		{"Test-2", 2},
		{"Test-3", 3},
		{"Test-4", 4},
		{"Test-5", 6},
		{"Test-6", -32},
	}

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Errorf("Error in creating SQL mock")
			}
			defer db.Close()

			t.Logf(v.name)
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Errorf("Failed to create a transaction")
			}

			if err = flock.SetLimit(v.limit); err != nil {
				if v.limit <= 0 {
					t.Log("Error:", err)
					return
				} else {
					t.Fatal(err)
				}
			}

			num := len(rows) / v.limit
			if len(rows)%v.limit > 0 {
				num++
			}
			t.Logf("No of statements: %d", num)
			for i := 0; i < num; i++ {
				mock.ExpectExec("INSERT INTO ").WillReturnResult(sqlmock.NewResult(1, 1))
			}

			if err := flock.InsertBulk(context.Background(), tx, rows, table, "Random", sqrl.Dollar, nil); err != nil {
				t.Errorf("Couldn't insert data: %v", err)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Unfulfilled Expectations")
			}
		})
	}

}
