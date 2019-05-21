package flock

import (
	"context"
	"fmt"
	"errors"
	"reflect"
	"time"
	"database/sql"
	"github.com/elgris/sqrl"
)

var sqlLimit int = 1000

func InsertBulk(ctx context.Context, tx *sql.Tx, rows []map[string]interface{}, table Table, tableName string) error {
	return insertBulk(ctx, tx, rows, table, tableName, funcMap)
}

func insertBulk(ctx context.Context, tx *sql.Tx, rows []map[string]interface{}, table Table, tableName string, funcMap map[string]reflect.Value) error {
	start := time.Now()
	totalRows := len(rows)
	currentRow := 0
	for {

		count := 0

		inst := BuildInsertStatement(table, tableName, sqrl.Dollar)

		for _, row := range rows[currentRow:] {
			data, err := CalculateValuesOfRow(row, table, funcMap)
			if err != nil {
				return err
			}

			inst = inst.Values(data...)

			if count++; count == sqlLimit {
				break
			}
		}

		query, args, err := inst.ToSql()
		if err != nil {
			return err
		}

		fmt.Println(query,"\n",args)
		v, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		fmt.Println(v)

		if currentRow += count; currentRow >= totalRows {
			break
		}
	}
	fmt.Println(time.Since(start))
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func CalculateValuesOfRow(row map[string]interface{}, table Table, funcMap map[string]reflect.Value) ([]interface{}, error) {
	data := make([]interface{}, 0, len(row))

	for _, key := range table.Ordered {
		col := table.Keys[key]
		rv := row[col.Value]

		var i reflect.Value
		if rv != nil {
			i = reflect.ValueOf(rv)
		} else {
			i = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())
		}

		for _, f := range col.Functions {
			in := append(f.Parameters, i)

			rt := funcMap[f.Name].Call(in)
			if len(rt) == 1 {
				i = rt[0]
			}

			if len(rt) == 2 {
				if !rt[1].IsNil() {
					return nil, rt[1].Interface().(error) // This should be checked before hand
				}

				i = rt[0]
			}
		}
		data = append(data, i.Interface())
	}

	return data, nil
}

func BuildInsertStatement(table Table, tableName string, format sqrl.PlaceholderFormat) *sqrl.InsertBuilder {
	cols := make([]string, 0, len(table.Keys))
	for _, key := range table.Ordered {
		cols = append(cols, key)
	}

	if tableName == "" {
		tableName = table.Name
	}

	return sqrl.Insert(tableName).PlaceholderFormat(format).Columns(cols...).Suffix("ON CONFLICT DO NOTHING")
}

func BuildSingleInsertQuery(table Table, tableName string, format sqrl.PlaceholderFormat) (string, error) {
	query, _, err := BuildInsertStatement(table, tableName, format).
		Values(make([]interface{}, len(table.Keys))).
		ToSql()

	return query, err
}

func SetLimit(in int) (error) {
	if in >= 0 {
		sqlLimit = in
	} else {
		return errors.New("Limit needs to be an integer greater than 0.")
	}
	return nil
}

func GetLimit() (int) {
	return sqlLimit
}