package flock

import (
	"context"
	"github.com/elgris/sqrl"
	"reflect"
)

func InsertBulk(ctx context.Context, db sqrl.ExecerContext, rows []map[string]interface{}, table Table, tableName string) error {
	return insertBulk(ctx, db, rows, table, tableName, funcMap)
}

func insertBulk(ctx context.Context, db sqrl.ExecerContext, rows []map[string]interface{}, table Table, tableName string, funcMap map[string]reflect.Value) error {
	inst := BuildInsertStatement(table, tableName, sqrl.Dollar)

	for _, row := range rows {
		data, err := CalculateValuesOfRow(row, table, funcMap)
		if err != nil {
			return err
		}

		inst = inst.Values(data...)
	}

	query, args, err := inst.ToSql()
	if err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return err
	}

	return nil
}

func CalculateValuesOfRow(row map[string]interface{}, table Table, funcMap map[string]reflect.Value) ([]interface{}, error) {
	data := make([]interface{}, 0, len(row))
	for rk, rv := range row {
		for _, col := range table.Keys {
			if col.Value == rk {
				i := reflect.ValueOf(rv)
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
				break
			}
		}
	}
	return data, nil
}

func BuildInsertStatement(table Table, tableName string, format sqrl.PlaceholderFormat) *sqrl.InsertBuilder {
	cols := make([]string, 0, len(table.Keys))
	for i := range table.Keys {
		cols = append(cols, i)
	}

	if tableName == "" {
		tableName = table.Name
	}

	return sqrl.Insert(tableName).PlaceholderFormat(format).Columns(cols...)
}

func BuildSingleInsertQuery(table Table, tableName string, format sqrl.PlaceholderFormat) (string, error) {
	query, _, err := BuildInsertStatement(table, tableName, format).
		Values(make([]interface{}, len(table.Keys))).
		ToSql()

	return query, err
}
