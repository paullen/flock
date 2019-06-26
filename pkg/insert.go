package flock

import (
	"context"
	"errors"
	"reflect"

	"github.com/elgris/sqrl"
)

var sqlLimit = 1000

// InsertBulk ...
func InsertBulk(ctx context.Context, db sqrl.ExecerContext, rows []map[string]interface{}, table Table, tableName string, format sqrl.PlaceholderFormat, varFields map[string]Variable) error {
	for sqlLimit < len(rows) {
		if err := insertBulk(ctx, db, rows[0:sqlLimit], table, tableName, funcMap, format, varFields); err != nil {
			return err
		}
		rows = rows[sqlLimit:]
	}

	return insertBulk(ctx, db, rows, table, tableName, funcMap, format, varFields)
}

func insertBulk(ctx context.Context, db sqrl.ExecerContext, rows []map[string]interface{}, table Table, tableName string, funcMap map[string]reflect.Value, format sqrl.PlaceholderFormat, varFields map[string]Variable) error {
	inst := BuildInsertStatement(table, tableName, format)
	for _, row := range rows {
		data, err := CalculateValuesOfRow(row, table, funcMap, varFields)
		if err != nil {
			return err
		}

		inst = inst.Values(data...)
	}

	query, args, err := inst.ToSql()
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, query, args...)
	return err
}

// CalculateValuesOfRow ...
func CalculateValuesOfRow(row map[string]interface{}, table Table, funcMap map[string]reflect.Value, varFields map[string]Variable) ([]interface{}, error) {
	data := make([]interface{}, 0, len(row))
	for _, key := range table.Ordered {
		col := table.Keys[key]
		rv := row[col.Value]
		v := varFields[col.Value]
		var i reflect.Value
		if rv != nil {
			i = reflect.ValueOf(rv)
		} else {
			i = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())
		}

		for _, f := range col.Functions {
			if v.Func == f.Name {
				for i, k := range v.index {
					f.Parameters[k] = reflect.ValueOf(row[v.Column[i]])
				}
			}
			in := append(f.Parameters, i)

			rt := funcMap[f.Name].Call(in)
			if len(rt) == 1 {
				i = rt[0]
			}

			if len(rt) == 2 {
				if !rt[1].IsNil() {
					return nil, rt[1].Interface().(error) // The check for error on 2nd return is done by goodFunc()
				}

				i = rt[0]
			}
		}
		data = append(data, i.Interface())
	}

	return data, nil
}

// BuildInsertStatement ...
func BuildInsertStatement(table Table, tableName string, format sqrl.PlaceholderFormat) *sqrl.InsertBuilder {
	cols := make([]string, 0, len(table.Keys))
	for _, key := range table.Ordered {
		cols = append(cols, key)
	}

	return sqrl.Insert(tableName).PlaceholderFormat(format).Columns(cols...).Suffix("ON CONFLICT DO NOTHING")
}

// BuildSingleInsertQuery ...
func BuildSingleInsertQuery(table Table, tableName string, format sqrl.PlaceholderFormat) (string, error) {
	query, _, err := BuildInsertStatement(table, tableName, format).
		Values(make([]interface{}, len(table.Keys))).
		ToSql()

	return query, err
}

// SetLimit ...
func SetLimit(in int) error {
	if in > 0 {
		sqlLimit = in
	} else {
		return errors.New("limit needs to be an integer greater than 0")
	}
	return nil
}

// Limit ...
func Limit() int {
	return sqlLimit
}
