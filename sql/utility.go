package flock

import (
	"context"
	"database/sql"
)

//ConnectDB -  Return database connection interface when passed the connection string and database
func ConnectDB(u string, databaseSrv string) (*sql.DB, error) {

	db, err := sql.Open(databaseSrv, u)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

//GetData  - Returns select query results
func GetData(ctx context.Context, db *sql.DB, query string, args []interface{}) ([]map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.ColumnTypes()

	res := []map[string]interface{}{}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colTyp := range cols {
			val := columnPointers[i].(*interface{})
			m[colTyp.Name()] = *val
		}

		res = append(res, m)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
