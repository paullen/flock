package main

import (
	"database/sql"
	"regexp"
	// "strings"
)

// Generates parameterized query and arguments according to the values and query provided
func parseQuery(query string, params map[string]interface{}) (string, []interface{}) {
	r := regexp.MustCompile(`\@([a-zA-Z]+)`)
	namedParams := r.FindAllStringSubmatch(query, -1)
	// query = r.ReplaceAllString(query, `?`)
	args := make([]interface{}, 0)
	for _, name := range namedParams {
		args = append(args, sql.Named(name[1], params[name[1]]))
	}
	return query, args
}
