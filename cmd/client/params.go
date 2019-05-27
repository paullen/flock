package main

import (
	"fmt"
	"regexp"
)

func parseQuery(query string, params map[string]interface{}) (string, []interface{}) {
	r := regexp.MustCompile(`\@([a-zA-Z]+)`)
	namedParams := r.FindAllStringSubmatch(query, -1)
	query = r.ReplaceAllString(query, fmt.Sprintf("?"))
	args := make([]interface{}, 0)
	for _, name := range namedParams {
		args = append(args, params[name[1]])
	}
	return query, args
}
