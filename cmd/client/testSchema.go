package main

import (
	"fmt"
	"io"
	"regexp"

	flock "github.com/srikrsna/flock/pkg"
)

// Reads the flock file and extracts the named parameters
func testSchema(f io.Reader) ([]string, error) {

	params := make([]string, 0)
	fl, err := flock.ParseSchema(f)
	if err != nil {
		fmt.Println("Failed to parse schema. Please check your .fl file.")
		return params, err
	}

	r := regexp.MustCompile(`\@([a-zA-Z]+)`)
	for _, v := range fl.Entries {
		param := r.FindAllStringSubmatch(v.Query, -1)
		for _, v := range param {
			params = append(params, v[1])
		}
	}
	return params, nil
}
