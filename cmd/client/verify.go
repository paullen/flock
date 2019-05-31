package main

import (
	"bytes"
	"fmt"
	"regexp"

	flock "github.com/srikrsna/flock/pkg"
)

// Reads the flock file and extracts the named parameters
func testSchema(f []byte) ([]string, error) {

	buf := bytes.NewBuffer(f)

	params := make([]string, 0)
	fl, err := flock.ParseSchema(buf)
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

func testPlugin(f []byte) error {
	if _, err := flock.PluginHandler(f); err != nil {
		return err
	}
	return nil
}
