package main

import (
	"os"
	"testing"

	flock "github.com/srikrsna/flock/pkg"
)

func TestParseQuery(t *testing.T) {
	params := map[string]interface{}{"first": "hello", "theird": "is"}
	f, err := os.Open("./schema_test.fl")
	if err != nil {
		t.Fatalf("unable to open file")
	}
	defer f.Close()

	fl, err := flock.ParseSchema(f)
	if err != nil {
		t.Fatalf("unable to parse schema file")
	}
	query, args := parseQuery(fl.Entries[0].Query, params)
	t.Logf(query)
	t.Log(args)
}
