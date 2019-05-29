package flock_test

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	flock "github.com/srikrsna/flock/pkg"
)

var replacet = flag.Bool("replacet", false, "Replace flag replaces the table output files specifically instead of comparing them")

func TestBuildTables(t *testing.T) {
	const (
		inPath  = "./test_files/inputs/"
		outPath = "./test_files/outputs/tables/"
	)
	tests := []struct {
		flName    string
		tableName string
		wantErr   bool
	}{
		{
			"test.fl",
			"test_table.txt",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.flName, func(t *testing.T) {
			f, err := os.Open(inPath + tt.flName)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			fl, err := flock.ParseSchema(f)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSchema() error = %v, wantErr %v", err, tt.wantErr)
			}

			tables := flock.BuildTables(fl)

			if *replace || *replacet {
				f, err := os.Create(outPath + tt.tableName)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				file, err := json.MarshalIndent(tables, "", "\t")
				if err != nil {
					t.Errorf("failed to replace table output: %v", err)
				}
				_, err = f.Write(file)
				if err != nil {
					t.Errorf("failed to replace table output: %v", err)
				}
			}

			exp, err := ioutil.ReadFile(outPath + tt.tableName)
			if err != nil {
				t.Fatal(err)
			}
			expectedTables := make(map[string]flock.Table)
			if err := json.Unmarshal(exp, &expectedTables); err != nil {
				t.Errorf("failed to read output file: %v", err)
			}
			if !reflect.DeepEqual(tables, expectedTables) {
				t.Errorf("mismatched table output, expected: %v, got: %v", expectedTables, tables)
			}
		})
	}
}
