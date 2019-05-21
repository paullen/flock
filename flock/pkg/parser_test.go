package flock_test

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	//"fmt"
	"testing"

	"github.com/alecthomas/repr"
	"github.com/sergi/go-diff/diffmatchpatch"
	flock "github.com/srikrsna/flock/pkg"
)

var replace = flag.Bool("replace", false, "Replace flag replaces the output files instead of comparing them")

var printOpts = []repr.Option{
	repr.Indent("\t"),
	repr.OmitEmpty(true),
}

func TestParseSchema(t *testing.T) {
	const (
		inPath  = "./test_files/inputs/"
		outPath = "./test_files/outputs/"
	)
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			"test.fl",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(inPath + tt.name)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			fl, err := flock.ParseSchema(f)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSchema() error = %v, wantErr %v", err, tt.wantErr)
			}

			//fmt.Println(flock.BuildTables(fl))
			if *replace {
				f, err := os.Create(outPath + tt.name)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()

				repr.New(f, printOpts...).Print(fl)
			}

			exp, err := ioutil.ReadFile(outPath + tt.name)
			if err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			repr.New(&buf, printOpts...).Print(fl)

			if !bytes.Equal(buf.Bytes(), exp) {
				dmp := diffmatchpatch.New()
				diff := dmp.DiffMain(string(buf.Bytes()), string(exp), true)
				t.Errorf("mismatched parse output %s", dmp.DiffPrettyText(diff))
			}
		})
	}
}
