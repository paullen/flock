package flock_test

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/alecthomas/repr"
	"github.com/sergi/go-diff/diffmatchpatch"
	flock "github.com/srikrsna/flock/pkg"
)

var replacef = flag.Bool("replacef", false, "Replace flag replaces the flock output files specifically instead of comparing them")
var replace = flag.Bool("replace", false, "Replace flag replaces both the flock and table output files instead of comparing them")

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
			v, ok := fl.Entries[0].Fields[0].Functions[0].Parameters[0].Value()
			t.Logf("***************** %v -- %v", v, ok)
			if *replace || *replacef {
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
