package flock_test

import (
	"flag"
	"io/ioutil"
	"testing"

	flock "github.com/srikrsna/flock/pkg"
)

var replacefu = flag.Bool("replacefu", false, "Replace the output files instead of comparing them")

func TestPluginHandler(t *testing.T) {
	f, err := ioutil.ReadFile("./test_files/inputs/test1.go")
	if err != nil {
		t.Fatal(err)
	}

	funcs, err := flock.PluginHandler(f)
	if err != nil {
		t.Error(err)
	}
	t.Log(funcs)
	t.Log(funcs["SayHi"])
}
