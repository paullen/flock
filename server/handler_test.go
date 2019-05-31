package server

import (
	"io/ioutil"
	"testing"
)

func TestGenerateBase(t *testing.T) {
	info := map[string]([]string){"User": []string{"Am", "Wa", "Y"}, "Blah": []string{"B", "L", "A", "H"}}
	b, err := generateBase(info)
	if err != nil {
		t.Errorf("failed to generate base fl")
	}
	if err := ioutil.WriteFile("generated/test.fl", b, 0777); err != nil {
		t.Errorf("failed to write to file")
	}
}
