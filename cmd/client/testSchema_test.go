package main

import (
	"os"
	"reflect"
	"testing"
)

func TestTestSchema(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		output []string
	}{
		{"Test-1", "./schema_test.fl", []string{"first", "theird"}},
	}
	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			f, err := os.Open(v.input)
			if err != nil {
				t.Error(err.Error())
			}
			params, err := testSchema(f)

			if !reflect.DeepEqual(params, v.output) {
				t.Errorf("output did not match, expected: %v, got: %v", v.output, params)
			}
		})
	}
}
