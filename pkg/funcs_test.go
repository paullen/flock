package flock

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestRegisterFunc(t *testing.T) {
	testFuncs := FuncMap{
		"one":  func() int { return 1 },
		"two":  func(string) bool { return false },
		"four": func(float64) (error, error) { return nil, nil },
	}
	RegisterFunc(testFuncs)
}

func TestGoodFunc(t *testing.T) {
	testFuncs := []struct {
		function interface{}
		valid    bool
	}{
		{func() int { return 0 }, true},
		{func(bool) bool { return false }, true},
		{func() (int, error) { return 0, errors.New("Boo") }, true},
		{func() { fmt.Println("WO") }, false},
		{func() (int, bool) { return 0, true }, false},
		{func() (int, int, string) { return 1, 2, "cow" }, false},
		{func() (float64, uint32, bool) { return 1.00000003, 34, true }, false},
		{func(float64) (error, error) { return nil, nil }, true},
	}
	for _, v := range testFuncs {
		if goodFunc(reflect.TypeOf(v.function)) != v.valid {
			t.Errorf("Expected %v, got %v", v.valid, !v.valid)
		}
	}
}
