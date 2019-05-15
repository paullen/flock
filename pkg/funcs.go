package flock

import (
	"fmt"
	"reflect"
)

var funcMap = map[string]reflect.Value{}

type FuncMap map[string]interface{}

var (
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

func RegisterFunc(fm FuncMap) {
	for name, v := range fm {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Func {
			panic("value for " + name + " not a function")
		}

		if !goodFunc(rv.Type()) {
			panic(fmt.Errorf("can't install method/function %q with %d results", name, rv.Type().NumOut()))
		}

		funcMap[name] = rv
	}
}

func goodFunc(typ reflect.Type) bool {
	// We allow functions with 1 result or 2 results where the second is an error.
	switch {
	case typ.NumOut() == 1:
		return true
	case typ.NumOut() == 2 && typ.Out(1) == errorType:
		return true
	}
	return false
}
