package flock

import "reflect"

type Table struct {
	Name    string
	Keys    map[string]Column
	Ordered []string
}

type Column struct {
	Value     string
	Functions []Func
}

type Func struct {
	Name       string
	Parameters []reflect.Value
}

// BuildTables
func BuildTables(flock *Flock) map[string]Table {
	res := make(map[string]Table, len(flock.Entries))
	for _, e := range flock.Entries {
		var t Table

		t.Name = e.Name

		t.Keys = make(map[string]Column, len(e.Fields))
		t.Ordered = make([]string, 0, len(e.Fields))

		for _, field := range e.Fields {
			c := Column{Value: field.Value, Functions: make([]Func, 0, len(field.Functions))}

			for _, fun := range field.Functions {
				f := Func{
					Name:       fun.Name,
					Parameters: make([]reflect.Value, 0, len(fun.Parameters)),
				}

				for _, arg := range fun.Parameters {
					f.Parameters = append(f.Parameters, reflect.ValueOf(arg.Value()))
				}

				c.Functions = append(c.Functions, f)
			}

			t.Keys[field.Key] = c
			t.Ordered = append(t.Ordered, field.Key)
		}

		res[e.Name] = t
	}

	return res
}
