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

type Variable struct {
	Func string
	index []int
	Column []string
}
// BuildTables
func BuildTables(flock *Flock) (map[string]Table, map[string]map[string]Variable) {
	res := make(map[string]Table, len(flock.Entries))
	vars := make(map[string]map[string]Variable, len(flock.Entries))
	for _, e := range flock.Entries {
		var t Table
		t.Name = e.Name
		t.Keys = make(map[string]Column, len(e.Fields))
		t.Ordered = make([]string, 0, len(e.Fields))
		vars[e.Name] = make(map[string]Variable)
		for _, field := range e.Fields {
			c := Column{Value: field.Value, Functions: make([]Func, 0, len(field.Functions))}
			v := Variable {
				index: make([]int, 0),
				Column: make([]string, 0),
			}
			for _, fun := range field.Functions {
				f := Func{
					Name:       fun.Name,
					Parameters: make([]reflect.Value, 0, len(fun.Parameters)),
				}
				v.Func = fun.Name
				for _, arg := range fun.Parameters {
					val, flag := arg.Value()
					if flag {
						v.index = append(v.index, len(f.Parameters))
						v.Column = append(v.Column, val.(string))
					}
					f.Parameters = append(f.Parameters, reflect.ValueOf(val))
				}
				c.Functions = append(c.Functions, f)
			}

			t.Keys[field.Key] = c
			t.Ordered = append(t.Ordered, field.Key)
			vars[e.Name][field.Value] = v
		}
		res[e.Name] = t
	}

	return res, vars
}
