package flock

import (
	"io"

	"github.com/alecthomas/participle"
)

type Flock struct {
	Entries []*Entry `@@*`
}

type Entry struct {
	Name   string   `@(String|Ident|RawString) "{"`
	Query  string   `@RawString`
	Fields []*Field `"{" @@* "}" "}"`
}

type Field struct {
	Key       string       `("-"@Ident "="`
	Value     string       `@Ident`
	Functions []*FieldFunc `@@* )`
}

type FieldFunc struct {
	Name       string           `( "|" @Ident`
	Parameters []*FuncParameter ` @@*)`
}

type FuncParameter struct {
	Key    *string  `( @Ident`
	String *string  `| @(String | RawString)`
	Int    *int64   `| @Int`
	Float  *float64 `| @Float`
	Char   *rune    `| @Char`
	Bool   *bool    `| @("true" | "false") )`
}

func (f FuncParameter) Value() (interface{}, bool) {
	switch {
	case f.Key != nil:
		return *f.Key, true
	case f.String != nil:
		return *f.String, false
	case f.Int != nil:
		return *f.Int, false
	case f.Float != nil:
		return *f.Float, false
	case f.Char != nil:
		return *f.Char, false
	case f.Bool != nil:
		return *f.Bool, false
	default:
		return nil, false
	}
}

type Directive struct {
	Name string   `@Ident "("`
	Vars []string `{ ("$"@Ident) } ")"`
}

var parser = participle.MustBuild(&Flock{}, participle.UseLookahead(1))

func ParseSchema(r io.Reader) (*Flock, error) {
	var f Flock
	return &f, parser.Parse(r, &f)
}
