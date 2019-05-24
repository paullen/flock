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
	Key       string       `(@Ident "="`
	Value     string       `@Ident`
	Functions []*FieldFunc `@@* )`
}

type FieldFunc struct {
	Name       string           `( "|" @Ident`
	Parameters []*FuncParameter ` @@* )`
}

type FuncParameter struct {
	String *string  `( @(String | RawString)`
	Int    *int64   `| @Int`
	Float  *float64 `| @Float`
	Char   *rune    `| @Char`
	Bool   *bool    `| @("true" | "false") )`
}

func (f FuncParameter) Value() interface{} {
	switch {
	case f.String != nil:
		return *f.String
	case f.Int != nil:
		return *f.Int
	case f.Float != nil:
		return *f.Float
	case f.Char != nil:
		return *f.Char
	case f.Bool != nil:
		return *f.Bool
	default:
		return nil
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
