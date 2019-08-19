package csv

import "reflect"

// ParseFuncArgs maps the arguments value by their name and is used
// when calling a ParseFunc function
type FuncArgs map[string]interface{}

// ParserArg is the argument configuration for the parser as defined
// in the loaded configuration.
type ParserArg struct {
	Value  string      `yaml:"value"`
	Values []ParserArg `yaml:"values"`
	Col    string      `yaml:"col"`
	Cols   []string    `yaml:"cols"`
}

// ArgDef maps the argument name to its expected type from the parser
type ArgDef map[string]reflect.Type
