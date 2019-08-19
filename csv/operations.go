package csv

import (
	"fmt"
)

type OpFunc func(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error)

var operations = map[string]Operation{}

func AddOperations(newOps ...Operation) error {
	for _, op := range newOps {
		if _, ok := operations[op.Name]; ok {
			return fmt.Errorf("operation '%s' already exists", op.Name)
		}

		operations[op.Name] = op
	}
	return nil
}

type OperationConf struct {
	Name      string `yaml:"name"`
	Operation string `yaml:"operation"`

	NewState  bool   `yaml:"newState"`
	KeepState bool   `yaml:"keepState"`
	FromState string `yaml:"fromState"`

	Args map[string]OpArg
}

type OpState struct {
	Rows []Row
	Defs ValueDefs
}

type Operation struct {
	Name   string
	OpFunc OpFunc
	ArgDef ArgDef
}

func (op *Operation) Execute(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	return op.OpFunc(rows, defs, args)
}

type OpArg struct {
	Value  string   `yaml:"value"`
	Values []string `yaml:"values"`
}
