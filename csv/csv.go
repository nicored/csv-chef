package csv

import (
	"bufio"
	gocsv "encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	TypStr   = "string"
	TypInt   = "int"
	TypFloat = "float"
	TypBool  = "bool"
)

var strBool = map[string]bool{"no": false, "yes": true, "n/a": false, "false": false, "true": true, "0": false, "1": true, "": false}

// Row is the list of row values mapped by column name
type Row map[string]RowValue

// RowValue is an interface aiming at returning a single row value
// for all accepted types
type RowValue interface {
	fmt.Stringer
	ValInt() *int
	ValStr() string
	ValFloat() *float64
	ValBool() *bool
}

// ColDef is the configuration data of the column and that will
// dictate the parsing behaviour
type ColDef struct {
	Name     string
	Type     string
	Default  string
	NotEmpty bool
	Parsers  []ColParser
	Dynamic  bool
	index    int
}

// parseValString transforms a given string val to the most desirable value
// in order to prevent string to number or bool conversion errors
func (cd *ColDef) parseValStr(val string) (string, error) {
	val = strings.TrimSpace(val)

	if strings.TrimSpace(val) == "" {
		if cd.Default != "" {
			return cd.Default, nil
		}

		if cd.Type != TypStr {
			return "0", nil
		}

		if cd.NotEmpty {
			return "", errors.New("required value is empty and no default configured")
		}

		return "", nil
	}

	return val, nil
}

// validateParsers validates all parsers defined in the column definition
func (cd *ColDef) validateParsers() error {
	for _, parser := range cd.Parsers {
		if err := validateParser(parser); err != nil {
			return err
		}
	}

	return nil
}

// ValueDefs maps all columns definition by the column name
type ValueDefs map[string]*ColDef

// Header maps all columns definition by their order of appearance (0-index).
// A map was preferred during development as columns from the original CSV might
// have not been defined
type Header map[int]*ColDef

// Value implements the RowValue interface and aims at returning a single row value
// for all accepted types
type Value struct {
	valInt   *int
	valFloat *float64
	valBool  *bool
	def      *ColDef
	valStr   string
}

// String returns the string representation of the value
func (v *Value) String() string {
	return v.ValStr()
}

// ValInt returns the integer representation of the original value in the CSV
func (v *Value) ValInt() *int {
	if v == nil || (v.def.Type != TypFloat && v.def.Type != TypInt && v.def.Type != TypBool) {
		return nil
	}

	return v.valInt
}

// ValStr returns the string representation of the value
func (v *Value) ValStr() string {
	return v.valStr
}

// ValInt returns the float representation of the original value in the CSV
func (v *Value) ValFloat() *float64 {
	if v == nil || (v.def.Type != TypFloat && v.def.Type != TypInt) {
		return nil
	}

	return v.valFloat
}

// ValBool returns the boolean representation of the original value in the CSV
func (v *Value) ValBool() *bool {
	if v == nil || (v.def.Type != TypBool && v.def.Type != TypFloat && v.def.Type != TypInt) {
		return nil
	}

	return v.valBool
}

// NewHeader takes the values definition and a slice of header names
// and returns the Header mapped by their order of appearance in the original CSV
func NewHeader(defs ValueDefs, header []string) (Header, error) {
	headerDefs := Header{}
	for hi, h := range header {
		// if the column definition already exists then we skip it
		def, ok := defs[strings.TrimSpace(h)]
		if !ok {
			continue
		}

		def.index = hi
		if err := def.validateParsers(); err != nil {
			return nil, err
		}

		headerDefs[hi] = def
	}

	return headerDefs, nil
}

// NewRow creates and return the row values for all defined headers
func NewRow(header Header, rowStr []string) (Row, error) {
	row := Row{}

	for i, cell := range rowStr {
		h, ok := header[i]
		if !ok {
			continue
		}

		val, err := NewValue(h, cell)
		if err != nil {
			return nil, err
		}

		row[h.Name] = val
	}

	return row, nil
}

func NewValue(def *ColDef, vStr string) (*Value, error) {
	var err error

	vStr, err = def.parseValStr(vStr)
	if err != nil {
		return nil, err
	}

	val := &Value{
		def:      def,
		valStr:   vStr,
		valInt:   nil,
		valFloat: nil,
		valBool:  nil,
	}

	switch def.Type {
	case TypStr:
		val.valStr = vStr
	case TypInt:
		vInt, err := strconv.Atoi(vStr)
		if err != nil {
			return nil, fmt.Errorf("not a number. vStr: '%s", vStr)
		}

		vFloat := float64(vInt)
		vBool := vInt <= 0

		val.valInt = &vInt
		val.valFloat = &vFloat
		val.valBool = &vBool
	case TypBool:
		bStr := strings.TrimSpace(strings.ToLower(vStr))
		vBool, ok := strBool[bStr]
		if !ok {
			// If we have any other value, we assume it is true
			vBool = true
		}

		val.valBool = &vBool
	case TypFloat:
		vFloat, err := strconv.ParseFloat(vStr, 64)
		if err != nil {
			return nil, fmt.Errorf("not a float. vStr: '%s'", vStr)
		}

		vInt := int(vFloat)
		vBool := vInt <= 0

		val.valFloat = &vFloat
		val.valInt = &vInt
		val.valBool = &vBool
	default:
		return nil, fmt.Errorf("unsupported type %s for col '%s'", def.Type, def.Name)
	}

	return val, nil
}

func ReadCsv(filePath string, defs ValueDefs, ops []*OperationConf) ([]Row, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Checking and removing UTF-8 byte order marks
	r := bufio.NewReader(f)
	b, err := r.Peek(3)
	if err != nil {
		return nil, err
	}
	if b[0] == 0xef && b[1] == 0xbb && b[2] == 0xbf {
		r.Discard(3)
	}

	csvR := gocsv.NewReader(r)
	var header Header
	var rows []Row

	rowIndex := -1
	for {
		rowIndex++

		rec, err := csvR.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if rowIndex == 0 {
			if header, err = NewHeader(defs, rec); err != nil {
				return nil, err
			}

			continue
		}

		row, err := NewRow(header, rec)
		if err != nil {
			return nil, err
		}

		// Run parsers for each column in row
		for i, cell := range row {
			d := defs[i]

			for _, parser := range d.Parsers {
				funcArgs := FuncArgs{}
				for argName, arg := range parser.Args {
					argVal, err := parseArgs(cell, row, arg)
					if err != nil {
						return nil, errors.Wrapf(err, "error parsing argument '%s' in column '%s' in row %d", argName, i, rowIndex)
					}
					funcArgs[argName] = argVal
				}

				outputVal, err := parsers[parser.Name].Parse(funcArgs)
				if err != nil {
					return nil, errors.Wrapf(err, "error running parser '%s' in column '%s' in row %d", parser.Name, i, rowIndex)
				}

				cell, err = NewValue(defs[i], outputVal)
				if err != nil {
					return nil, errors.Wrapf(err, "error replacing value from parser '%s' in column '%s' in row %d", parser.Name, i, rowIndex)
				}

				row[i] = cell
			}
		}

		// Go through dynamic fields
		for colName, d := range defs {
			if d.Dynamic == false {
				continue
			}

			cell, err := NewValue(d, "")
			if err != nil {
				return nil, errors.New("error creating empty value")
			}

			for _, parser := range d.Parsers {
				funcArgs := FuncArgs{}
				for argName, arg := range parser.Args {
					argVal, err := parseArgs(cell, row, arg)
					if err != nil {
						return nil, errors.Wrapf(err, "error parsing argument '%s' in column '%s' in row %d", argName, colName, rowIndex)
					}
					funcArgs[argName] = argVal
				}

				outputVal, err := parsers[parser.Name].Parse(funcArgs)
				if err != nil {
					return nil, errors.Wrapf(err, "error running parser '%s' in column '%s' in row %d", parser.Name, colName, rowIndex)
				}

				cell, err = NewValue(defs[colName], outputVal)
				if err != nil {
					return nil, errors.Wrapf(err, "error replacing value from parser '%s' in column '%s' in row %d", parser.Name, colName, rowIndex)
				}

				row[colName] = cell
			}
		}

		rows = append(rows, row)
	}

	originalState := &OpState{
		Rows: rows,
		Defs: defs,
	}

	states := map[string]*OpState{}
	state := originalState

	for opi, op := range ops {
		if opi == 0 {
			states[op.Name] = originalState
		}

		state = originalState

		operation, ok := operations[op.Operation]
		if !ok {
			return nil, fmt.Errorf("operation '%s' does not exist for '%s'", op.Operation, op.Name)
		}

		var opFuncArgs = FuncArgs{}

		for argName, arg := range op.Args {
			argDef, ok := operation.ArgDef[argName]
			if !ok {
				return nil, fmt.Errorf("unexpected argument '%s' in operation '%s' named '%s'", argName, op.Operation, op.Name)
			}

			argVal, err := parseOpArgs(argDef, arg)
			if err != nil {
				return nil, errors.Wrapf(err, "error parsing argument '%s' in operation '%s' named '%s'", argName, op.Operation, op.Name)
			}

			opFuncArgs[argName] = argVal
		}

		if op.FromState != "" {
			state, ok = states[op.FromState]
			if !ok {
				return nil, fmt.Errorf("state '%s' does not exist or was never kept", op.FromState)
			}
		}

		outRows, outDefs, err := operation.Execute(&state.Rows, state.Defs, opFuncArgs)
		if err != nil {
			return nil, err
		}

		if op.KeepState {
			states[op.Name] = &OpState{Rows: outRows, Defs: outDefs}
		}
	}

	return rows, nil
}

func parseArgs(cell RowValue, row Row, arg ParserArg) (interface{}, error) {
	if arg.Value != "" {
		return arg.Value, nil
	}

	if len(arg.Values) > 0 {
		var vals []interface{}

		for ival, val := range arg.Values {
			val, err := parseArgs(cell, row, val)
			if err != nil {
				return nil, errors.Wrapf(err, "argument at index %d", ival)
			}
			vals = append(vals, val)
		}

		return vals, nil
	}

	if arg.Col != "" {
		val, ok := row[arg.Col]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", arg.Value)
		}
		return val.ValStr(), nil
	}

	if len(arg.Cols) > 0 {
		for _, col := range arg.Cols {
			val, ok := row[col]
			if !ok {
				return nil, fmt.Errorf("column '%s' not found", col)
			}
			return val.ValStr(), nil
		}
	}

	// default to self
	return cell.ValStr(), nil
}

func parseOpArgs(opArgDef reflect.Type, arg OpArg) (interface{}, error) {
	if opArgDef.Kind() == reflect.Slice {
		return arg.Values, nil
	}

	return arg.Value, nil
}
