package csv

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/robertkrimen/otto"
	"path/filepath"
	"strconv"

	"reflect"
	"strings"
)

// ParseFunc is the definition of the function used to run the parser
// for the given arguments
type ParseFunc func(args FuncArgs) (string, error)

// ColParser is the column parser as defined in the loaded configuration
type ColParser struct {
	Name string               `yaml:"name"`
	Args map[string]ParserArg `yaml:"args"`
}

// parsers is a list of all available parsers mapped by parser name
var parsers = map[string]ParserI{}

// ParserI is the parser's interface
type ParserI interface {
	Name() string
	Parser() ParseFunc
	ArgDef() ArgDef
	Parse(args FuncArgs) (string, error)
}

// JsParserI is the Javascript parser interface which also inherits from
// ParserI interface's behaviours
type JsParserI interface {
	ParserI
	Script() string
}

// validateParser validates that the parser is available for use
// and that the provided arguments' name and type match the parser's
// requirements
func validateParser(colParser ColParser) error {
	name := colParser.Name
	args := colParser.Args

	// validating that the parser has been loaded
	parser, ok := parsers[name]
	if !ok {
		return fmt.Errorf("parser '%s' does not exist", name)
	}

	for arg, val := range args {
		// validating that all provided arguments are required
		parserArg, ok := parser.ArgDef()[arg]
		if !ok {
			return fmt.Errorf("parser '%s' does not take argument '%s'", name, arg)
		}

		// validating that all provided arguments are of the right type
		if err := validateParserArgType(parserArg, val); err != nil {
			return errors.Wrapf(err, "invalid type for argument '%s' in parser '%s'", arg, name)
		}
	}

	return nil
}

// validateParserArgType validates that all provided arguments are of the right type
func validateParserArgType(defType reflect.Type, arg ParserArg) error {
	if len(arg.Values) > 0 && defType.Kind() != reflect.Slice {
		return fmt.Errorf("type must either be 'self', 'col', or 'val', not 'array'")
	}

	if len(arg.Values) == 0 && defType.Kind() == reflect.Slice {
		return fmt.Errorf("type must be 'array'")
	}

	return nil
}

// AddParsers adds given parsers to the list
func AddParsers(parsersList ...ParserI) error {
	for _, parser := range parsersList {
		name := strings.TrimSpace(parser.Name())

		if name == "" {
			return errors.New("parser's name cannot be empty")
		}

		if _, ok := parsers[name]; ok {
			return fmt.Errorf("parser with name '%s' already exists", name)
		}

		parsers[parser.Name()] = parser
	}

	return nil
}

// NewJSParser creates a javascript parser from a javascript file
func NewJSParser(filename string) (JsParserI, error) {
	vm := otto.New()

	script, err := vm.Compile(filename, nil)
	if err != nil {
		return nil, err
	}

	// running the script without checking for errors, all we want is the required args list
	vm.Run(script)

	// retrieving the list of required arguments from the javascript script by fetching the 'args' variable
	reqVals, err := vm.Get("args")
	if err != nil {
		return nil, err
	}

	reqValsI, err := reqVals.Export()
	if err != nil {
		return nil, err
	}

	parser := &JsParser{
		name:   filepath.Base(filename),
		script: script.String(),
	}

	// checking if we have required arguments
	if reqValsI != nil {
		args, ok := reqValsI.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("js error: 'args' must be an object in '%s'", filename)
		}

		parserArgs := ArgDef{}

		// we translate the required arguments type from the js args definition to their go type
		for arg, typ := range args {
			switch typ {
			case "string":
				parserArgs[arg] = reflect.TypeOf("")
			case "array":
				parserArgs[arg] = reflect.TypeOf([]interface{}{})
			case "object":
				parserArgs[arg] = reflect.TypeOf(map[string]interface{}{})
			case "bool":
				parserArgs[arg] = reflect.TypeOf(true)
			default:
				return nil, fmt.Errorf("type '%s' is not supported in '%s'", typ, filename)
			}
		}

		parser.args = parserArgs
	}

	// implement the ParserFunc function
	parser.parser = func(args FuncArgs) (string, error) {
		vm := otto.New()

		// Making sure that the provided argument values match the defined required types
		for arg, typ := range parser.args {
			val, ok := args[arg]
			if !ok {
				return "", fmt.Errorf("arg '%s' required but missing", arg)
			}

			valType := reflect.TypeOf(val)
			if typ != valType {
				return "", fmt.Errorf("unexpected argument type. Expected '%s', got '%s' in '%s'", typ.String(), valType.String(), filename)
			}

			vm.Set(arg, val)
		}

		if _, err = vm.Run(parser.Script()); err != nil {
			return "", err
		}

		// We expect the string variable 'output' in the js script to be defined and ready for extraction
		output, err := vm.Get("output")
		if err != nil {
			return "", err
		}

		return output.String(), nil
	}

	return parser, nil
}

// Parser implements the ParserI interface
// which holds a built-in parser available in the API
type Parser struct {
	name   string    // the name of the parser
	parser ParseFunc // the function parsing value(s) from the argument(s)
	args   ArgDef    // arguments are values to be parsed
}

// Name returns the name of the parser
func (p *Parser) Name() string {
	return p.name
}

// ParseFunc returns the function used to parse the value(s)
func (p *Parser) Parser() ParseFunc {
	return p.parser
}

// Args returns the provided values used for the parsing
func (p *Parser) ArgDef() ArgDef {
	return p.args
}

// Parse runs the parser
func (p *Parser) Parse(args FuncArgs) (string, error) {
	return p.parser(args)
}

// JsParser is a parser enabling javascript code to do the parsing
type JsParser struct {
	name   string
	parser ParseFunc
	args   ArgDef
	script string
}

// Name returns the name of the parser
func (jp *JsParser) Name() string {
	return jp.name
}

// ParseFunc returns the function used to parse the value(s)
func (jp *JsParser) Parser() ParseFunc {
	return jp.parser
}

// Args returns the provided values used for the parsing
func (jp *JsParser) ArgDef() ArgDef {
	return jp.args
}

// Parse runs the parser
func (jp *JsParser) Parse(args FuncArgs) (string, error) {
	return jp.parser(args)
}

// Script returns the compiled javascript script used by the parser
func (jp *JsParser) Script() string {
	return jp.script
}

func argString(args FuncArgs, argName string) (string, error) {
	vI, ok := args[argName]
	if !ok {
		return "", fmt.Errorf("'%s' argument not provided", argName)
	}

	vS, ok := vI.(string)
	if !ok {
		return "", fmt.Errorf("'%s' must be a string", argName)
	}

	return vS, nil
}

func argInt(args FuncArgs, argName string) (int, error) {
	vI, ok := args[argName]
	if !ok {
		return 0, fmt.Errorf("'%s' argument not provided", argName)
	}

	vInt, ok := vI.(int)
	if ok {
		return vInt, nil
	}

	vS, ok := vI.(string)
	if !ok {
		return 0, fmt.Errorf("'%s' must be an integer", argName)
	}

	var err error
	if vInt, err = strconv.Atoi(vS); err != nil {
		return 0, fmt.Errorf("'%s' must be an integer", argName)
	}

	return vInt, nil
}

func argBool(args FuncArgs, argName string) (bool, error) {
	vI, ok := args[argName]
	if !ok {
		return false, fmt.Errorf("'%s' argument not provided", argName)
	}

	vBool, ok := vI.(bool)
	if ok {
		return vBool, nil
	}

	vS, ok := vI.(string)
	if !ok {
		return false, fmt.Errorf("'%s' must be a boolean", argName)
	}

	vBool, _ = strBool[vS]
	return vBool, nil
}

func argSliceString(args FuncArgs, argName string) ([]string, error) {
	vI, ok := args[argName]
	if !ok {
		return nil, fmt.Errorf("'%s' argument not provided", argName)
	}

	vS, ok := vI.([]string)
	if !ok {
		return nil, fmt.Errorf("'%s' must be a slice of strings", argName)
	}

	return vS, nil
}
