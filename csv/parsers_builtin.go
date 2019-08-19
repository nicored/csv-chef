package csv

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

func init() {
	// Loading all built-in parsers
	err := AddParsers(
		concatParser,
		tolowercaseParser,
		toUppercaseParser,
		fileExtParser,
		fileExistsParser,
		fileMd5Parser,
		containsParser,
	)

	// This should not happen
	if err != nil {
		panic(err)
	}
}

var tolowercaseParser = &Parser{
	name:   "lowercase",
	parser: changeCase(false),
	args:   ArgDef{"value": reflect.TypeOf("")},
}

var toUppercaseParser = &Parser{
	name:   "uppercase",
	parser: changeCase(true),
	args:   ArgDef{"value": reflect.TypeOf("")},
}

func changeCase(upper bool) ParseFunc {
	return func(args FuncArgs) (string, error) {
		val, ok := args["value"]
		if !ok {
			return "", errors.New("val argument not provided")
		}

		valTyp := reflect.TypeOf(val)
		if valTyp.Kind() != reflect.String {
			return "", fmt.Errorf("val isn't a string. %s given", valTyp.String())
		}

		if upper == true {
			return strings.ToUpper(val.(string)), nil
		}

		return strings.ToLower(val.(string)), nil
	}
}

var concatParser = &Parser{
	name:   "concat",
	parser: concat,
	args:   ArgDef{"values": reflect.TypeOf([]interface{}{})},
}

func concat(args FuncArgs) (string, error) {
	values, ok := args["values"]
	if !ok {
		return "", errors.New("values argument not provided")
	}

	var valuesStr []string
	vall := values.([]interface{})

	for _, val := range vall {
		switch reflect.TypeOf(val).Kind() {
		case reflect.String:
			valuesStr = append(valuesStr, val.(string))
		case reflect.Float64:
			valuesStr = append(valuesStr, fmt.Sprintf("%f", val.(float64)))
		case reflect.Int:
			valuesStr = append(valuesStr, strconv.Itoa(val.(int)))
		}
	}

	return strings.Join(valuesStr, ""), nil
}

var fileExtParser = &Parser{
	name:   "ext",
	parser: extParser,
	args:   ArgDef{"value": reflect.TypeOf("")},
}

func extParser(args FuncArgs) (string, error) {
	val, ok := args["filename"]
	if !ok {
		return "", errors.New("filename argument not provided")
	}

	fileName := val.(string)

	ext := filepath.Ext(fileName)
	if ext != "" && ext[0] == '.' {
		return ext[1:], nil
	}

	return ext, nil
}

var fileExistsParser = &Parser{
	name:   "fileExists",
	parser: fileExists,
	args:   ArgDef{"value": reflect.TypeOf("")},
}

func fileExists(args FuncArgs) (string, error) {
	val, ok := args["filename"]
	if !ok {
		return "", errors.New("filename argument not provided")
	}

	if _, err := os.Stat(val.(string)); os.IsNotExist(err) {
		return "false", nil
	}

	return "true", nil
}

var fileMd5Parser = &Parser{
	name:   "fileMd5",
	parser: fileMd5,
	args:   ArgDef{"filename": reflect.TypeOf("")},
}

func fileMd5(args FuncArgs) (string, error) {
	val, ok := args["filename"]
	if !ok {
		return "", errors.New("filename argument not provided")
	}

	fileName := val.(string)
	// if file does not exist, we return an empty string
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return "", nil
	}

	cnt, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", err
	}

	fMd5 := md5.Sum(cnt)
	return hex.EncodeToString(fMd5[:]), nil
}

var containsParser = &Parser{
	name:   "contains",
	parser: contains,
	args:   ArgDef{"value": reflect.TypeOf(""), "term": reflect.TypeOf("")},
}

func contains(args FuncArgs) (string, error) {
	val, ok := args["value"]
	if !ok {
		return "", errors.New("value argument not provided")
	}

	term, ok := args["term"]
	if !ok {
		return "", errors.New("term argument not provided")
	}

	valStr := val.(string)
	termStr := term.(string)

	if strings.Contains(valStr, termStr) {
		return "true", nil
	}

	return "false", nil
}
