package csv

import (
	"crypto/md5"
	gocsv "encoding/csv"
	"encoding/hex"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func init() {
	err := AddOperations(
		printOperation,
		toFileOperation,
		sortOperation,
		dupesCountOp,
		findDupesOp,
		mergeDupesOp,
		md5FileOp,
	)
	if err != nil {
		panic(err)
	}
}

var printOperation = Operation{
	Name:   "print",
	OpFunc: opPrint,
	ArgDef: ArgDef{"cols": reflect.TypeOf([]string{})},
}

func opPrint(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	colsI, ok := args["cols"]
	if !ok {
		return nil, nil, errors.New("cols argument not provided")
	}

	cols := colsI.([]string)

	w := gocsv.NewWriter(os.Stdout)

	// printing header
	var header []string
	for _, h := range cols {
		header = append(header, h)
	}
	w.Write(header)

	for i, r := range *rows {
		var output []string
		for _, col := range cols {
			output = append(output, r[col].ValStr())
		}
		w.Write(output)

		if i > 1 && i%100 == 0 {
			w.Flush()
		}
	}

	w.Flush()
	return nil, nil, nil
}

var toFileOperation = Operation{
	Name:   "toFile",
	OpFunc: opToFile,
	ArgDef: ArgDef{"filename": reflect.TypeOf(""), "cols": reflect.TypeOf([]string{})},
}

func opToFile(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	colsI, ok := args["cols"]
	if !ok {
		return nil, nil, errors.New("cols argument not provided")
	}

	cols := colsI.([]string)

	val, ok := args["filename"]
	if !ok {
		return nil, nil, errors.New("filename argument not provided")
	}

	fileName := val.(string)

	wf, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return nil, nil, err
	}
	defer wf.Close()

	w := gocsv.NewWriter(wf)

	// printing header
	var header []string
	for _, h := range cols {
		header = append(header, h)
	}
	w.Write(header)

	for i, r := range *rows {
		var output []string
		for _, col := range cols {
			output = append(output, r[col].ValStr())
		}
		w.Write(output)

		if i > 1 && i%100 == 0 {
			w.Flush()
		}
	}

	w.Flush()
	return nil, nil, nil
}

var sortOperation = Operation{
	Name:   "sort",
	OpFunc: opSort,
	ArgDef: ArgDef{"cols": reflect.TypeOf([]string{}), "order": reflect.TypeOf([]string{})},
}

func opSort(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	colsI, ok := args["cols"]
	if !ok {
		return nil, nil, errors.New("cols argument not provided")
	}
	cols := colsI.([]string)

	orderI, ok := args["order"]
	if !ok {
		return nil, nil, errors.New("order argument not provided")
	}
	order := orderI.([]string)

	if len(order) != len(cols) {
		return nil, nil, errors.New("number of items in 'order' must be equal to number of items in 'cols'")
	}
	sort.Slice(*rows,
		func(i, j int) bool {
			for colI, col := range cols {
				colDef := defs[col]

				if order[colI] != "desc" && order[colI] != "asc" {
					order[colI] = "asc"
				}

				if colDef.Type == TypStr {
					if order[i] == "asc" {
						if (*rows)[i][col].ValStr() < (*rows)[j][col].ValStr() {
							return true
						}

						if (*rows)[i][col].ValStr() > (*rows)[j][col].ValStr() {
							return false
						}
					}

					if order[i] == "desc" {
						if (*rows)[i][col].ValStr() > (*rows)[j][col].ValStr() {
							return true
						}

						if (*rows)[i][col].ValStr() < (*rows)[j][col].ValStr() {
							return false
						}
					}
				}

				if colDef.Type == TypFloat || colDef.Type == TypInt {
					if order[i] == "asc" {
						if *(*rows)[i][col].ValFloat() < *(*rows)[j][col].ValFloat() {
							return true
						}

						if *(*rows)[i][col].ValFloat() > *(*rows)[j][col].ValFloat() {
							return false
						}
					}

					if order[i] == "desc" {
						if *(*rows)[i][col].ValFloat() > *(*rows)[j][col].ValFloat() {
							return true
						}

						if *(*rows)[i][col].ValFloat() < *(*rows)[j][col].ValFloat() {
							return false
						}
					}
				}
			}

			return false
		})

	return nil, nil, nil
}

var dupesCountOp = Operation{
	Name:   "dupesCount",
	OpFunc: opDupesCount,
	ArgDef: ArgDef{
		"indexCols": reflect.TypeOf([]string{}),
		"outCols":   reflect.TypeOf([]string{}),
		"countCol":  reflect.TypeOf(""),
		"gt":        reflect.TypeOf(int(1)),
	},
}

func opDupesCount(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	colsI, ok := args["indexCols"]
	if !ok {
		return nil, nil, errors.New("indexCols argument not provided")
	}
	cols := colsI.([]string)

	outColsI, ok := args["outCols"]
	if !ok {
		return nil, nil, errors.New("outCols argument not provided")
	}
	outCols := outColsI.([]string)

	countColI, ok := args["countCol"]
	if !ok {
		return nil, nil, errors.New("countCol argument not provided")
	}
	countColName := countColI.(string)

	gtI, ok := args["gt"]
	if !ok {
		return nil, nil, errors.New("gt argument not provided")
	}
	gt, err := strconv.Atoi(gtI.(string))
	if err != nil {
		return nil, nil, err
	}

	m := map[string][]Row{}
	for _, row := range *rows {
		index := ""

		for _, col := range cols {
			index += row[col].ValStr()
		}

		if _, ok := m[index]; !ok {
			m[index] = []Row{}
		}

		m[index] = append(m[index], row)
	}

	header := Header{}
	for i, col := range outCols {
		header[i] = defs[col]
	}
	header[len(header)] = &ColDef{
		Name:    countColName,
		Type:    TypInt,
		Dynamic: true,
	}

	var outRows []Row
	for _, grp := range m {
		if gt >= len(grp) {
			continue
		}

		var rec []string

		for _, col := range outCols {
			rec = append(rec, grp[0][col].ValStr())
		}

		rec = append(rec, strconv.Itoa(len(grp)))

		grpRow, err := NewRow(header, rec)
		if err != nil {
			return nil, nil, err
		}

		outRows = append(outRows, grpRow)
	}

	outDefs := ValueDefs{}
	for _, h := range header {
		outDefs[h.Name] = h
	}

	return outRows, outDefs, nil
}

var findDupesOp = Operation{
	Name:   "findDuplicates",
	OpFunc: opFindDuplicates,
	ArgDef: ArgDef{
		"indexCols":  reflect.TypeOf([]string{}),
		"outCols":    reflect.TypeOf([]string{}),
		"idCol":      reflect.TypeOf(""),
		"dupeIdsCol": reflect.TypeOf(""),
		"sep":        reflect.TypeOf(""),
	},
}

func opFindDuplicates(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	var err error

	var cols []string
	if cols, err = argSliceString(args, "indexCols"); err != nil {
		return nil, nil, err
	}

	var outCols []string
	if outCols, err = argSliceString(args, "outCols"); err != nil {
		return nil, nil, err
	}

	var idCol string
	if idCol, err = argString(args, "idCol"); err != nil {
		return nil, nil, err
	}

	var dupeIdsCol string
	if dupeIdsCol, err = argString(args, "dupeIdsCol"); err != nil {
		return nil, nil, err
	}

	var sep string
	if sep, err = argString(args, "sep"); err != nil {
		return nil, nil, err
	}

	m := map[string][]Row{}
	for _, row := range *rows {
		index := ""

		for _, col := range cols {
			index += row[col].ValStr()
		}

		if _, ok := m[index]; !ok {
			m[index] = []Row{}
		}

		m[index] = append(m[index], row)
	}

	header := Header{}
	for i, col := range outCols {
		header[i] = defs[col]
	}

	header[len(header)] = &ColDef{
		Name:    dupeIdsCol,
		Type:    TypStr,
		Dynamic: true,
	}

	var outRows []Row
	for _, grp := range m {
		if len(grp) == 1 {
			continue
		}
		var rec []string

		for _, col := range outCols {
			rec = append(rec, grp[0][col].ValStr())
		}

		var revs []string
		for i, grpItem := range grp {
			if i == 0 {
				continue
			}

			revs = append(revs, grpItem[idCol].ValStr())
		}
		rec = append(rec, strings.Join(revs, sep))

		grpRow, err := NewRow(header, rec)
		if err != nil {
			return nil, nil, err
		}

		outRows = append(outRows, grpRow)
	}

	outDefs := ValueDefs{}
	for _, h := range header {
		outDefs[h.Name] = h
	}

	return outRows, outDefs, nil
}

var mergeDupesOp = Operation{
	Name:   "mergeDupes",
	OpFunc: opMergeDupes,
	ArgDef: ArgDef{
		"indexCols":   reflect.TypeOf([]string{}),
		"outCols":     reflect.TypeOf([]string{}),
		"mergeValues": reflect.TypeOf(true),
	},
}

func opMergeDupes(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	var err error

	var cols []string
	if cols, err = argSliceString(args, "indexCols"); err != nil {
		return nil, nil, err
	}

	var outCols []string
	if outCols, err = argSliceString(args, "outCols"); err != nil {
		return nil, nil, err
	}

	var mergeValues bool
	if mergeValues, err = argBool(args, "mergeValues"); err != nil {
		return nil, nil, err
	}

	m := map[string][]Row{}

	// building the indexes and mapping them to their respective rows
	for _, row := range *rows {
		index := ""

		for _, col := range cols {
			index += row[col].ValStr()
		}

		if _, ok := m[index]; !ok {
			m[index] = []Row{}
		}

		m[index] = append(m[index], row)
	}

	// preparing the new output header defs
	header := Header{}
	for i, col := range outCols {
		header[i] = defs[col]
	}

	var outRows []Row
	for _, grp := range m {
		var rec []string

		for _, col := range outCols {
			for gi, grpItem := range grp {
				val := grpItem[col].ValStr()

				if mergeValues && val == "" && gi < len(grp)-1 {
					continue
				}

				rec = append(rec, val)
				break
			}
		}

		grpRow, err := NewRow(header, rec)
		if err != nil {
			return nil, nil, err
		}

		outRows = append(outRows, grpRow)
	}

	outDefs := ValueDefs{}
	for _, h := range header {
		outDefs[h.Name] = h
	}

	return outRows, outDefs, nil
}

var md5FileOp = Operation{
	Name:   "filesMd5",
	OpFunc: opMd5File,
	ArgDef: ArgDef{
		"filenameCol": reflect.TypeOf(""),
		"md5Col":      reflect.TypeOf(""),
		"outCols":     reflect.TypeOf([]string{}),
		"threads":     reflect.TypeOf(1),
	},
}

func opMd5File(rows *[]Row, defs ValueDefs, args FuncArgs) ([]Row, ValueDefs, error) {
	var err error

	var filenameCol string
	if filenameCol, err = argString(args, "filenameCol"); err != nil {
		return nil, nil, err
	}

	var md5Col string
	if md5Col, err = argString(args, "md5Col"); err != nil {
		return nil, nil, err
	}

	var outCols []string
	if outCols, err = argSliceString(args, "outCols"); err != nil {
		return nil, nil, err
	}

	var threads int
	if threads, err = argInt(args, "threads"); err != nil {
		return nil, nil, err
	}

	header := Header{}
	for i, col := range outCols {
		header[i] = defs[col]
	}
	md5ColDef := &ColDef{
		Name:    md5Col,
		Type:    TypStr,
		Dynamic: true,
	}
	header[len(header)] = md5ColDef

	cpRows := *rows
	tot := len(cpRows)

	if tot < threads {
		threads = tot
	}

	var ch = make(chan int, threads)
	var wg sync.WaitGroup

	wg.Add(tot)

	for _, row := range cpRows {
		go func(r Row) {
			ch <- 1

			filename := r[filenameCol].ValStr()

			// if file does not exist, we return an empty string
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				r[md5Col], _ = NewValue(md5ColDef, "")
				<-ch
				wg.Done()
				return
			}

			cnt, err := ioutil.ReadFile(filename)
			if err != nil {
				r[md5Col], _ = NewValue(md5ColDef, "")
				<-ch
				wg.Done()
				return
			}

			fMd5 := md5.Sum(cnt)
			md5Str := hex.EncodeToString(fMd5[:])

			r[md5Col], _ = NewValue(md5ColDef, md5Str)

			<-ch
			wg.Done()
		}(row)
	}

	wg.Wait()

	outDefs := ValueDefs{}
	for _, h := range header {
		outDefs[h.Name] = h
	}

	return cpRows, outDefs, nil
}
