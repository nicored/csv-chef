package main

import (
	"github.com/nicored/csv-chef/csv"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Config struct {
	JsParser   []string             `yaml:"jsParsers"`
	Cols       []*csv.ColDef        `yaml:"cols"`
	Operations []*csv.OperationConf `yaml:"operations"`
}

type Data struct {
	Config    *Config
	ValueDefs csv.ValueDefs

	configFile string
	csvFile    string
}

func main() {
	if len(os.Args) != 3 {
		logrus.Fatal("expecting 2 arguments, the configuration file and the csv file. eg. csv-chef myconfig.yml mycsv.csv")
	}

	d, err := NewData(os.Args[1], os.Args[2])
	if err != nil {
		logrus.Fatal(err)
	}

	d.Do()
}

func NewData(configFile string, csvFile string) (data *Data, err error) {
	data = &Data{
		configFile: configFile,
		csvFile:    csvFile,
	}

	if err = data.parseConfig(); err != nil {
		return
	}

	return
}

func (d *Data) Do() error {
	_, err := csv.ReadCsv(d.csvFile, d.ValueDefs, d.Config.Operations)
	if err != nil {
		return err
	}

	return nil
}

func (d *Data) parseConfig() error {
	content, err := ioutil.ReadFile(d.configFile)
	if err != nil {
		return err
	}

	conf := &Config{}
	err = yaml.Unmarshal(content, conf)
	if err != nil {
		return err
	}

	d.Config = conf

	if err = d.parseColDefs(); err != nil {
		return err
	}

	return d.importJsParsers()
}

func (d *Data) importJsParsers() error {
	for _, jsFilepath := range d.Config.JsParser {
		parser, err := csv.NewJSParser(jsFilepath)
		if err != nil {
			return err
		}

		if err = csv.AddParsers(parser); err != nil {
			return err
		}
	}

	return nil
}

func (d *Data) parseColDefs() (err error) {
	def := csv.ValueDefs{}

	for _, c := range d.Config.Cols {
		def[c.Name] = c
	}

	d.ValueDefs = def
	return nil
}