package main

import (
	"flag"
	"fmt"
	"reflect"

	"github.com/in4it/mysql2parquet/pkg/parquet"
)

func main() {
	var (
		filename string
		column   string
	)

	flag.StringVar(&filename, "filename", "", "input to read")
	flag.StringVar(&column, "column", "", "column to read")

	flag.Parse()
	p := parquet.NewReader()
	p.Open(filename)
	result, err := p.ReadColumn(column)
	if err != nil {
		panic(err)
	}
	for _, v := range result {
		switch reflect.TypeOf(v).String() {
		case "*int32":
			fmt.Printf("%d\n", *v.(*int32))
		case "*string":
			fmt.Printf("%s\n", *v.(*string))
		case "*float32":
			fmt.Printf("%f\n", *v.(*float32))
		default:
			fmt.Printf("Type not recognized: %s\n", reflect.TypeOf(v).String())
		}
	}

}
