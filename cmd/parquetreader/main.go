package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

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
	result, parquetType, err := p.ReadColumn(column)
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}
	for _, v := range result {
		switch parquetType {
		case "DATE":
			tm := time.Unix(int64(*v.(*int32)), 0)
			fmt.Printf("%s\n", tm.Format("2006-01-02"))
		case "TIMESTAMP_MILLIS":
			tm := time.Unix(*v.(*int64), 0)
			fmt.Printf("%s\n", tm.Format("2006-01-02 15:04:05"))
		case "FLOAT":
			fmt.Printf("%f\n", *v.(*float32))
		case "DOUBLE":
			fmt.Printf("%f\n", *v.(*float64))
		case "INT32":
			fmt.Printf("%d\n", *v.(*int32))
		case "INT64":
			fmt.Printf("%d\n", *v.(*int64))
		case "BYTE_ARRAY":
			fmt.Printf("%s\n", *v.(*string))
		default:
			fmt.Printf("Type not recognized: %s - %s\n", parquetType, reflect.TypeOf(v).String())
		}
	}

}
