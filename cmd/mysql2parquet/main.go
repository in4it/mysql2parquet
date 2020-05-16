package main

import (
	"flag"
	"fmt"

	"github.com/in4it/mysql2parquet/pkg/mysql"
	"github.com/in4it/mysql2parquet/pkg/parquet"
)

func main() {
	var (
		connectionString string
		query            string
		out              string
	)

	flag.StringVar(&connectionString, "connectionString", "", "MySQL connectionstring")
	flag.StringVar(&query, "query", "", "query")
	flag.StringVar(&out, "out", "", "outputfile")

	flag.Parse()

	// do mysql query
	m := mysql.New()
	m.Init(connectionString)
	m.Query(query)

	// initialize parquet file and schema
	columnNames, columnTypes := m.GetColumnInfo()
	schema := getSchema(columnNames, columnTypes)
	p := parquet.NewWriter()
	p.Open(out, schema)

	for {
		row, nextRow := m.GetRow()
		if !nextRow {
			break
		}
		data := make([]string, len(columnNames))
		for k, v := range row {
			data[k] = string(v.RowData)
		}
		p.WriteLine(data)
	}

	m.RowClose()
	m.Close()
	p.Close()

}

func MySQLToParquetType(mysqlType string) string {
	parquetType := ""
	switch mysqlType {
	case "BOOLEAN":
		parquetType = "BOOL"
	case "INT":
		parquetType = "INT32"
	case "FLOAT":
		parquetType = "FLOAT"
	case "DOUBLE":
		parquetType = "DOUBLE"
	case "VARCHAR":
		parquetType = "UTF8"
	default:
		panic(fmt.Errorf("Encoding not found: %s", mysqlType))
	}
	return parquetType
}

func getSchema(columnNames, columnTypes []string) []string {
	ret := []string{}
	for k, v := range columnNames {
		parquetType := MySQLToParquetType(columnTypes[k])
		if parquetType == "UTF8" {
			ret = append(ret, fmt.Sprintf("name=%s, type=%s, encoding=PLAIN_DICTIONARY", v, parquetType))
		} else {
			ret = append(ret, fmt.Sprintf("name=%s, type=%s", v, parquetType))
		}
	}
	return ret
}
