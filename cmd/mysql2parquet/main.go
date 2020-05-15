package main

import (
	"fmt"

	"github.com/in4it/mysql2parquet/pkg/mysql"
	"github.com/in4it/mysql2parquet/pkg/parquet"
)

func main() {
	// do mysql query
	m := mysql.New()
	m.Init("root:secret@tcp(127.0.0.1:3306)/test")
	m.Query("select * from test")

	// initialize parquet file and schema
	columnNames, columnTypes := m.GetColumnInfo()
	schema := getSchema(columnNames, columnTypes)
	p := parquet.New()
	p.Open("filename.parquet", schema)
	p.Close()

	for _, v := range m.GetRow() {
		fmt.Printf("%s (%s): %s\n", v.RowName, v.RowType, v.RowData)
	}
	m.RowClose()
	m.Close()

}

func getSchema(columnNames, columnTypes []string) []string {
	ret := []string{}
	for k, v := range columnNames {
		parquetType := ""
		switch columnTypes[k] {
		case "VARCHAR":
			parquetType = "UTF8"
		}
		if parquetType == "UTF8" {
			ret = append(ret, fmt.Sprint("name=%s, type=%, encoding=PLAIN_DICTIONARY", v, parquetType))

		} else {
			ret = append(ret, fmt.Sprint("name=%s, type=%s", v, parquetType))
		}
	}
	return ret
}
