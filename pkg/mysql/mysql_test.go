package mysql

import (
	"fmt"
	"testing"
)

const connString = "root:secret@tcp(127.0.0.1:3306)/test"

func initDB() {
	mysql := New()
	mysql.Init(connString)
	mysql.Query("DROP TABLE IF EXISTS test")
	mysql.Query("CREATE TABLE test (id INT PRIMARY KEY NOT NULL, description NVARCHAR(255), amount FLOAT, orderdate DATE, ordertime DATETIME, ordertimestamp TIMESTAMP)")
	for i := 0; i < 10; i++ {
		f := float32(i) / 100
		mysql.Query(fmt.Sprintf("INSERT INTO test (id, description, amount, orderdate, ordertime, ordertimestamp) VALUES (%d, 'desc-%d', %f, NOW(), NOW(), NOW())", i, i, f))
	}
	mysql.Close()
}
func TestQuery(t *testing.T) {
	initDB()
	mysql := New()
	mysql.Init(connString)
	mysql.Query("select * from test")

	columnNames, _ := mysql.GetColumnInfo()

	for i := 0; i < 10; i++ {
		row, nextRow := mysql.GetRow()
		if !nextRow {
			break
		}
		data := make([]string, len(columnNames))
		for k, v := range row {
			data[k] = string(v.RowData)
		}
		fmt.Printf("%+v\n", data)
	}

	mysql.RowClose()
	mysql.Close()

}
