package mysql

import (
	"fmt"
	"testing"
)

const connString = "root:secret@tcp(127.0.0.1:3306)/test"

func initDB() {
	mysql := New()
	mysql.Init(connString)
	mysql.Query("DROP TABLE test")
	mysql.Query("CREATE TABLE test (id INT PRIMARY KEY NOT NULL, description NVARCHAR(255), amount FLOAT)")
	for i := 0; i < 100; i++ {
		f := float32(i) / 100
		mysql.Query(fmt.Sprintf("INSERT INTO test (id, description, amount) VALUES (%d, 'desc-%d', %f)", i, i, f))
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
