package mysql

import (
	"fmt"
	"testing"
)

func TestQuery(t *testing.T) {
	mysql := New()
	mysql.Init("root:secret@tcp(127.0.0.1:3306)/test")
	mysql.Query("select * from test")
	for _, v := range mysql.GetRow() {
		fmt.Printf("%s (%s): %s\n", v.RowName, v.RowType, v.RowData)
	}
	mysql.RowClose()
	mysql.Close()

}
