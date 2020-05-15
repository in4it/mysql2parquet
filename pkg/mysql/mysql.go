package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	db          *sql.DB
	queryResult QueryResult
}
type QueryResult struct {
	rows        *sql.Rows
	columnNames []string
	columnTypes []*sql.ColumnType
}

type GenericRow struct {
	rowName string
	rowType string
	rowData []byte
}

func New() *MySQL {
	return &MySQL{}
}
func (m *MySQL) Init(connectionString string) {
	var err error
	m.db, err = sql.Open("mysql", connectionString)
	if err != nil {
		panic(err.Error())
	}
}
func (m *MySQL) Close() {
	m.db.Close()
}

func (m *MySQL) RowClose() {
	m.queryResult.rows.Close()
}

func (m *MySQL) Query(queryString string) {
	var err error
	m.queryResult.rows, err = m.db.Query(queryString)
	if err != nil {
		panic(err.Error())
	}
	m.queryResult.columnNames, err = m.queryResult.rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	m.queryResult.columnTypes, err = m.queryResult.rows.ColumnTypes()
	if err != nil {
		panic(err.Error())
	}

}
func (m *MySQL) GetRow() []GenericRow {
	var ret []GenericRow
	vals := make([]interface{}, len(m.queryResult.columnNames))
	for i, _ := range m.queryResult.columnNames {
		vals[i] = new(sql.RawBytes)
	}
	for m.queryResult.rows.Next() {
		err := m.queryResult.rows.Scan(vals...)
		if err != nil {
			panic(err)
		}
		for k := range vals {
			ret = append(ret, GenericRow{
				rowName: m.queryResult.columnNames[k],
				rowType: m.queryResult.columnTypes[k].DatabaseTypeName(),
				rowData: *vals[k].(*sql.RawBytes),
			})
		}
	}
	return ret
}
