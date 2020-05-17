package parquet

import (
	"fmt"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type ParquetReader struct {
	fr source.ParquetFile
	pr *reader.ParquetReader
}

func NewReader() *ParquetReader {
	return &ParquetReader{}
}
func (p *ParquetReader) Open(filename string) error {
	var err error

	p.fr, err = local.NewLocalFileReader(filename)
	if err != nil {
		return err
	}

	p.pr, err = reader.NewParquetReader(p.fr, nil, 4)
	if err != nil {
		return err
	}
	return nil
}

func (p *ParquetReader) GetAvailableColumns(m map[string]int32) []string {
	var ret []string
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func (p *ParquetReader) isConvertedType(convertedType *parquet.ConvertedType) (string, bool) {
	if convertedType == nil {
		return "", false
	}
	switch convertedType.String() {
	case "DATE":
		return "DATE", true
	case "TIMESTAMP_MILLIS":
		return "TIMESTAMP_MILLIS", true
	default:
		return "", false
	}
}

func (p *ParquetReader) ReadColumn(columnName string) ([]interface{}, string, error) {
	// determine schema
	fullColumnName := "Parquet_go_root." + columnName
	var (
		index       int32
		parquetType string
		ok          bool
	)
	if index, ok = p.pr.SchemaHandler.MapIndex[fullColumnName]; !ok {
		return nil, "", fmt.Errorf("column %s doesn't exist\nAvailable columns: %s", fullColumnName, strings.Join(p.GetAvailableColumns(p.pr.SchemaHandler.MapIndex), ", "))
	}
	if val, convertedType := p.isConvertedType(p.pr.SchemaHandler.SchemaElements[index].ConvertedType); convertedType {
		parquetType = val
	} else {
		if parquetType == "BYTE_ARRAY" {
			parquetType = p.pr.SchemaHandler.SchemaElements[index].ConvertedType.String()
		} else {
			parquetType = p.pr.SchemaHandler.SchemaElements[index].Type.String()
		}
	}

	// return value
	num := int(p.pr.GetNumRows())
	res, err := p.pr.ReadPartialByNumber(num, fullColumnName)
	if err != nil {
		return res, parquetType, err
	}

	return res, parquetType, nil
}
