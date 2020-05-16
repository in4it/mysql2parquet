package parquet

import (
	"github.com/xitongsys/parquet-go-source/local"
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

func (p *ParquetReader) ReadColumn(columnName string) ([]interface{}, error) {
	num := int(p.pr.GetNumRows())
	res, err := p.pr.ReadPartialByNumber(num, "parquet_go_root."+columnName)
	if err != nil {
		return res, err
	}

	return res, nil
}
