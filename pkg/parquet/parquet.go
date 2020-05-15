package parquet

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type Parquet struct {
	fw source.ParquetFile
	pw *writer.CSVWriter
}

func New() *Parquet {
	return &Parquet{}
}
func (p *Parquet) Open(filename string, schema []string) error {
	var err error
	p.fw, err = local.NewLocalFileWriter(filename)
	if err != nil {
		return err
	}
	p.pw, err = writer.NewCSVWriter(schema, p.fw, 4)
	if err != nil {
		return err
	}
	return nil
}
func (p *Parquet) WriteLine(data []string) error {
	rec := make([]*string, len(data))
	for j := 0; j < len(data); j++ {
		rec[j] = &data[j]
	}
	if err := p.pw.WriteString(rec); err != nil {
		return err
	}
	return nil
}
func (p *Parquet) Close() error {
	if err := p.pw.WriteStop(); err != nil {
		return err
	}
	p.fw.Close()
	return nil
}
