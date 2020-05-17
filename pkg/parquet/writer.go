package parquet

import (
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	fw source.ParquetFile
	pw *writer.CSVWriter
}

func NewWriter() *ParquetWriter {
	return &ParquetWriter{}
}
func (p *ParquetWriter) Open(filename string, schema []string, compression string) error {
	var err error
	p.fw, err = local.NewLocalFileWriter(filename)
	if err != nil {
		return err
	}
	p.pw, err = writer.NewCSVWriter(schema, p.fw, 4)
	if err != nil {
		return err
	}
	switch strings.ToLower(compression) {
	case "snappy":
		p.pw.CompressionType = parquet.CompressionCodec_SNAPPY
	case "gzip":
		p.pw.CompressionType = parquet.CompressionCodec_GZIP
	case "lzo":
		p.pw.CompressionType = parquet.CompressionCodec_LZO
	case "lz4":
		p.pw.CompressionType = parquet.CompressionCodec_LZ4
	case "brotli":
		p.pw.CompressionType = parquet.CompressionCodec_BROTLI
	}
	return nil
}
func (p *ParquetWriter) WriteLine(data []string) error {
	rec := make([]*string, len(data))
	for j := 0; j < len(data); j++ {
		rec[j] = &data[j]
	}
	if err := p.pw.WriteString(rec); err != nil {
		return err
	}
	return nil
}
func (p *ParquetWriter) Close() error {
	if err := p.pw.WriteStop(); err != nil {
		return err
	}
	p.fw.Close()
	return nil
}
