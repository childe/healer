package healer

import (
	"bytes"
	"compress/gzip"
)

type GzipCompressor struct {
}

func (c *GzipCompressor) Compress(value []byte) ([]byte, error) {
	var (
		buf bytes.Buffer
		err error
	)
	writer := gzip.NewWriter(&buf)
	if _, err = writer.Write(value); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
