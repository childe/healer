package healer

import (
	"bytes"

	lz4 "github.com/pierrec/lz4/v4"
)

type LZ4Compressor struct {
}

func (c *LZ4Compressor) Compress(value []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	if _, err := writer.Write(value); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
