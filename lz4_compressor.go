package healer

import (
	"bytes"

	"github.com/pierrec/lz4"
)

type LZ4Compressor struct {
}

func (c *LZ4Compressor) Compress(value []byte) ([]byte, error) {
	var (
		buf bytes.Buffer
		err error
	)
	writer := lz4.NewWriter(&buf)
	if _, err = writer.Write(value); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
