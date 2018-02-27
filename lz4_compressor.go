package healer

import "github.com/bkaradzic/go-lz4"

type LZ4Compressor struct {
}

func (c *LZ4Compressor) Compress(value []byte) ([]byte, error) {
	return lz4.Encode(nil, value)
}
