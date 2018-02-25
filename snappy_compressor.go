package healer

import "github.com/eapache/go-xerial-snappy"

type SnappyCompressor struct {
}

func (c *SnappyCompressor) Compress(value []byte) ([]byte, error) {
	return snappy.Encode(value), nil
}
