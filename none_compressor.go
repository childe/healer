package healer

type NoneCompressor struct {
}

func (c *NoneCompressor) Compress(value []byte) ([]byte, error) {
	return value, nil
}
