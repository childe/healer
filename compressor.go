package healer

type Compressor interface {
	Compress([]byte) ([]byte, error)
}

func NewCompressor(cType string) Compressor {
	switch cType {
	case "gzip":
		return &GzipCompressor{}
	case "lz4":
		return &LZ4Compressor{}
	case "snappy":
		return &SnappyCompressor{}
	case "none":
		return &NoneCompressor{}
	}
	return nil
}
