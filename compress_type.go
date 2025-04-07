package healer

import (
	"fmt"
	"strings"
)

// CompressType represents the compression algorithm used in Kafka messages
type CompressType int8

const (
	CompressionNone   CompressType = 0
	CompressionGzip   CompressType = 1
	CompressionSnappy CompressType = 2
	CompressionLz4    CompressType = 3
)

// String returns the string representation of CompressType
func (c CompressType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionGzip:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLz4:
		return "lz4"
	default:
		return "unknown"
	}
}

// ParseCompressType converts a string to CompressType
func ParseCompressType(s string) (CompressType, error) {
	switch strings.ToLower(s) {
	case "none":
		return CompressionNone, nil
	case "gzip":
		return CompressionGzip, nil
	case "snappy":
		return CompressionSnappy, nil
	case "lz4":
		return CompressionLz4, nil
	default:
		return CompressionNone, fmt.Errorf("unknown compression type: %s", s)
	}
}
