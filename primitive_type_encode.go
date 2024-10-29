package healer

import (
	"encoding/binary"
	"io"
)

func encodeNullableString(s string) []byte {
	payload := make([]byte, binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(1+len(s)))
	offset += copy(payload[offset:], s)
	return payload[:offset]
}

func writeNullableString(w io.Writer, s string) {
	payload := make([]byte, binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(1+len(s)))
	w.Write(payload[:offset])
	w.Write([]byte(s))
}
