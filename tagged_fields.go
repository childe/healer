package healer

import (
	"bytes"
	"encoding/binary"
)

type TaggedFields []TaggedField

type TaggedField struct {
	Tag  int
	Data []byte
}

func (r TaggedFields) Encode() []byte {
	buf := new(bytes.Buffer)
	payload := make([]byte, binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(len(r)))
	buf.Write(payload[:offset])

	for _, v := range r {
		buf.Write(v.Encode())
	}
	return buf.Bytes()
}

func (r *TaggedField) Encode() []byte {
	payload := make([]byte, len(r.Data)+binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(r.Tag))
	n := copy(payload[offset:], r.Data)
	return payload[:offset+n]
}
