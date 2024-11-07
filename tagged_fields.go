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

func (r *TaggedFields) length() (l int) {
	if r == nil {
		return 1
	}
	l = binary.MaxVarintLen64
	for _, v := range *r {
		l += v.length()
	}
	return
}

func (r TaggedFields) Encode() []byte {
	if r == nil {
		return []byte{0}
	}
	buf := new(bytes.Buffer)
	payload := make([]byte, binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(len(r)))
	buf.Write(payload[:offset])

	for _, v := range r {
		buf.Write(v.Encode())
	}
	return buf.Bytes()
}

func DecodeTaggedFields(payload []byte, version uint16) (r TaggedFields, length int) {
	offset := 0
	taggedFieldsLength, n := binary.Uvarint(payload)
	offset += n
	for i := uint64(0); i < taggedFieldsLength; i++ {
		tag, n := binary.Uvarint(payload[offset:])
		offset += n
		dataLength, n := binary.Uvarint(payload[offset:])
		offset += n
		data := payload[offset : offset+int(dataLength)]
		offset += int(dataLength)
		r = append(r, TaggedField{Tag: int(tag), Data: data})
	}
	return r, offset
}

func (r *TaggedField) length() int {
	return binary.MaxVarintLen32 + +binary.MaxVarintLen32 + len(r.Data)
}
func (r *TaggedField) Encode() []byte {
	payload := make([]byte, len(r.Data)+binary.MaxVarintLen64)
	offset := binary.PutUvarint(payload, uint64(r.Tag))
	offset += binary.PutUvarint(payload[offset:], uint64(len(r.Data)))
	n := copy(payload[offset:], r.Data)
	return payload[:offset+n]
}
