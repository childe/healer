package healer

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// RecordHeader is concluded in Record
type RecordHeader struct {
	Key   string
	Value []byte
}

func decodeHeader(payload []byte) (header RecordHeader, offset int) {
	keyLength, o := binary.Varint(payload[offset:])
	offset += o
	header.Key = string(payload[offset : offset+int(keyLength)])
	offset += int(keyLength)

	valueLength, o := binary.Varint(payload[offset:])
	offset += o
	header.Value = make([]byte, valueLength)
	offset += copy(header.Value, payload[offset:offset+int(valueLength)])
	return
}

// Record is one message in fetch response with magic byte 2
type Record struct {
	attributes     int8
	timestampDelta int64
	offsetDelta    int32
	key            []byte
	value          []byte
	Headers        []RecordHeader
}

// Encode encodes a record to a byte slice
func (r *Record) Encode(version uint16) (payload []byte, err error) {
	// First encode everything except the length field
	buf := new(bytes.Buffer)

	// Encode attributes
	if err := buf.WriteByte(byte(r.attributes)); err != nil {
		return nil, err
	}

	// Encode timestampDelta
	timeBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(timeBuf, r.timestampDelta)
	if _, err := buf.Write(timeBuf[:n]); err != nil {
		return nil, err
	}

	// Encode offsetDelta
	offsetBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(offsetBuf, int64(r.offsetDelta))
	if _, err := buf.Write(offsetBuf[:n]); err != nil {
		return nil, err
	}

	// Encode key

	keyLenBuf := make([]byte, binary.MaxVarintLen64)
	if r.key == nil {
		binary.PutVarint(keyLenBuf, -1)
	} else {
		n = binary.PutVarint(keyLenBuf, int64(len(r.key)))
	}
	buf.Write(keyLenBuf[:n])
	buf.Write(r.key)

	// Encode value
	valueLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(valueLenBuf, int64(len(r.value)))
	buf.Write(valueLenBuf[:n])
	buf.Write(r.value)

	// Encode Headers count
	headerCountBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(headerCountBuf, int64(len(r.Headers)))
	if _, err := buf.Write(headerCountBuf[:n]); err != nil {
		return nil, err
	}

	// Encode each Header
	for _, header := range r.Headers {
		// Encode Key length
		keyLenBuf := make([]byte, binary.MaxVarintLen64)
		n = binary.PutVarint(keyLenBuf, int64(len(header.Key)))
		if _, err := buf.Write(keyLenBuf[:n]); err != nil {
			return nil, err
		}

		// Write Key
		if _, err := buf.WriteString(header.Key); err != nil {
			return nil, err
		}

		// Encode Value length
		valueLenBuf := make([]byte, binary.MaxVarintLen64)
		n = binary.PutVarint(valueLenBuf, int64(len(header.Value)))
		if _, err := buf.Write(valueLenBuf[:n]); err != nil {
			return nil, err
		}

		// Write Value
		if _, err := buf.Write(header.Value); err != nil {
			return nil, err
		}
	}

	// Get the encoded content
	recordBytes := buf.Bytes()

	// Create final buffer including length
	finalBuf := new(bytes.Buffer)

	// Encode length (not including the length field itself)
	lengthBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(lengthBuf, int64(len(recordBytes)))
	if _, err := finalBuf.Write(lengthBuf[:n]); err != nil {
		return nil, err
	}

	// Write the remaining record
	if _, err := finalBuf.Write(recordBytes); err != nil {
		return nil, err
	}

	return finalBuf.Bytes(), nil
}

// DecodeToRecord decodes the struct Record from the given payload.
func DecodeToRecord(payload []byte) (record Record, offset int, err error) {
	length, o := binary.Varint(payload)
	if length == 0 || len(payload[o:]) < int(length) {
		return record, o, errUncompleteRecord
	}
	offset += o

	record.attributes = int8(payload[offset])
	offset++

	timestampDelta, o := binary.Varint(payload[offset:])
	record.timestampDelta = int64(timestampDelta)
	offset += o

	offsetDelta, o := binary.Varint(payload[offset:])
	record.offsetDelta = int32(offsetDelta)
	offset += o

	keyLength, o := binary.Varint(payload[offset:])
	offset += o

	if keyLength > 0 {
		record.key = make([]byte, keyLength)
		offset += copy(record.key, payload[offset:offset+int(keyLength)])
	}

	valueLen, o := binary.Varint(payload[offset:])
	offset += o
	if valueLen > 0 {
		record.value = make([]byte, valueLen)
		offset += copy(record.value, payload[offset:offset+int(valueLen)])
	}

	headerCount, o := binary.Varint(payload[offset:])
	offset += o
	record.Headers = make([]RecordHeader, headerCount)
	for i := range record.Headers {
		record.Headers[i], o = decodeHeader(payload[offset:])
		offset += o
	}

	return
}

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	/**
	The CRC covers the data from the attributes to the end of the batch
	(i.e. all the bytes that follow the CRC). It is located after the magic byte,
	which means that clients must parse the magic byte before deciding how to
	interpret the bytes between the batch length and the magic byte.
	The partition leader epoch field is not included in the CRC computation to
	avoid the need to recompute the CRC when this field is assigned for every
	batch that is received by the broker. The CRC-32C (Castagnoli) polynomial
	is used for the computation.
	**/
	CRC             uint32
	Attributes      int16
	LastOffsetDelta int32
	BaseTimestamp   int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	Records         []*Record
}

// Encode encodes the RecordBatch to a byte slice.
// The first 8 bytes are reserved for the length of the batch.
func (r *RecordBatch) Encode(version uint16) (payload []byte, err error) {
	buf := new(bytes.Buffer)

	// BatchLength is variable, so reserve space for it.
	defer func() {
		binary.BigEndian.PutUint32(payload[8:], uint32(buf.Len()-12)) // 12 is baseoffset + batchlength
	}()

	// Encode RecordBatch
	if err := binary.Write(buf, binary.BigEndian, r.BaseOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.BatchLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.PartitionLeaderEpoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Magic); err != nil {
		return nil, err
	}

	crcOffset := buf.Len()
	binary.Write(buf, binary.BigEndian, r.CRC)
	defer func() {
		crc := crc32.Checksum(payload[crcOffset+4:], castagnoliTable)
		binary.BigEndian.PutUint32(payload[crcOffset:], crc)
	}()

	if err := binary.Write(buf, binary.BigEndian, r.Attributes); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.LastOffsetDelta); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.BaseTimestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.MaxTimestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.ProducerID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.ProducerEpoch); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.BaseSequence); err != nil {
		return nil, err
	}

	// Encode Records
	if err := binary.Write(buf, binary.BigEndian, int32(len(r.Records))); err != nil {
		return nil, err
	}

	compress := CompressType(r.Attributes & 0b11)

	if compress == CompressionNone {
		for _, record := range r.Records {
			recordBytes, err := record.Encode(version)
			if err != nil {
				return nil, err
			}
			if _, err := buf.Write(recordBytes); err != nil {
				return nil, err
			}
		}
	} else {
		compressor := NewCompressor(compress.String())
		b := new(bytes.Buffer)
		for _, record := range r.Records {
			recordBytes, err := record.Encode(version)
			if err != nil {
				return nil, err
			}
			if _, err := b.Write(recordBytes); err != nil {
				return nil, err
			}
		}

		compressed, err := compressor.Compress(b.Bytes())
		if err != nil {
			return nil, err
		}

		if _, err := buf.Write(compressed); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// DecodeToRecord decodes the struct Record from the given payload.
// only used in test
func DecodeToRecordBatch(payload []byte) (r RecordBatch, offset int, err error) {
	r.BaseOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.BatchLength = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.Magic = int8(payload[offset]) // only 2 supported for now
	offset++

	r.CRC = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Attributes = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.LastOffsetDelta = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.BaseTimestamp = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.MaxTimestamp = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.ProducerID = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.ProducerEpoch = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.BaseSequence = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	recordCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Records = make([]*Record, recordCount)
	for i := range r.Records {
		record, o, err := DecodeToRecord(payload[offset:])
		offset += o
		if err != nil {
			return r, offset, err
		}
		r.Records[i] = &record
	}

	return r, offset, nil
}
