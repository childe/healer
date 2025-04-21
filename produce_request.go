package healer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

type produceRequestP struct {
	Partition         int32
	RecordBatchesSize int32
	RecordBatches     []RecordBatch
	TaggedFields      TaggedFields `json:"tagged_fields"`
}

type produceRequestT struct {
	TopicName      string
	PartitonBlocks []produceRequestP
	TaggedFields   TaggedFields `json:"tagged_fields"`
}

type ProduceRequest struct {
	*RequestHeader
	transactionalID *string `healer:"minVersion:3"`
	RequiredAcks    int16
	Timeout         int32
	TopicBlocks     []produceRequestT
	TaggedFields    TaggedFields `json:"tagged_fields"`
}

var tagsCacheProduceRequest atomic.Value

func (r *ProduceRequest) tags() (fieldsVersions map[string]uint16) {
	if v := tagsCacheProduceRequest.Load(); v != nil {
		return v.(map[string]uint16)
	}

	fieldsVersions = healerTags(*r)
	tagsCacheProduceRequest.Store(fieldsVersions)
	return
}

func (p *produceRequestP) encode(w io.Writer, version uint16, isFlexible bool) error {
	binary.Write(w, binary.BigEndian, p.Partition)
	messageSetSize := 0

	// TODO if w is bytes.Buffer
	recordBatchesPayload := bytes.NewBuffer(nil)
	for _, batches := range p.RecordBatches {
		payload, err := batches.Encode(version)
		if err != nil {
			return err
		}
		recordBatchesPayload.Write(payload)
		messageSetSize += len(payload)
	}

	if isFlexible {
		binary.Write(w, binary.BigEndian, encodeCompactArrayLength(messageSetSize))
	} else {
		binary.Write(w, binary.BigEndian, int32(messageSetSize))
	}

	io.Copy(w, recordBatchesPayload)

	if isFlexible {
		binary.Write(w, binary.BigEndian, p.TaggedFields.Encode())
	}

	return nil
}

func (t *produceRequestT) encode(w io.Writer, version uint16, isFlexible bool) error {
	if isFlexible {
		writeCompactString(w, t.TopicName)
	} else {
		writeString(w, t.TopicName)
	}

	if isFlexible {
		binary.Write(w, binary.BigEndian, encodeCompactArrayLength(len(t.PartitonBlocks)))
	} else {
		binary.Write(w, binary.BigEndian, int32(len(t.PartitonBlocks)))
	}

	for _, partitionBlock := range t.PartitonBlocks {
		if err := partitionBlock.encode(w, version, isFlexible); err != nil {
			return err
		}
	}

	if isFlexible {
		binary.Write(w, binary.BigEndian, t.TaggedFields.Encode())
	}

	return nil
}

func (r *ProduceRequest) Encode(version uint16) (payload []byte) {
	tags := r.tags()

	buf := new(bytes.Buffer)

	var (
		err error
	)

	isFlexible := r.IsFlexible()

	// update length
	binary.Write(buf, binary.BigEndian, uint32(0))
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(buf.Len()-4))
	}()

	if _, err := r.RequestHeader.WriteTo(buf); err != nil {
		return nil
	}

	if r.APIVersion >= tags["StatesFilter"] {
		if isFlexible {
			_, err = writeCompactNullableString(buf, r.transactionalID)
		} else {
			_, err = writeNullableString(buf, r.transactionalID)
		}
		if err != nil {
			return
		}
	}

	binary.Write(buf, binary.BigEndian, uint16(r.RequiredAcks))
	binary.Write(buf, binary.BigEndian, uint32(r.Timeout))

	if isFlexible {
		binary.Write(buf, binary.BigEndian, encodeCompactArrayLength(len(r.TopicBlocks)))
	} else {
		binary.Write(buf, binary.BigEndian, uint32(len(r.TopicBlocks)))
	}

	for _, t := range r.TopicBlocks {
		t.encode(buf, version, isFlexible)
	}

	if isFlexible {
		binary.Write(buf, binary.BigEndian, r.TaggedFields.Encode())
	}

	return buf.Bytes()
}

// only used in test
func (r *ProduceRequest) Decode(data []byte, version uint16) error {
	offset := 0
	var o int

	length := binary.BigEndian.Uint32(data[offset:])
	if length+4 != uint32(len(data)) {
		return fmt.Errorf("produce request length do not match: %d!=%d", length+4, len(data))
	}
	offset += 4

	// Decode RequestHeader
	header, o := DecodeRequestHeader(data[offset:])
	r.RequestHeader = &header
	offset += o

	isFlexible := r.IsFlexible()

	// Decode transactionalID if applicable
	if r.APIVersion >= r.tags()["StatesFilter"] {
		if isFlexible {
			r.transactionalID, o = compactNullableString(data[offset:])
		} else {
			r.transactionalID, o = nullableString(data[offset:])
		}
		offset += o
	}

	// Decode RequiredAcks
	r.RequiredAcks = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Decode Timeout
	r.Timeout = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Decode TopicBlocks
	var topicCount int
	if isFlexible {
		c, o := compactArrayLength(data[offset:])
		offset += o
		topicCount = int(c)
	} else {
		topicCount = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}

	r.TopicBlocks = make([]produceRequestT, topicCount)

	for i := range topicCount {
		if isFlexible {
			s, o := compactString(data[offset:])
			offset += o
			r.TopicBlocks[i].TopicName = s
		} else {
			s, o := nonnullableString(data[offset:])
			offset += o
			r.TopicBlocks[i].TopicName = s
		}

		var partitionCount int
		if isFlexible {
			c, o := compactArrayLength(data[offset:])
			offset += o
			partitionCount = int(c)
		} else {
			partitionCount = int(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
		}

		r.TopicBlocks[i].PartitonBlocks = make([]produceRequestP, partitionCount)

		for j := range partitionCount {
			p := &r.TopicBlocks[i].PartitonBlocks[j]

			p.Partition = int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4

			if isFlexible {
				c, o := compactArrayLength(data[offset:])
				offset += o
				p.RecordBatchesSize = int32(c)
			} else {
				p.RecordBatchesSize = int32(binary.BigEndian.Uint32(data[offset:]))
				offset += 4
			}

			// offset += int(p.RecordBatchesSize)

			// 解码记录批次
			recordBatch, o, _ := DecodeToRecordBatch(data[offset:])
			p.RecordBatches = append(p.RecordBatches, recordBatch)
			offset += o

			if isFlexible {
				tf, o := DecodeTaggedFields(data[offset:])
				p.TaggedFields = tf
				offset += o
			}
		}

		if isFlexible {
			tf, o := DecodeTaggedFields(data[offset:])
			r.TopicBlocks[i].TaggedFields = tf
			offset += o
		}
	}
	if isFlexible {
		tf, o := DecodeTaggedFields(data[offset:])
		r.TaggedFields = tf
		offset += o
	}

	return nil
}
