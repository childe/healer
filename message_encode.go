package healer

import (
	"bytes"
	"encoding/binary"
)

// only used in unit test
func (r *FetchResponse) Encode(version uint16) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
		return nil, err
	}

	// Encode ThrottleTimeMs
	if err := binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}

	// Encode ErrorCode
	if err := binary.Write(buf, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}

	// Encode SessionID
	if err := binary.Write(buf, binary.BigEndian, r.SessionID); err != nil {
		return nil, err
	}

	// Encode Responses
	if err := binary.Write(buf, binary.BigEndian, int32(len(r.Responses))); err != nil {
		return nil, err
	}

	for topic, partitions := range r.Responses {
		// Encode topic
		if err := binary.Write(buf, binary.BigEndian, int16(len(topic))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(topic); err != nil {
			return nil, err
		}

		// Encode partitions
		if err := binary.Write(buf, binary.BigEndian, int32(len(partitions))); err != nil {
			return nil, err
		}

		for _, partition := range partitions {
			// Encode PartitionIndex
			if err := binary.Write(buf, binary.BigEndian, partition.PartitionID); err != nil {
				return nil, err
			}

			// Encode ErrorCode
			if err := binary.Write(buf, binary.BigEndian, partition.ErrorCode); err != nil {
				return nil, err
			}

			// Encode HighWatermark
			if err := binary.Write(buf, binary.BigEndian, partition.HighWatermark); err != nil {
				return nil, err
			}

			// Encode LastStableOffset
			if err := binary.Write(buf, binary.BigEndian, partition.LastStableOffset); err != nil {
				return nil, err
			}

			// Encode LogStartOffset
			if err := binary.Write(buf, binary.BigEndian, partition.LogStartOffset); err != nil {
				return nil, err
			}

			// Encode AbortedTransactions
			if err := binary.Write(buf, binary.BigEndian, int32(len(partition.AbortedTransactions))); err != nil {
				return nil, err
			}

			for _, tx := range partition.AbortedTransactions {
				if err := binary.Write(buf, binary.BigEndian, tx.ProducerID); err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.BigEndian, tx.FirstOffset); err != nil {
					return nil, err
				}
			}

			offset := buf.Len()
			binary.Write(buf, binary.BigEndian, uint32(0)) // placeholder for RecordBatchesLength

			for _, recordBatch := range partition.RecordBatches {
				recordBatchBytes, err := recordBatch.Encode(version)
				if err != nil {
					return nil, err
				}

				if _, err := buf.Write(recordBatchBytes); err != nil {
					return nil, err
				}
			}
			// Update RecordBatchesLength
			binary.BigEndian.PutUint32(buf.Bytes()[offset:], uint32(buf.Len()-offset-4))
		}
	}

	return buf.Bytes(), nil
}

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
	if err := binary.Write(buf, binary.BigEndian, r.CRC); err != nil {
		return nil, err
	}
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

	compress := CompressType(r.Attributes & 0x1)

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

// Encode encodes a record to a byte slice
// just for test
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

	// Encode key length
	keyLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(keyLenBuf, int64(len(r.key)))
	if _, err := buf.Write(keyLenBuf[:n]); err != nil {
		return nil, err
	}

	// Write key
	if len(r.key) > 0 {
		if _, err := buf.Write(r.key); err != nil {
			return nil, err
		}
	}

	// Encode value length
	valueLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(valueLenBuf, int64(len(r.value)))
	if _, err := buf.Write(valueLenBuf[:n]); err != nil {
		return nil, err
	}

	// Write value
	if len(r.value) > 0 {
		if _, err := buf.Write(r.value); err != nil {
			return nil, err
		}
	}

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
