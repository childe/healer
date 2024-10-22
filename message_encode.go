package healer

import (
	"bytes"
	"encoding/binary"
)

func (r FetchResponse) Encode(version uint16) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, r.CorrelationID); err != nil {
		return nil, err
	}

	// 编码 ThrottleTimeMs
	if err := binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs); err != nil {
		return nil, err
	}

	// 编码 ErrorCode
	if err := binary.Write(buf, binary.BigEndian, r.ErrorCode); err != nil {
		return nil, err
	}

	// 编码 SessionID
	if err := binary.Write(buf, binary.BigEndian, r.SessionID); err != nil {
		return nil, err
	}

	// 编码 Responses
	if err := binary.Write(buf, binary.BigEndian, int32(len(r.Responses))); err != nil {
		return nil, err
	}

	for topic, partitions := range r.Responses {
		// 编码 topic
		if err := binary.Write(buf, binary.BigEndian, int16(len(topic))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(topic); err != nil {
			return nil, err
		}

		// 编码 partitions
		if err := binary.Write(buf, binary.BigEndian, int32(len(partitions))); err != nil {
			return nil, err
		}

		for _, partition := range partitions {
			// 编码 PartitionIndex
			if err := binary.Write(buf, binary.BigEndian, partition.PartitionID); err != nil {
				return nil, err
			}

			// 编码 ErrorCode
			if err := binary.Write(buf, binary.BigEndian, partition.ErrorCode); err != nil {
				return nil, err
			}

			// 编码 HighWatermark
			if err := binary.Write(buf, binary.BigEndian, partition.HighWatermark); err != nil {
				return nil, err
			}

			// 编码 LastStableOffset
			if err := binary.Write(buf, binary.BigEndian, partition.LastStableOffset); err != nil {
				return nil, err
			}

			// 编码 LogStartOffset
			if err := binary.Write(buf, binary.BigEndian, partition.LogStartOffset); err != nil {
				return nil, err
			}

			// 编码 AbortedTransactions
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

			recordBatchBytes, err := partition.RecordBatch.Encode(version)
			if err != nil {
				return nil, err
			}

			if err := binary.Write(buf, binary.BigEndian, int32(len(recordBatchBytes))); err != nil {
				return nil, err
			}

			if _, err := buf.Write(recordBatchBytes); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func (r *RecordBatch) Encode(version uint16) (payload []byte, err error) {
	buf := new(bytes.Buffer)

	// RecordBatch 的 BatchLength 是变长的，所以先预留空间
	defer func() {
		binary.BigEndian.PutUint32(payload[8:], uint32(buf.Len()-8))
	}()

	// 编码 RecordBatch
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

	// 编码 Records
	if err := binary.Write(buf, binary.BigEndian, int32(len(r.Records))); err != nil {
		return nil, err
	}
	for _, record := range r.Records {
		recordBytes, err := record.Encode(version)
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(recordBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// Encode encodes a record to a byte slice
// just for test
func (r *Record) Encode(version uint16) (payload []byte, err error) {
	buf := new(bytes.Buffer)
	defer func() {
		lengthBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutVarint(lengthBuf, int64(len(payload)))
		payload = append(lengthBuf[:n], payload...)
	}()

	// 编码 length

	// 编码 attributes
	attrBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(attrBuf, int64(r.attributes))
	if _, err := buf.Write(attrBuf[:n]); err != nil {
		return nil, err
	}

	// 编码 timestampDelta
	timeBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(timeBuf, r.timestampDelta)
	if _, err := buf.Write(timeBuf[:n]); err != nil {
		return nil, err
	}

	// 编码 offsetDelta
	offsetBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(offsetBuf, int64(r.offsetDelta))
	if _, err := buf.Write(offsetBuf[:n]); err != nil {
		return nil, err
	}

	// 编码 key 长度
	keyLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(keyLenBuf, int64(len(r.key)))
	if _, err := buf.Write(keyLenBuf[:n]); err != nil {
		return nil, err
	}

	// 写入 key
	if _, err := buf.Write(r.key); err != nil {
		return nil, err
	}

	// 编码 value 长度
	valueLenBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(valueLenBuf, int64(len(r.value)))
	if _, err := buf.Write(valueLenBuf[:n]); err != nil {
		return nil, err
	}

	// 写入 value
	if _, err := buf.Write(r.value); err != nil {
		return nil, err
	}

	// 编码 Headers 数量
	headerCountBuf := make([]byte, binary.MaxVarintLen64)
	n = binary.PutVarint(headerCountBuf, int64(len(r.Headers)))
	if _, err := buf.Write(headerCountBuf[:n]); err != nil {
		return nil, err
	}

	// 编码每个 Header
	for _, header := range r.Headers {
		// 编码 Key 长度
		keyLenBuf := make([]byte, binary.MaxVarintLen64)
		n = binary.PutVarint(keyLenBuf, int64(len(header.Key)))
		if _, err := buf.Write(keyLenBuf[:n]); err != nil {
			return nil, err
		}

		// 写入 Key
		if _, err := buf.WriteString(header.Key); err != nil {
			return nil, err
		}

		// 编码 Value 长度
		valueLenBuf := make([]byte, binary.MaxVarintLen64)
		n = binary.PutVarint(valueLenBuf, int64(len(header.Value)))
		if _, err := buf.Write(valueLenBuf[:n]); err != nil {
			return nil, err
		}

		// 写入 Value
		if _, err := buf.Write(header.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
