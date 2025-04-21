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
