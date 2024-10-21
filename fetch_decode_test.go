package healer

import (
	"bytes"
	"context"
	"testing"
)

func TestFetchResponseDecode(t *testing.T) {
	resp := FetchResponse{
		CorrelationID:  1,
		ThrottleTimeMs: 2,
		ErrorCode:      0,
		SessionID:      3,
		Responses: map[string][]PartitionResponse{
			"test-topic": {
				{
					PartitionIndex:      1,
					ErrorCode:           0,
					HighWatermark:       90,
					LastStableOffset:    100,
					LogStartOffset:      110,
					AbortedTransactions: nil,
					RecordBatch: RecordBatch{
						BaseOffset:           120,
						PartitionLeaderEpoch: 0,
						Magic:                2,
						CRC:                  123456,
						Attributes:           0,
						LastOffsetDelta:      10,
						BaseTimestamp:        1630000000000,
						MaxTimestamp:         1630000010000,
						ProducerID:           1000,
						ProducerEpoch:        1,
						BaseSequence:         0,
						Records: []Record{
							{
								length:         20,
								attributes:     0,
								timestampDelta: 1000,
								offsetDelta:    0,
								keyLength:      5,
								key:            []byte("key-1"),
								valueLen:       7,
								value:          []byte("value-1"),
								Headers:        []RecordHeader{},
							},
							{
								length:         20,
								attributes:     0,
								timestampDelta: 2000,
								offsetDelta:    1,
								keyLength:      5,
								key:            []byte("key-2"),
								valueLen:       7,
								value:          []byte("value-2"),
								Headers:        []RecordHeader{},
							},
						},
					},
				},
			},
		},
	}
	payload, err := resp.Encode(10)
	if err != nil {
		t.Error(err)
	}
	t.Logf("payload length: %d", len(payload))

	reader := bytes.NewReader(payload)
	messages := make(chan *FullMessage, 10)
	decoder := fetchResponseStreamDecoder{
		ctx:         context.Background(),
		buffers:     reader,
		messages:    messages,
		totalLength: len(payload) + 4,
		version:     10,
	}

	startOffset := int64(0)
	err = decoder.streamDecode(context.Background(), startOffset)
	if err != nil {
		t.Error(err)
	}

	i := 0
	for msg := range messages {
		t.Logf("topic: %s, partition: %d, offset: %d key: %s value: %s",
			msg.TopicName, msg.PartitionID, msg.Message.Offset, msg.Message.Key, msg.Message.Value)
		i++
		if i >= 2 {
			break
		}
	}
}
