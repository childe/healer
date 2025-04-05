package healer

import (
	"bytes"
	"context"
	"testing"
)

var resp FetchResponse = FetchResponse{
	CorrelationID:  1,
	ThrottleTimeMs: 2,
	ErrorCode:      0,
	SessionID:      3,
	Responses: map[string][]PartitionResponse{
		"test-topic": {
			{
				PartitionID:      1,
				ErrorCode:        0,
				HighWatermark:    90,
				LastStableOffset: 100,
				LogStartOffset:   110,
				AbortedTransactions: []struct {
					ProducerID  int64
					FirstOffset int64
				}{
					{ProducerID: 1, FirstOffset: 2},
				},
				RecordBatches: []RecordBatch{
					{
						BaseOffset:           120,
						PartitionLeaderEpoch: 130,
						Magic:                2,
						CRC:                  0x01020304,
						Attributes:           0,
						LastOffsetDelta:      10,
						BaseTimestamp:        1630000000000,
						MaxTimestamp:         1630000010000,
						ProducerID:           200,
						ProducerEpoch:        210,
						BaseSequence:         220,
						Records: []Record{
							{
								length:         20,
								attributes:     0,
								timestampDelta: 1000,
								offsetDelta:    0,
								key:            []byte("key-1"),
								value:          []byte("value-1"),
								Headers:        []RecordHeader{},
							},
							{
								length:         20,
								attributes:     0,
								timestampDelta: 2000,
								offsetDelta:    1,
								key:            []byte("key-2"),
								value:          []byte("value-2"),
								Headers:        []RecordHeader{},
							},
						},
					},
				},
			},
		},
	},
}

// complete 2 records, no partial data
func TestFetchResponseDecodeComplete(t *testing.T) {
	for _, version := range []uint16{7, 10} {
		payload, err := resp.Encode(version)
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
			version:     version,
		}

		startOffset := int64(0)
		err = decoder.streamDecode(context.Background(), startOffset)
		if err != nil {
			t.Error(err)
		}
		close(messages)

		i := 0
		for msg := range messages {
			t.Logf("topic: %s, partition: %d, offset: %d key: %s value: %s",
				msg.TopicName, msg.PartitionID, msg.Message.Offset, msg.Message.Key, msg.Message.Value)
			if msg.Error != nil {
				t.Error(msg.Error)
			}
			i++
		}
		if i != 2 {
			t.Errorf("expect 2 message, but got %d", i)
		}
	}
}

// append some bytes to the end of the payload
func TestFetchResponseDecodeWithPartialRecords1(t *testing.T) {
	for _, version := range []uint16{7, 10} {
		payload, err := resp.Encode(version)
		if err != nil {
			t.Error(err)
		}
		payload = append(payload, []byte{0x00, 0x00, 0x00, 0x00}...)
		t.Logf("payload length: %d", len(payload))

		reader := bytes.NewReader(payload)
		messages := make(chan *FullMessage, 10)
		decoder := fetchResponseStreamDecoder{
			ctx:         context.Background(),
			buffers:     reader,
			messages:    messages,
			totalLength: len(payload) + 4,
			version:     version,
		}

		startOffset := int64(0)
		err = decoder.streamDecode(context.Background(), startOffset)
		if err != nil {
			t.Error(err)
		}
		close(messages)

		i := 0
		for msg := range messages {
			t.Logf("topic: %s, partition: %d, offset: %d key: %s value: %s",
				msg.TopicName, msg.PartitionID, msg.Message.Offset, msg.Message.Key, msg.Message.Value)
			if msg.Error != nil {
				t.Error(msg.Error)
			}
			i++
		}
		if i != 2 {
			t.Errorf("expect 2 message, but got %d", i)
		}
	}
}

// encode 2 records and trancate the last some bytes
func TestFetchResponseDecodeWithPartialRecords2(t *testing.T) {
	for _, version := range []uint16{7, 10} {
		payload, err := resp.Encode(version)
		if err != nil {
			t.Error(err)
		}
		payload = payload[:len(payload)-8]
		t.Logf("payload length: %d", len(payload))

		reader := bytes.NewReader(payload)
		messages := make(chan *FullMessage, 10)
		decoder := fetchResponseStreamDecoder{
			ctx:         context.Background(),
			buffers:     reader,
			messages:    messages,
			totalLength: len(payload) + 4,
			version:     version,
		}

		startOffset := int64(0)
		err = decoder.streamDecode(context.Background(), startOffset)
		if err != nil {
			t.Error(err)
		}
		close(messages)

		i := 0
		for msg := range messages {
			t.Logf("msg: %+v", msg)

			t.Logf("topic: %s, partition: %d, offset: %d key: %s value: %s",
				msg.TopicName, msg.PartitionID, msg.Message.Offset, msg.Message.Key, msg.Message.Value)
			if msg.Error != nil {
				t.Error(msg.Error)
			}
			i++
		}
		if i != 1 {
			t.Errorf("expect 1 message, but got %d", i)
		}
	}
}
