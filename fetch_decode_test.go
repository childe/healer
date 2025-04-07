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
								attributes:     0,
								timestampDelta: 1000,
								offsetDelta:    0,
								key:            []byte("key-1"),
								value:          []byte("value-1"),
								Headers:        []RecordHeader{},
							},
							{
								attributes:     0,
								timestampDelta: 2000,
								offsetDelta:    1,
								key:            []byte("key-22"),
								value:          []byte("value-22"),
								Headers:        []RecordHeader{},
							},
						},
					}, {
						BaseOffset:           120,
						PartitionLeaderEpoch: 130,
						Magic:                2,
						CRC:                  0x01020304,
						Attributes:           0 | int16(CompressionGzip),
						LastOffsetDelta:      10,
						BaseTimestamp:        1630000000000,
						MaxTimestamp:         1630000010000,
						ProducerID:           200,
						ProducerEpoch:        210,
						BaseSequence:         220,
						Records: []Record{
							{
								attributes:     0,
								timestampDelta: 1000,
								offsetDelta:    2,
								key:            []byte("key-3333333333"),
								value:          []byte("value-3333333333"),
								Headers:        []RecordHeader{},
							},
							{
								attributes:     0,
								timestampDelta: 2000,
								offsetDelta:    3,
								key:            []byte("key-4"),
								value:          []byte("value-4"),
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
		if i != 4 {
			t.Errorf("expect 4 message, but got %d", i)
		}
	}
}

// append some bytes to the end of the payload
func TestFetchResponseDecodeWithPartialRecords1(t *testing.T) {
	for _, version := range []uint16{7, 10} {
		t.Logf("version: %d", version)

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
			t.Logf("msg: %+v", msg)
			t.Logf("topic: %s, partition: %d, offset: %d key: %s value: %s",
				msg.TopicName, msg.PartitionID, msg.Message.Offset, msg.Message.Key, msg.Message.Value)
			if msg.Error != nil {
				t.Error(msg.Error)
			}
			i++
		}
		if i != 4 {
			t.Errorf("expect 4 message, but got %d", i)
		}
	}
}

// encode 2 records and trancate the last some bytes
func TestFetchResponseDecodeWithPartialRecords2(t *testing.T) {
	for _, version := range []uint16{7, 10} {
		t.Logf("version: %d", version)

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
		if i != 2 {
			t.Errorf("expect 2 message, but got %d", i)
		}
	}
}
