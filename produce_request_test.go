package healer

import (
	"bytes"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestProduceRequestEncodeDecodeRoundTrip(t *testing.T) {
	convey.Convey("Test ProduceRequest Encode/Decode Round Trip", t, func() {
		clientID := "test-client"
		transactionalID := "test-txn-id"

		request := &ProduceRequest{
			RequestHeader: &RequestHeader{
				APIKey:        API_ProduceRequest,
				APIVersion:    9,
				CorrelationID: 123,
				ClientID:      &clientID,
			},
			transactionalID: &transactionalID,
			RequiredAcks:    1,
			Timeout:         5000,
			TopicBlocks: []produceRequestT{
				{
					TopicName: "test-topic",
					PartitonBlocks: []produceRequestP{
						{
							Partition: 0,
							RecordBatches: []RecordBatch{
								{
									BaseOffset:           0,
									BatchLength:          0, // 将在编码过程中计算
									PartitionLeaderEpoch: 0,
									Magic:                2, // Kafka 0.11+
									CRC:                  0, // 将在编码过程中计算
									Attributes:           0,
									LastOffsetDelta:      0,
									BaseTimestamp:        1618483330000,
									MaxTimestamp:         1618483330000,
									ProducerID:           1,
									ProducerEpoch:        0,
									BaseSequence:         0,
									Records: []*Record{
										{
											attributes:     0,
											timestampDelta: 0,
											offsetDelta:    0,
											key:            []byte("key1"),
											value:          []byte("value1"),
											Headers:        []RecordHeader{},
										},
										{
											attributes:     0,
											timestampDelta: 0,
											offsetDelta:    0,
											key:            []byte("key1"),
											value:          []byte("value1"),
											Headers:        []RecordHeader{},
										},
									},
								},
								{
									BaseOffset:           0,
									BatchLength:          0, // 将在编码过程中计算
									PartitionLeaderEpoch: 0,
									Magic:                2, // Kafka 0.11+
									CRC:                  0, // 将在编码过程中计算
									Attributes:           0,
									LastOffsetDelta:      0,
									BaseTimestamp:        1618483330000,
									MaxTimestamp:         1618483330000,
									ProducerID:           1,
									ProducerEpoch:        0,
									BaseSequence:         0,
									Records: []*Record{
										{
											attributes:     0,
											timestampDelta: 0,
											offsetDelta:    0,
											key:            []byte("key1"),
											value:          []byte("value1"),
											Headers:        []RecordHeader{},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		version := uint16(9)
		encodedData := request.Encode(version)

		decodedRequest := &ProduceRequest{}
		err := decodedRequest.Decode(encodedData, version)

		// set some fields to zero to test decoding correctly
		for i := range decodedRequest.TopicBlocks {
			t := &decodedRequest.TopicBlocks[i]
			for j := range t.PartitonBlocks {
				p := &t.PartitonBlocks[j]
				p.RecordBatchesSize = 0
				for k := range p.RecordBatches {
					b := &p.RecordBatches[k]
					b.BatchLength = 0
					b.CRC = 0
				}
			}
		}

		convey.So(err, convey.ShouldBeNil)
		convey.So(decodedRequest, convey.ShouldResemble, request)
	})
}

// 测试边界情况：空topic
func TestProduceRequestWithEmptyTopics(t *testing.T) {
	clientID := "test-client"

	request := &ProduceRequest{
		RequestHeader: &RequestHeader{
			APIKey:        API_ProduceRequest,
			APIVersion:    0,
			CorrelationID: 456,
			ClientID:      &clientID,
		},
		RequiredAcks: 0,
		Timeout:      1000,
		TopicBlocks:  []produceRequestT{},
	}

	// 编码请求
	version := uint16(9)
	encodedData := request.Encode(version)
	if encodedData == nil {
		t.Fatal("Failed to encode ProduceRequest with empty topics")
	}

	// 解码请求
	decodedRequest := &ProduceRequest{}
	err := decodedRequest.Decode(encodedData, version)
	if err != nil {
		t.Fatalf("Failed to decode ProduceRequest with empty topics: %v", err)
	}

	// 验证
	if len(decodedRequest.TopicBlocks) != 0 {
		t.Errorf("Expected empty TopicBlocks, got %d topics", len(decodedRequest.TopicBlocks))
	}
}

// 测试大型消息
func TestProduceRequestWithLargeMessage(t *testing.T) {
	clientID := "test-client"

	// 创建一个大消息值
	largeValue := bytes.Repeat([]byte("a"), 1024*1024) // 1MB

	request := &ProduceRequest{
		RequestHeader: &RequestHeader{
			APIKey:        API_ProduceRequest,
			APIVersion:    0,
			CorrelationID: 789,
			ClientID:      &clientID,
		},
		RequiredAcks: 1,
		Timeout:      10000,
		TopicBlocks: []produceRequestT{
			{
				TopicName: "large-message-topic",
				PartitonBlocks: []produceRequestP{
					{
						Partition: 0,
						RecordBatches: []RecordBatch{
							{
								BaseOffset:           0,
								Magic:                2,
								PartitionLeaderEpoch: 0,
								Attributes:           0,
								LastOffsetDelta:      0,
								BaseTimestamp:        1618483330000,
								MaxTimestamp:         1618483330000,
								ProducerID:           1,
								ProducerEpoch:        0,
								BaseSequence:         0,
								Records: []*Record{
									{
										attributes:     0,
										timestampDelta: 0,
										offsetDelta:    0,
										key:            []byte("large-key"),
										value:          largeValue,
										Headers:        []RecordHeader{},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 编码请求
	version := uint16(0)
	encodedData := request.Encode(version)
	if encodedData == nil {
		t.Fatal("Failed to encode ProduceRequest with large message")
	}

	// 解码请求
	decodedRequest := &ProduceRequest{}
	err := decodedRequest.Decode(encodedData, version)
	if err != nil {
		t.Fatalf("Failed to decode ProduceRequest with large message: %v", err)
	}

	// 验证解码后的数据是否完整
	if len(decodedRequest.TopicBlocks) != 1 {
		t.Fatalf("Expected 1 topic, got %d", len(decodedRequest.TopicBlocks))
	}

	if decodedRequest.TopicBlocks[0].TopicName != "large-message-topic" {
		t.Errorf("Topic name mismatch: got %s, want large-message-topic",
			decodedRequest.TopicBlocks[0].TopicName)
	}
}
