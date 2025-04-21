package healer

import (
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
