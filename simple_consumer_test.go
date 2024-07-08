package healer

import (
	"fmt"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestRefreshPartiton(t *testing.T) {
	mockey.PatchConvey("TestEmptyMeta", t, func() {
		topic := "testTopic"
		partitionID := 1

		simpleConsumer := &SimpleConsumer{
			topic:       topic,
			partitionID: int32(partitionID),
		}

		mockey.Mock((*Brokers).RequestMetaData).Return(MetadataResponse{}, nil).Build()

		err := simpleConsumer.refreshPartiton()

		convey.So(err.Error(), convey.ShouldEqual, "partition not found in meetadata response")
	})

	mockey.PatchConvey("TestNotFoundMeta", t, func() {
		topic := "testTopic"
		partitionID := 1

		simpleConsumer := &SimpleConsumer{
			topic:       topic,
			partitionID: int32(partitionID),
		}

		mockey.Mock((*Brokers).RequestMetaData).Return(MetadataResponse{
			TopicMetadatas: []TopicMetadata{
				{
					TopicName: "fakeTopic",
					PartitionMetadatas: []*PartitionMetadataInfo{
						{
							PartitionID: 1,
							Leader:      1,
							Replicas:    []int32{1},
						},
					},
				},
			},
		}, nil).Build()

		err := simpleConsumer.refreshPartiton()

		convey.So(err.Error(), convey.ShouldEqual, "partition not found in meetadata response")
	})

	mockey.PatchConvey("TestNormal", t, func() {
		topic := "testTopic"
		partitionID := 1

		simpleConsumer := &SimpleConsumer{
			topic:       topic,
			partitionID: int32(partitionID),
		}

		mockey.Mock((*Brokers).RequestMetaData).Return(MetadataResponse{
			TopicMetadatas: []TopicMetadata{
				{
					TopicName: "testTopic",
					PartitionMetadatas: []*PartitionMetadataInfo{
						{
							PartitionID: 1,
							Leader:      1,
							Replicas:    []int32{1, 2},
						},
					},
				},
			},
		}, nil).Build()

		err := simpleConsumer.refreshPartiton()

		convey.So(err, convey.ShouldBeNil)
		convey.So(simpleConsumer.partition.Replicas, convey.ShouldResemble, []int32{1, 2})
	})
}

func TestNewSimpleConsumer(t *testing.T) {
	mockey.PatchConvey("TestNewSimpleConsumer", t, func() {
		topic := "testTopic"
		partitionID := 1
		config := map[string]interface{}{
			"bootstrap.servers": "localhost:9092",
			"client.id":         "healer-test",
		}

		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{
			brokersInfo: map[int32]*BrokerInfo{
				1: {
					NodeID: 0,
					Host:   "localhost",
					Port:   9092,
				},
			},
		}, nil).Build()
		mockey.Mock((*SimpleConsumer).refreshPartiton).Return(nil).Build()

		simpleConsumer, err := NewSimpleConsumer(topic, int32(partitionID), config)

		convey.So(err, convey.ShouldBeNil)
		convey.So(simpleConsumer, convey.ShouldNotBeNil)
	})
}
func TestConsume(t *testing.T) {
	mockey.PatchConvey("TestConsume", t, func() {
		topic := "testTopic"
		partitionID := 1
		config := map[string]interface{}{
			"bootstrap.servers": "localhost:9092",
			"client.id":         "healer-test",
		}

		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{
			brokersInfo: map[int32]*BrokerInfo{
				1: {
					NodeID: 0,
					Host:   "localhost",
					Port:   9092,
				},
			},
		}, nil).Build()
		mockey.Mock((*SimpleConsumer).refreshPartiton).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).getLeaderBroker).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).initOffset).Return().Build()
		mockey.Mock((*Broker).getHighestAvailableAPIVersion).Return(10).Build()
		requestFetchStreamingly := mockey.Mock((*Broker).requestFetchStreamingly).
			To(func(fetchRequest *FetchRequest) (r io.Reader, responseLength uint32, err error) {
				println("mock requestFetchStreamingly")
				return nil, 0, nil
			}).Build()

		streamDecode := mockey.Mock((*fetchResponseStreamDecoder).streamDecode).To(func(decoder *fetchResponseStreamDecoder, startOffset int64) error {
			for i := 0; i < 5; i++ {
				select {
				case <-decoder.ctx.Done():
					return nil
				case decoder.messages <- &FullMessage{
					TopicName:   topic,
					PartitionID: int32(partitionID),
					Error:       nil,
					Message: &Message{
						Offset:      1,
						MessageSize: 10,
						Crc:         1,
						MagicByte:   1,
						Attributes:  1,
						Timestamp:   1,
						Key:         []byte("test"),
						Value:       []byte(fmt.Sprintf("test-%d", i)),
					},
				}:
				}
			}
			return nil
		}).Build()

		type testCase struct {
			messageChanLength            int
			maxMessage                   int
			requestFetchStreaminglyCount int
			streamDecodeCount            int
		}
		for _, tc := range []testCase{
			{
				messageChanLength:            0,
				maxMessage:                   1,
				requestFetchStreaminglyCount: 1,
				streamDecodeCount:            1,
			},
			{
				messageChanLength:            0,
				maxMessage:                   2,
				requestFetchStreaminglyCount: 2, // incremental, add 1 for the first time
				streamDecodeCount:            2, // incremental, add 1 for the first time
			},
			{
				messageChanLength:            0,
				maxMessage:                   5,
				requestFetchStreaminglyCount: 4,
				streamDecodeCount:            4,
			},
		} {
			t.Logf("test case: %+v", tc)
			simpleConsumer, err := NewSimpleConsumer(topic, int32(partitionID), config)
			convey.So(err, convey.ShouldBeNil)
			convey.So(simpleConsumer, convey.ShouldNotBeNil)

			messages := make(chan *FullMessage, tc.messageChanLength)
			msg, err := simpleConsumer.Consume(-2, messages)
			convey.So(err, convey.ShouldBeNil)
			convey.So(msg, convey.ShouldNotBeNil)

			count := 0
			for count < tc.maxMessage {
				m := <-msg
				println("msg:", string(m.Message.Value))
				count++
			}
			simpleConsumer.Stop()
			print("stopped")

			convey.So(count, convey.ShouldEqual, tc.maxMessage)
			convey.So(requestFetchStreamingly.Times(), convey.ShouldEqual, tc.requestFetchStreaminglyCount)
			convey.So(streamDecode.Times(), convey.ShouldEqual, tc.streamDecodeCount)
		}
	})
}
