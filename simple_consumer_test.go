package healer

import (
	"context"
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
		initOffset := mockey.Mock((*SimpleConsumer).initOffset).Return().Build()
		getHighestAvailableAPIVersion := mockey.Mock((*Broker).getHighestAvailableAPIVersion).Return(10).Build()
		requestFetchStreamingly := mockey.Mock((*Broker).requestFetchStreamingly).
			To(func(ctx context.Context, fetchRequest *FetchRequest, buffers chan []byte) error {
				return nil
				// for i := 0; i < 10000; i++ {
				// 	buffers <- []byte("test")
				// }
				// return nil
			}).Build()

		streamDecode := mockey.Mock((*fetchResponseStreamDecoder).streamDecode).To(func(decoder *fetchResponseStreamDecoder, version uint16, startOffset int64) error {
			for i := 0; i < 10; i++ {
				decoder.messages <- &FullMessage{
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
						Value:       []byte("test"),
					},
				}
			}
			return nil
		}).Build()

		simpleConsumer, err := NewSimpleConsumer(topic, int32(partitionID), config)

		convey.So(err, convey.ShouldBeNil)
		convey.So(simpleConsumer, convey.ShouldNotBeNil)

		msg, err := simpleConsumer.Consume(-2, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(msg, convey.ShouldNotBeNil)

		count := 0
		wg := make(chan struct{})
		go func() {
			defer close(wg)
			for {
				select {
				case m := <-msg:
					print(m)
					count++
					if m == nil {
						return
					}
					// case <-time.After(time.Duration(1) * time.Second):
					// 	panic("consume 10 messages timeout")
				}
			}
		}()

		simpleConsumer.Stop()
		<-wg

		convey.So(count, convey.ShouldEqual, 10)
		convey.So(initOffset.Times(), convey.ShouldEqual, 1)
		convey.So(getHighestAvailableAPIVersion.Times(), convey.ShouldEqual, 2)
		convey.So(streamDecode.Times(), convey.ShouldEqual, 1)
		convey.So(requestFetchStreamingly.Times(), convey.ShouldEqual, 1)
	})
}
