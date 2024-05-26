package healer

import (
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

func TestSimpleConsumer(t *testing.T) {
	mockey.PatchConvey("TestSimpleConsumer", t, func() {
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
