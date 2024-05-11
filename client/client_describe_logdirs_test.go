package client

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/childe/healer"
	"github.com/smartystreets/goconvey/convey"
)

func genMockResult(topic healer.DescribeLogDirsRequestTopic) (r []healer.DescribeLogDirsResponseTopic) {
	for _, p := range topic.Partitions {
		r = append(r, healer.DescribeLogDirsResponseTopic{
			TopicName: topic.TopicName,
			Partitions: []healer.DescribeLogDirsResponsePartition{
				{
					PartitionID: p,
					Size:        1024,
					OffsetLag:   0,
					IsFutureKey: false,
				},
			},
		})
	}
	return
}

func TestDescribeLogDirs(t *testing.T) {
	brokers := &healer.Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDescribeLogDirs", t, func() {
		topics := []string{"test"}
		mockMetadataResponse := healer.MetadataResponse{
			TopicMetadatas: []healer.TopicMetadata{
				{
					TopicErrorCode: 0,
					TopicName:      "test1",
					IsInternal:     false,
					PartitionMetadatas: [](*healer.PartitionMetadataInfo){
						{
							PartitionID: 1,
							Replicas:    []int32{1, 2},
						},
						{
							PartitionID: 2,
							Replicas:    []int32{2, 1},
						},
					},
				},
				{
					TopicErrorCode: 0,
					TopicName:      "test2",
					IsInternal:     false,
					PartitionMetadatas: [](*healer.PartitionMetadataInfo){
						{
							PartitionID: 1,
							Replicas:    []int32{2, 3, 4},
						},
					},
				},
			},
		}
		mockey.Mock((*healer.Brokers).RequestMetaData).Return(mockMetadataResponse, nil).Build()
		mockey.Mock((*healer.Brokers).GetBroker).Return(&healer.Broker{}, nil).Build()

		mockey.Mock((*healer.Broker).RequestAndGet).
			To(func(req healer.Request) (healer.Response, error) {
				r := req.(healer.DescribeLogDirsRequest)
				mockResponse := healer.DescribeLogDirsResponse{
					Results: []healer.DescribeLogDirsResponseResult{
						{
							LogDir: "/data/kafka1",
						},
						{
							LogDir: "/data/kafka2",
						},
					},
				}
				for i := range mockResponse.Results {
					mockResponse.Results[i].Topics = append(mockResponse.Results[i].Topics, healer.DescribeLogDirsResponseTopic{
						TopicName:  "others",
						Partitions: []healer.DescribeLogDirsResponsePartition{},
					})
					for _, topic := range r.Topics {
						result := genMockResult(topic)
						mockResponse.Results[i].Topics = append(mockResponse.Results[i].Topics, result...)
					}
				}
				return mockResponse, nil
			}).Build()

		responses, err := c.DescribeLogDirs(topics)
		wanted := map[int32]healer.DescribeLogDirsResponse{
			1: {Results: []healer.DescribeLogDirsResponseResult{{ErrorCode: 0, LogDir: "/data/kafka1", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}}}}, {ErrorCode: 0, LogDir: "/data/kafka2", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}}}}}},
			2: {Results: []healer.DescribeLogDirsResponseResult{{ErrorCode: 0, LogDir: "/data/kafka1", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}, {LogDir: "/data/kafka2", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}, {TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}}},
			3: {Results: []healer.DescribeLogDirsResponseResult{{ErrorCode: 0, LogDir: "/data/kafka1", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}, {ErrorCode: 0, LogDir: "/data/kafka2", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}}},
			4: {Results: []healer.DescribeLogDirsResponseResult{{ErrorCode: 0, LogDir: "/data/kafka1", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}, {ErrorCode: 0, LogDir: "/data/kafka2", Topics: []healer.DescribeLogDirsResponseTopic{{TopicName: "others", Partitions: []healer.DescribeLogDirsResponsePartition{}}, {TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}}}}}}}
		convey.So(responses, convey.ShouldResemble, wanted)
		convey.So(err, convey.ShouldBeNil)
	})
}
