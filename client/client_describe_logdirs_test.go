package client

import (
	"encoding/json"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/childe/healer"
	"github.com/smartystreets/goconvey/convey"
)

/*
- Broker1
  - data1
    - test1-1
	- other-1
  - data2
    - test1-2
    - test2-1
- Broker2
  - data1
    - test1-1
	- other-1
  - data2
    - test1-2
    - test2-1
*/

func TestDescribeLogDirs(t *testing.T) {
	brokers := &healer.Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDescribeLogDirs", t, func() {
		topics := []string{"test1", "test2"}
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
							Replicas:    []int32{1, 2},
						},
					},
				},
			},
		}
		mockey.Mock((*healer.Brokers).RequestMetaData).Return(mockMetadataResponse, nil).Build()
		mockey.Mock((*healer.Brokers).GetBroker).Return(&healer.Broker{}, nil).Build()

		mockey.Mock((*healer.Broker).RequestAndGet).
			To(func(req healer.Request) (healer.Response, error) {
				mockResponse := healer.DescribeLogDirsResponse{
					Results: []healer.DescribeLogDirsResponseResult{
						{
							LogDir: "/data1",
							Topics: []healer.DescribeLogDirsResponseTopic{
								{
									TopicName: "test1",
									Partitions: []healer.DescribeLogDirsResponsePartition{
										{PartitionID: 1, Size: 1024},
									},
								},
								{
									TopicName: "other",
									Partitions: []healer.DescribeLogDirsResponsePartition{
										{PartitionID: 1, Size: 1024},
									},
								},
							},
						},
						{
							LogDir: "/data2",
							Topics: []healer.DescribeLogDirsResponseTopic{
								{
									TopicName: "test1",
									Partitions: []healer.DescribeLogDirsResponsePartition{
										{PartitionID: 2, Size: 1024},
									},
								},
								{
									TopicName: "test2",
									Partitions: []healer.DescribeLogDirsResponsePartition{
										{PartitionID: 1, Size: 1024},
									},
								},
							},
						},
					},
				}
				return mockResponse, nil
			}).Build()

		responses, err := c.DescribeLogDirs(topics)
		b, _ := json.Marshal(responses)
		t.Log(string(b))
		wanted := map[int32]healer.DescribeLogDirsResponse{
			1: {Results: []healer.DescribeLogDirsResponseResult{
				{
					ErrorCode: 0, LogDir: "/data1", Topics: []healer.DescribeLogDirsResponseTopic{
						{
							TopicName:  "test1",
							Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}},
						},
					},
				},
				{
					ErrorCode: 0, LogDir: "/data2", Topics: []healer.DescribeLogDirsResponseTopic{
						{TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}},
						{TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}},
					},
				},
			},
			},
			2: {Results: []healer.DescribeLogDirsResponseResult{
				{
					ErrorCode: 0, LogDir: "/data1", Topics: []healer.DescribeLogDirsResponseTopic{
						{
							TopicName:  "test1",
							Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}},
						},
					},
				},
				{
					ErrorCode: 0, LogDir: "/data2", Topics: []healer.DescribeLogDirsResponseTopic{
						{TopicName: "test1", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}},
						{TopicName: "test2", Partitions: []healer.DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}},
					},
				},
			},
			},
		}
		b, _ = json.Marshal(wanted)
		t.Log(string(b))

		convey.So(responses, convey.ShouldResemble, wanted)
		convey.So(err, convey.ShouldBeNil)
	})
}
