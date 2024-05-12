package healer

import (
	"encoding/json"
	"testing"

	"github.com/bytedance/mockey"
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
    - test1-2
    - test2-1
  - data2
	- other-1
*/

func TestDescribeLogDirs(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDescribeLogDirs", t, func() {
		topics := []string{"test1", "test2"}
		mockMetadataResponse := MetadataResponse{
			TopicMetadatas: []TopicMetadata{
				{
					TopicErrorCode: 0,
					TopicName:      "test1",
					IsInternal:     false,
					PartitionMetadatas: [](*PartitionMetadataInfo){
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
					PartitionMetadatas: [](*PartitionMetadataInfo){
						{
							PartitionID: 1,
							Replicas:    []int32{1, 2},
						},
					},
				},
			},
		}
		mockey.Mock((*Brokers).RequestMetaData).Return(mockMetadataResponse, nil).Build()
		mockey.Mock((*Brokers).GetBroker).
			To(func(nodeID int32) (*Broker, error) {
				return &Broker{nodeID: nodeID}, nil
			}).
			Build()

		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				var mockResponse DescribeLogDirsResponse
				if b.nodeID == 1 {
					mockResponse = DescribeLogDirsResponse{
						Results: []DescribeLogDirsResponseResult{
							{
								LogDir: "/data1",
								Topics: []DescribeLogDirsResponseTopic{
									{
										TopicName: "test1",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
									{
										TopicName: "other",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
								},
							},
							{
								LogDir: "/data2",
								Topics: []DescribeLogDirsResponseTopic{
									{
										TopicName: "test1",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 2, Size: 1024},
										},
									},
									{
										TopicName: "test2",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
								},
							},
						},
					}
				}
				if b.nodeID == 2 {
					mockResponse = DescribeLogDirsResponse{
						Results: []DescribeLogDirsResponseResult{
							{
								LogDir: "/data1",
								Topics: []DescribeLogDirsResponseTopic{
									{
										TopicName: "test1",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
									{
										TopicName: "test1",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 2, Size: 1024},
										},
									},
									{
										TopicName: "test2",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
								},
							},
							{
								LogDir: "/data2",
								Topics: []DescribeLogDirsResponseTopic{
									{
										TopicName: "other",
										Partitions: []DescribeLogDirsResponsePartition{
											{PartitionID: 1, Size: 1024},
										},
									},
								},
							},
						},
					}
				}
				return mockResponse, nil
			}).Build()

		responses, err := c.DescribeLogDirs(topics)
		b, _ := json.Marshal(responses)
		t.Log(string(b))
		wanted := map[int32]DescribeLogDirsResponse{
			1: {Results: []DescribeLogDirsResponseResult{
				{
					ErrorCode: 0, LogDir: "/data1", Topics: []DescribeLogDirsResponseTopic{
						{
							TopicName:  "test1",
							Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}},
						},
					},
				},
				{
					ErrorCode: 0, LogDir: "/data2", Topics: []DescribeLogDirsResponseTopic{
						{TopicName: "test1", Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}}},
						{TopicName: "test2", Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}}},
					},
				},
			},
			},
			2: {Results: []DescribeLogDirsResponseResult{
				{
					ErrorCode: 0, LogDir: "/data1", Topics: []DescribeLogDirsResponseTopic{
						{
							TopicName:  "test1",
							Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}},
						},
						{
							TopicName:  "test1",
							Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 2, Size: 1024}},
						},
						{
							TopicName:  "test2",
							Partitions: []DescribeLogDirsResponsePartition{{PartitionID: 1, Size: 1024}},
						},
					},
				},
				{
					ErrorCode: 0, LogDir: "/data2", Topics: []DescribeLogDirsResponseTopic{},
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
