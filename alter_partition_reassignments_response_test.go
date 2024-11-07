package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestAlterPartitionReassignmentResponse(t *testing.T) {
	convey.Convey("Test AlterPartitionReassignmentResponse Encode and Decode", t, func() {
		var errorMsg string = "mock error"
		var version uint16 = 0
		header := NewResponseHeader(API_AlterPartitionReassignments, 0)
		header.CorrelationID = 1
		header.TaggedFields = TaggedFields{
			{
				Tag:  1,
				Data: []byte("hello"),
			},
			{
				Tag:  2,
				Data: []byte("healer"),
			},
		}
		response := AlterPartitionReassignmentsResponse{
			ResponseHeader: header,
			ErrorCode:      1,
			ErrorMsg:       &errorMsg,
			Responses: []*alterPartitionReassignmentsResponseTopic{
				{
					Name: "test-topic",
					Partitions: []*alterPartitionReassignmentsResponseTopicPartition{
						{
							PartitionID: 1,
							ErrorCode:   1,
							ErrorMsg:    &errorMsg,
						},
						{
							PartitionID: 2,
							ErrorCode:   2,
							ErrorMsg:    &errorMsg,
						},
					},
				},
			},
		}

		encoded := response.Encode(version)

		decoded, err := NewAlterPartitionReassignmentsResponse(encoded, version)
		convey.So(err, convey.ShouldBeNil)

		convey.So(*decoded, convey.ShouldResemble, response)
	})
}
