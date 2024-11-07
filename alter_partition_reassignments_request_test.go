package healer

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestAlterPartitionReassignmentRequest(t *testing.T) {
	convey.Convey("Test AlterPartitionReassignmentRequest Encode and Decode", t, func() {
		var version uint16 = 0
		var clientID = "healer"

		request := NewAlterPartitionReassignmentsRequest(1000)
		request.SetCorrelationID(100)
		request.ClientID = &clientID
		request.AddAssignment("test-topic", 1, []int32{1, 2, 3})

		encoded := request.Encode(version)

		decoded, err := DecodeAlterPartitionReassignmentsRequest(encoded, version)
		convey.So(err, convey.ShouldBeNil)

		convey.So(decoded, convey.ShouldResemble, request)
	})
}
