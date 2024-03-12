package healer

import "testing"

func TestAssgin(t *testing.T) {
	var (
		s          = &rangeAssignmentStrategy{}
		members    []string
		partitions []int32
		rst        map[string][]int32
	)
	members = []string{"1", "2"}
	partitions = []int32{0, 1, 2, 3, 4}
	rst = s.assignPartitions(members, partitions)

	if len(rst) != 2 {
		t.Error("rst length != 2")
	}
	if len(rst["1"]) != 3 {
		t.Error("partitions in memeber 1 != 3")
	}
	if len(rst["2"]) != 2 {
		t.Error("partitions in memeber 2 != 2")
	}

	members = []string{"1", "2"}
	partitions = []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	rst = s.assignPartitions(members, partitions)

	if len(rst) != 2 {
		t.Error("rst length != 2")
	}
	if len(rst["1"]) != 5 {
		t.Error("partitions in memeber 1 != 5")
	}
	if len(rst["2"]) != 5 {
		t.Error("partitions in memeber 2 != 5")
	}
}
