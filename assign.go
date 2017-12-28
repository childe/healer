package healer

import (
	"sort"

	"github.com/golang/glog"
)

type AssignmentStrategy interface {
	// generally topicMetadatas is returned by metaDataRequest sent by GroupConsumer
	Assign([]*Member, []*TopicMetadata) GroupAssignment
}

type RangeAssignmentStrategy struct {
}

/*
type PartitionMetadataInfo struct {
	PartitionErrorCode int16
	PartitionId        uint32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

type TopicMetadata struct {
	TopicErrorCode     int16
	TopicName          string
	PartitionMetadatas []*PartitionMetadataInfo
}

type PartitionAssignment struct {
	Topic     string
	Partition int32
}
type MemberAssignment struct {
	Version              int16
	PartitionAssignments []*PartitionAssignment
	UserData             []byte
}

type GroupAssignment []struct {
	MemberID         string
	MemberAssignment []byte
}
*/

// partitions in one topic
// (3,5)=>[(0,2),(2,2),(4,1)]  (5,10)=>[(0,2), (2,2), (4,2), (6,2), (8,2)]
func (r *RangeAssignmentStrategy) assignPartitions(memberCount, partitionCount int) [][]int {
	rst := make([][]int, memberCount)
	remanent := partitionCount
	for i := memberCount - 1; i >= 0; i-- {
		rst[i] = make([]int, 2)
		rst[i][1] = remanent / (i + 1)
		remanent -= rst[i][1]
		rst[i][0] = remanent
	}

	return rst
}

type ByPartitionID []*PartitionMetadataInfo

func (a ByPartitionID) Len() int           { return len(a) }
func (a ByPartitionID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPartitionID) Less(i, j int) bool { return a[i].PartitionId < a[j].PartitionId }

func (r *RangeAssignmentStrategy) Assign(
	members []*Member, topicMetadatas []*TopicMetadata) GroupAssignment {

	partitionsAssignments := make(map[string][][]int)
	for _, topicMetadata := range topicMetadatas {
		sort.Sort(ByPartitionID(topicMetadata.PartitionMetadatas))
		partitionsAssignments[topicMetadata.TopicName] = r.assignPartitions(len(members), len(topicMetadata.PartitionMetadatas))
	}

	glog.V(10).Infof("topic partitions assignments:%v", partitionsAssignments)

	groupAssignment := make([]struct {
		MemberID         string
		MemberAssignment []byte
	}, len(members))

	// memberAssignments is temporary, will be encoded into []byte after all partitions assigned
	memberAssignments := make([]*MemberAssignment, len(members))

	for i, member := range members {
		groupAssignment[i].MemberID = member.MemberID

		memberAssignments[i] = &MemberAssignment{
			Version:              0,
			PartitionAssignments: make([]*PartitionAssignment, 0),
			UserData:             nil,
		}
		for _, topicMetadata := range topicMetadatas {
			partitionAssignment := &PartitionAssignment{
				Topic: topicMetadata.TopicName,
			}
			partitions := partitionsAssignments[topicMetadata.TopicName][i]
			partitionAssignment.Partitions = make([]int32, partitions[1])

			for j := 0; j < partitions[1]; j++ {
				idx := j + partitions[0]
				partitionAssignment.Partitions[j] = int32(topicMetadata.PartitionMetadatas[idx].PartitionId)
			}
			memberAssignments[i].PartitionAssignments = append(memberAssignments[i].PartitionAssignments, partitionAssignment)
		}
	}

	for i := range groupAssignment {
		groupAssignment[i].MemberAssignment = memberAssignments[i].Encode()
	}
	return groupAssignment
}
