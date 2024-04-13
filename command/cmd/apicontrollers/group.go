package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

func parseGroupDetail(group *healer.GroupDetail) (map[string]interface{}, error) {
	rst := map[string]interface{}{}
	rst["group_id"] = group.GroupID
	rst["state"] = group.State
	rst["protocol_type"] = group.ProtocolType
	rst["protocol"] = group.Protocol

	members := []map[string]interface{}{}
	for i := range group.Members {
		e := make(map[string]interface{})
		e["member_id"] = group.Members[i].MemberID
		e["client_id"] = group.Members[i].ClientID
		e["client_host"] = group.Members[i].ClientHost

		if len(group.Members[i].MemberAssignment) != 0 {
			memberAssignment, err := healer.NewMemberAssignment(group.Members[i].MemberAssignment)
			if err != nil {
				return nil, err
			}
			for _, p := range memberAssignment.PartitionAssignments {
				e["assignments"] = p
			}
		}
		members = append(members, e)
	}
	rst["members"] = members
	return rst, nil
}

func ListGroups(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")

	helper, err := healer.NewHelper(bootstrapServers, client, healer.DefaultBrokerConfig())
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	groups := helper.GetGroups()
	c.JSON(http.StatusOK, groups)
}

func DescribeGroups(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	group := c.Param("group")

	brokers, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	coordinatorResponse, err := brokers.FindCoordinator(client, group)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	klog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

	req := healer.NewDescribeGroupsRequest(client, []string{group})
	resp, err := coordinator.RequestAndGet(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	groups := []map[string]interface{}{}
	for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
		g, err := parseGroupDetail(group)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
		}
		groups = append(groups, g)
	}
	c.JSON(http.StatusOK, groups)
}

func getPartitions(brokers *healer.Brokers, topic, client string) ([]int32, error) {
	metadataResponse, err := brokers.RequestMetaData(client, []string{topic})

	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0)
	for _, topicMetadata := range metadataResponse.TopicMetadatas {
		for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
			partitions = append(partitions, partitionMetadata.PartitionID)
		}
	}

	return partitions, nil
}

func getOffset(brokers *healer.Brokers, topic, client string) (map[int32]int64, error) {
	var (
		partitionID int32 = -1
		timestamp   int64 = -1
	)
	offsetsResponses, err := brokers.RequestOffsets(client, topic, partitionID, timestamp, 1)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, offsetsResponse := range offsetsResponses {
		for topic, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
			for _, partitionOffset := range partitionOffsets {
				if len(partitionOffset.Offsets) == 0 {
					rst[partitionOffset.Partition] = -1
					continue
				}
				if len(partitionOffset.Offsets) != 1 {
					return nil, fmt.Errorf("%s[%d] offsets length mismatch: %v", topic, partitionOffset.Partition, partitionOffset.Offsets)
				}
				rst[partitionOffset.Partition] = partitionOffset.Offsets[0]
			}
		}
	}
	return rst, nil
}

func getCommittedOffset(brokers *healer.Brokers, topic string, partitions []int32, groupID, client string) (map[int32]int64, error) {
	coordinatorResponse, err := brokers.FindCoordinator(client, groupID)
	if err != nil {
		return nil, err
	}
	coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		return nil, err
	}
	defer coordinator.Close()

	r := healer.NewOffsetFetchRequest(1, client, groupID)
	for _, p := range partitions {
		r.AddPartiton(topic, p)
	}

	resp, err := coordinator.RequestAndGet(r)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, t := range resp.(healer.OffsetFetchResponse).Topics {
		for _, p := range t.Partitions {
			rst[p.PartitionID] = p.Offset
		}
	}
	return rst, nil
}

func GetPending(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	groupID := c.Param("group")
	topicName := c.Param("topic")

	bs, err := healer.NewBrokers(bootstrapServers)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}
	defer bs.Close()

	offsets, err := getOffset(bs, topicName, client)
	if err != nil {
		klog.Errorf("get offsets error: %s", err)
	}

	partitions, err := getPartitions(bs, topicName, client)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	committedOffsets, err := getCommittedOffset(bs, topicName, partitions, groupID, client)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	type info struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
		Committed int64 `json:"committed"`
		Lag       int64 `json:"lag"`
	}
	rst := make([]info, 0)
	for _, partitionID := range partitions {
		pending := offsets[partitionID] - committedOffsets[partitionID]
		rst = append(rst, info{
			Partition: partitionID,
			Offset:    offsets[partitionID],
			Committed: committedOffsets[partitionID],
			Lag:       pending,
		})
	}
	c.JSON(http.StatusOK, rst)
	return
}
