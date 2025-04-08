package apicontrollers

import (
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

// ListGroups godoc
// @Summary      列出消费者组
// @Description  获取所有消费者组列表
// @Tags         groups
// @Accept       json
// @Produce      json
// @Success      200    {array}   string
// @Router       /groups [get]
func ListGroups(c *gin.Context, clientID string) {
	bootstrapServers := c.Query("bootstrap")

	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	groups, err := client.ListGroups()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, groups)
}

// DescribeGroups godoc
// @Summary      获取消费者组详情
// @Description  获取指定消费者组的详细信息
// @Tags         groups
// @Accept       json
// @Produce      json
// @Param        group      path    string  true   "消费者组名称"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /group/{group} [get]
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

	groups := make([]*healer.GroupDetail, 0)
	for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
		groups = append(groups, group)
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
		for _, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
			for _, partitionOffset := range partitionOffsets {
				rst[partitionOffset.Partition] = partitionOffset.GetOffset()
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

// GetPending godoc
// @Summary      获取消费者组待消费消息
// @Description  获取指定消费者组在指定主题上的待消费消息信息
// @Tags         groups
// @Accept       json
// @Produce      json
// @Param        group  path      string  true  "消费者组名称"
// @Param        topic  path      string  true  "主题名称"
// @Success      200    {object}  map[string]interface{}
// @Router       /group/{group}/pending/{topic} [get]
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
