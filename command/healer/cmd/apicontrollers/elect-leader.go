package apicontrollers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// ElectLeaders godoc
// @Summary      选举领导者
// @Description  为指定分区选举新的领导者
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        timeout    query   string  true   "超时时间（毫秒）"
// @Param        topicPartitions body array true "主题分区列表，每个元素包含 topic 和 partition 字段"
// @Success      200       {object}  map[string]interface{}
// @Router       /elect-leaders [post]
func ElectLeaders(c *gin.Context, client string) {
	type topicPartition struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	}

	topicPartitions := make([]topicPartition, 0)
	if err := c.BindJSON(&topicPartitions); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("invalid fromat: %s", err))
		return
	}

	timeout := c.Query("timeout")
	timeoutMS, err := strconv.Atoi(timeout)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
		return
	}

	bootstrapServers := c.Query("bootstrap")
	config := healer.DefaultBrokerConfig()
	config.Net.TimeoutMSForEachAPI = make([]int, 68)
	config.Net.TimeoutMSForEachAPI[healer.API_AlterPartitionReassignments] = timeoutMS
	bs, err := healer.NewBrokersWithConfig(bootstrapServers, config)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	req := healer.NewElectLeadersRequest(int32(timeoutMS))
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	for _, v := range topicPartitions {
		req.Add(v.Topic, v.Partition)
	}

	resp, err := bs.ElectLeaders(&req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}
