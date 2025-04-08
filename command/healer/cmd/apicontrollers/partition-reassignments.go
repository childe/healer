package apicontrollers

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// ListPartitionReassignments godoc
// @Summary      列出分区重分配
// @Description  获取当前正在进行的分区重分配信息
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        timeout    query   string  false  "超时时间（毫秒），默认 30000"
// @Success      200       {object}  map[string]interface{}
// @Router       /list-partition-reassignments [post]
func ListPartitionReassignments(c *gin.Context, client string) {
	type reassignment struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	}

	var err error

	config := healer.DefaultBrokerConfig()

	timeoutMS := 30000
	timeout := c.Query("timeout")
	if timeout != "" {
		timeoutMS, err = strconv.Atoi(timeout)
		if err != nil {
			c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
			return
		}
	}

	config.Net.TimeoutMSForEachAPI = make([]int, 68)
	config.Net.TimeoutMSForEachAPI[healer.API_ListPartitionReassignments] = timeoutMS

	bootstrapServers := c.Query("bootstrap")
	admin, err := healer.NewClient(bootstrapServers, client)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}
	defer admin.Close()

	reassignments := make([]reassignment, 0)
	if err := c.BindJSON(&reassignments); err != nil {
		if err != io.EOF {
			c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
			return
		}
	}
	req := healer.NewListPartitionReassignmentsRequest(client, int32(timeoutMS))
	for _, v := range reassignments {
		req.AddTP(v.Topic, v.Partition)
	}

	resp, err := admin.ListPartitionReassignments(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, resp)
}

// AlterPartitionReassignments godoc
// @Summary      修改分区重分配
// @Description  修改 Kafka 主题分区的重分配计划
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        timeout    query   string  true   "超时时间（毫秒）"
// @Success      200       {object}  map[string]interface{}
// @Router       /alter-partition-reassignments [post]
func AlterPartitionReassignments(c *gin.Context, client string) {
	type reassignment struct {
		Topic     string  `json:"topic"`
		Partition int32   `json:"partition"`
		Replicas  []int32 `json:"replicas"`
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

	reassignments := make([]reassignment, 0)
	if err := c.BindJSON(&reassignments); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
		return
	}
	req := healer.NewAlterPartitionReassignmentsRequest(int32(timeoutMS))
	for _, v := range reassignments {
		req.AddAssignment(v.Topic, v.Partition, v.Replicas)
	}
	resp, err := bs.AlterPartitionReassignments(&req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}
