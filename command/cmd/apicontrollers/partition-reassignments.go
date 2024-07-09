package apicontrollers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func ListPartitionReassignments(c *gin.Context, client string) {
	type reassignment struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	}

	timeout := c.Query("timeout")
	timeoutMS, err := strconv.Atoi(timeout)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("timeout value error: %s", err))
		return
	}

	config := healer.DefaultBrokerConfig()
	config.Net.TimeoutMSForEachAPI = make([]int, 68)
	config.Net.TimeoutMSForEachAPI[healer.API_ListPartitionReassignments] = timeoutMS
	bootstrapServers := c.Query("bootstrap")
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
	req := healer.NewListPartitionReassignmentsRequest(client, int32(timeoutMS))
	for _, v := range reassignments {
		req.AddTP(v.Topic, v.Partition)
	}
	resp, err := bs.ListPartitionReassignments(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}

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
