package apicontrollers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

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
