package apicontrollers

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func GetTopicConfig(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	topic := c.Param("topic")
	config := c.Param("config")

	resources := []*healer.DescribeConfigsRequestResource{
		{
			ResourceType: healer.ConvertConfigResourceType("topic"),
			ResourceName: topic,
			ConfigNames:  []string{config},
		},
	}
	r := healer.NewDescribeConfigsRequest(client, resources)

	controller, err := bs.GetBroker(bs.Controller())
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := controller.RequestAndGet(r)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

func AlterTopicConfig(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	topic := c.Param("topic")
	config := c.Param("config")
	value := c.Param("value")

	r := healer.NewIncrementalAlterConfigsRequest(client)
	r.AddConfig(healer.ConvertConfigResourceType("topic"), topic, config, value)

	controller, err := bs.GetBroker(bs.Controller())
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := controller.RequestAndGet(r)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

func GetTopicOffsets(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	timestamp := c.Query("timestamp")
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("timestamp value error: %s", err))
		return
	}

	topic := c.Param("topic")

	rst := make([]healer.PartitionOffset, 0)

	offsetsResponse, err := bs.RequestOffsets(client, topic, -1, ts, 1)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	for _, x := range offsetsResponse {
		for _, partitionOffsetsList := range x.TopicPartitionOffsets {
			rst = append(rst, partitionOffsetsList...)
		}
	}

	sort.Slice(rst, func(i, j int) bool { return rst[i].Partition < rst[j].Partition })
	c.JSON(http.StatusOK, rst)
}

func GetTopicLogDirs(c *gin.Context, clientID string) {
	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	topic := c.Param("topic")

	rst, err := client.DescribeLogDirs([]string{topic})

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}
