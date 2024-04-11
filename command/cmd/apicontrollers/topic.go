package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func GetTopicConfigs(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	topic := c.Param("topic")

	resources := []*healer.DescribeConfigsRequestResource{
		{
			ResourceType: healer.ConvertConfigResourceType("topic"),
			ResourceName: topic,
			ConfigNames:  nil,
		},
	}
	r := healer.NewDescribeConfigsRequest(client, resources)

	controller, err := bs.GetBroker(bs.Controller())
	if err != nil {
		err = fmt.Errorf("failed to create crotroller broker: %w", err)
		c.String(http.StatusInternalServerError, err.Error())
	}
	resp, err := controller.RequestAndGet(r)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

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
