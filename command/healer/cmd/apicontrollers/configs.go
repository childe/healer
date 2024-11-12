package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func GetBrokerConfigs(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	broker := c.Param("broker")

	resources := []*healer.DescribeConfigsRequestResource{
		{
			ResourceType: healer.ConvertConfigResourceType("broker"),
			ResourceName: broker,
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
