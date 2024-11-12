package apicontrollers

import (
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func GetConfigs(c *gin.Context, resourceType, clientID string) {
	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	resourceName := c.Param(resourceType)

	resp, err := client.DescribeConfigs(resourceType, resourceName, nil)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

func GetTopicConfigs(c *gin.Context, clientID string) {
	GetConfigs(c, "topic", clientID)
}

func GetBrokerConfigs(c *gin.Context, clientID string) {
	GetConfigs(c, "broker", clientID)
}
