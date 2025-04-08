package apicontrollers

import (
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// GetTopicConfigs godoc
// @Summary      获取主题配置
// @Description  获取指定主题的所有配置信息
// @Tags         configs
// @Accept       json
// @Produce      json
// @Param        topic      path    string  true   "主题名称"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /configs/topic/{topic} [get]
func GetTopicConfigs(c *gin.Context, client string) {
	GetConfigs(c, "topic", client)
}

// GetBrokerConfigs godoc
// @Summary      获取 Broker 配置
// @Description  获取指定 Broker 的配置信息
// @Tags         configs
// @Accept       json
// @Produce      json
// @Param        broker     path    string  true   "Broker ID"
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /configs/broker/{broker} [get]
func GetBrokerConfigs(c *gin.Context, client string) {
	GetConfigs(c, "broker", client)
}

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
