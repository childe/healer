package apicontrollers

import (
	"errors"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// GetMetadata godoc
// @Summary      获取元数据
// @Description  获取 Kafka 集群元数据信息
// @Tags         metadata
// @Accept       json
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Router       /metadata [get]
func GetMetadata(c *gin.Context) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()
	topics := c.QueryArray("topics")
	resp, err := bs.RequestMetaData("healer-api", topics)
	if err != nil {
		var e healer.KafkaError
		if errors.As(err, &e) {
			c.JSON(http.StatusOK, resp)
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}
