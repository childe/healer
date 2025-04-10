package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// DescribeLogdirs godoc
// @Summary      描述日志目录
// @Description  获取指定主题的日志目录信息
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        topics     body    array   true   "主题名称列表"
// @Success      200       {object}  map[string]interface{}
// @Router       /describe-logdirs [post]
func DescribeLogdirs(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")

	admin, err := healer.NewClient(bootstrapServers, client)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	clientReq := struct {
		Topics []string `json:"topics"`
	}{}
	if err := c.BindJSON(&clientReq); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("topics value error: %s", err))
		return
	}

	responses, err := admin.DescribeLogDirs(clientReq.Topics)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	c.JSON(http.StatusOK, responses)
}
