package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// CreatePartitions godoc
// @Summary      创建分区
// @Description  为指定主题创建新的分区
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Param        topic      body    string  true   "主题名称"
// @Param        count      body    int32   true   "新增分区数量"
// @Param        timeout    body    int     true   "超时时间（毫秒）"
// @Success      200       {object}  map[string]interface{}
// @Router       /create-partitions [post]
func CreatePartitions(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	bs, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer bs.Close()

	clientReq := struct {
		Topic   string `json:"topic"`
		Count   int32  `json:"count"`
		Timeout int    `json:"timeout"`
	}{}
	if err := c.BindJSON(&clientReq); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
		return
	}

	req := healer.NewCreatePartitionsRequest(client, uint32(clientReq.Timeout), false)
	req.AddTopic(clientReq.Topic, clientReq.Count, nil)

	controller, err := bs.GetBroker(bs.Controller())
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	resp, err := controller.RequestAndGet(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, resp)
}
