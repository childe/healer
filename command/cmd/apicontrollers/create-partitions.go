package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

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
