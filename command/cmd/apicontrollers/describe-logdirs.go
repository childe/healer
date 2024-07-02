package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

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
