package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func DescribeAcls(c *gin.Context, clientID string) {
	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	req := healer.DescribeAclsRequestBody{}
	if err := c.BindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("reassignments value error: %s", err))
		return
	}

	rst, err := client.DescribeAcls(req)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}
