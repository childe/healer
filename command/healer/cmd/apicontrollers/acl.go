package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

func DescribeAcls(c *gin.Context, clientID string) {
	var req healer.DescribeAclsRequestBody
	if err := c.BindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("describe acl reqeust body format error: %s", err))
		return
	}

	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	rst, err := client.DescribeAcls(req)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}

func DeleteAcls(c *gin.Context, clientID string) {
	var filters []*healer.DeleteAclsFilter
	if err := c.BindJSON(&filters); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("acl filters value error: %s", err))
		return
	}

	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	rst, err := client.DeleteAcls(filters)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}

func CreateAcls(c *gin.Context, clientID string) {
	var aclCreation []healer.AclCreation
	if err := c.BindJSON(&aclCreation); err != nil {
		c.String(http.StatusBadRequest, fmt.Sprintf("acl filters value error: %s", err))
		return
	}

	bootstrapServers := c.Query("bootstrap")
	client, err := healer.NewClient(bootstrapServers, clientID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	defer client.Close()

	rst, err := client.CreateAcls(aclCreation)

	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, rst)
}
