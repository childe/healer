package apicontrollers

import (
	"fmt"
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
)

// DescribeAcls godoc
// @Summary      查看 ACL
// @Description  获取 ACL 配置信息
// @Tags         acls
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /describe-acls [post]
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

// DeleteAcls godoc
// @Summary      删除 ACL
// @Description  删除指定的 ACL 规则
// @Tags         acls
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /delete-acls [delete]
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

// CreateAcls godoc
// @Summary      创建 ACL
// @Description  创建新的 ACL 规则
// @Tags         acls
// @Accept       json
// @Produce      json
// @Param        bootstrap  query   string  true   "Kafka bootstrap servers, 格式: host1:port1,host2:port2"
// @Success      200       {object}  map[string]interface{}
// @Router       /create-acls [post]
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
