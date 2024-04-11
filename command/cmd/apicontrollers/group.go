package apicontrollers

import (
	"net/http"

	"github.com/childe/healer"
	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

func parseGroupDetail(group *healer.GroupDetail) (map[string]interface{}, error) {
	rst := map[string]interface{}{}
	rst["group_id"] = group.GroupID
	rst["state"] = group.State
	rst["protocol_type"] = group.ProtocolType
	rst["protocol"] = group.Protocol

	members := []map[string]interface{}{}
	for i := range group.Members {
		e := make(map[string]interface{})
		e["member_id"] = group.Members[i].MemberID
		e["client_id"] = group.Members[i].ClientID
		e["client_host"] = group.Members[i].ClientHost

		if len(group.Members[i].MemberAssignment) != 0 {
			memberAssignment, err := healer.NewMemberAssignment(group.Members[i].MemberAssignment)
			if err != nil {
				return nil, err
			}
			for _, p := range memberAssignment.PartitionAssignments {
				e["assignments"] = p
			}
		}
		members = append(members, e)
	}
	rst["members"] = members
	return rst, nil
}

func ListGroups(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")

	helper, err := healer.NewHelper(bootstrapServers, client, healer.DefaultBrokerConfig())
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	groups := helper.GetGroups()
	c.JSON(http.StatusOK, groups)
}

func DescribeGroups(c *gin.Context, client string) {
	bootstrapServers := c.Query("bootstrap")
	group := c.Param("group")

	brokers, err := healer.NewBrokers(bootstrapServers)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	coordinatorResponse, err := brokers.FindCoordinator(client, group)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	klog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

	req := healer.NewDescribeGroupsRequest(client, []string{group})
	resp, err := coordinator.RequestAndGet(req)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}

	groups := []map[string]interface{}{}
	for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
		g, err := parseGroupDetail(group)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
		}
		groups = append(groups, g)
	}
	c.JSON(http.StatusOK, groups)
}
