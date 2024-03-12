package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
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

var describeGroupsCmd = &cobra.Command{
	Use:   "describe-groups",
	Short: "describe groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		group, err := cmd.Flags().GetString("group")

		brokers, err := healer.NewBrokers(bs)
		if err != nil {
			return err
		}

		coordinatorResponse, err := brokers.FindCoordinator(client, group)
		if err != nil {
			return err
		}

		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return err
		}

		klog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

		req := healer.NewDescribeGroupsRequest(client, []string{group})
		resp, err := coordinator.RequestAndGet(req)
		if err != nil {
			return fmt.Errorf("failed to make describe_groups request: %w", err)
		}

		groups := []map[string]interface{}{}
		for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
			g, err := parseGroupDetail(group)
			if err != nil {
				return err
			}
			groups = append(groups, g)
		}

		b, err := json.MarshalIndent(groups, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))

		return nil
	},
}

func init() {
	describeGroupsCmd.Flags().String("group", "", "group names")
}
