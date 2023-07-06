package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var describeGroupsCmd = &cobra.Command{
	Use:   "describe-groups",
	Short: "describe groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		group, err := cmd.Flags().GetString("group")

		brokers, err := healer.NewBrokers(bs)
		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		coordinatorResponse, err := brokers.FindCoordinator(client, group)
		if err != nil {
			return err
		}

		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return err
		}

		glog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

		req := healer.NewDescribeGroupsRequest(client, []string{group})
		resp, err := coordinator.RequestAndGet(req)
		if err != nil {
			return fmt.Errorf("failed to make describe_groups request: %w", err)
		}

		rst := make([]map[string]interface{}, 0)
		for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
			members := group.Members
			for i := range members {
				e := make(map[string]interface{})
				e["member_id"] = members[i].MemberID
				e["client_id"] = members[i].ClientID
				e["client_host"] = members[i].ClientHost

				memberAssignment, err := healer.NewMemberAssignment(members[i].MemberAssignment)
				if err != nil {
					return err
				}
				for _, p := range memberAssignment.PartitionAssignments {
					e["assignments"] = p
				}
				rst = append(rst, e)
			}
		}

		b, err := json.MarshalIndent(rst, "", "  ")
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
