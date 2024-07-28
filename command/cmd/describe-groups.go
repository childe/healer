package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var describeGroupsCmd = &cobra.Command{
	Use:   "describe-groups",
	Short: "describe groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			return err
		}

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

		groups := make([]*healer.GroupDetail, 0)
		for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
			groups = append(groups, group)
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
