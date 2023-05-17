package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var deleteGroupsCmd = &cobra.Command{
	Use:   "delete-groups",
	Short: "delete groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bootStrapBrokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		groups, err := cmd.Flags().GetStringSlice("groups")

		brokers, err := healer.NewBrokers(bootStrapBrokers)
		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		for _, group := range groups {
			coordinatorResponse, err := brokers.FindCoordinator(client, group)
			if err != nil {
				return err
			}

			coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
			if err != nil {
				return err
			}
			glog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

			req := healer.NewDeleteGroupsRequest(client, groups)
			respBody, err := coordinator.Request(req)
			if err != nil {
				return fmt.Errorf("failed to request delete_groups: %w", err)
			}
			resp, err := healer.NewDeleteGroupsResponse(respBody)

			if err != nil {
				return fmt.Errorf("failed to get delete_groups response: %w", err)
			}

			s, err := json.MarshalIndent(resp, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal delete_groups response: %w", err)
			}
			fmt.Println(string(s))
		}

		return nil
	},
}

func init() {
	deleteGroupsCmd.Flags().StringSlice("groups", nil, "group names, separated by comma")
}
