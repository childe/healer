package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var deleteGroupsCmd = &cobra.Command{
	Use:   "delete-groups",
	Short: "delete groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bootStrapBrokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		groups, err := cmd.Flags().GetStringSlice("groups")
		if err != nil {
			return err
		}

		// Use the new Client.DeleteGroups method
		healerClient, err := healer.NewClient(bootStrapBrokers, client)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer healerClient.Close()

		// Use the new DeleteGroups method
		resp, err := healerClient.DeleteGroups(groups)
		if err != nil {
			return fmt.Errorf("failed to delete groups: %w", err)
		}

		// Check for errors in response
		if respErr := resp.Error(); respErr != nil {
			klog.Warningf("Some groups may not have been deleted successfully: %v", respErr)
		}

		// Print response in JSON format
		s, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal delete_groups response: %w", err)
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	deleteGroupsCmd.Flags().StringSliceP("groups", "g", nil, "group names, separated by comma")
}
