package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var alterConfigsCmd = &cobra.Command{
	Use:   "alter-configs",
	Short: "alter configs",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		resourceType, err := cmd.Flags().GetString("resource-type")
		if err != nil {
			return err
		}
		resourceName, err := cmd.Flags().GetString("resource-name")
		if err != nil {
			return err
		}
		configName, err := cmd.Flags().GetString("config-name")
		if err != nil {
			return err
		}
		configValue, err := cmd.Flags().GetString("config-value")
		if err != nil {
			return err
		}

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}
		klog.V(5).Infof("controller: %s", controller.GetAddress())

		r := healer.NewIncrementalAlterConfigsRequest(client)
		r.AddConfig(healer.ConvertConfigResourceType(resourceType), resourceName, configName, configValue)

		resp, err := controller.RequestAndGet(r)
		if err != nil {
			return fmt.Errorf("faild to alter configs: %w", err)
		}

		b, _ := json.MarshalIndent(resp.(healer.IncrementalAlterConfigsResponse), "", "  ")
		fmt.Println(string(b))
		return nil
	},
}

func init() {
	alterConfigsCmd.Flags().String("resource-type", "", "the resource type: topic, broker, broker_logger")
	alterConfigsCmd.Flags().String("resource-name", "", "the resource name")
	alterConfigsCmd.Flags().String("config-name", "", "the config name")
	alterConfigsCmd.Flags().String("config-value", "", "the config value")
}
