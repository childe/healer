package cmd

import (
	"encoding/json"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var electLeadersCmd = &cobra.Command{
	Use:   "elect-leaders",
	Short: "elect leaders",

	RunE: func(cmd *cobra.Command, args []string) error {
		type topicPartition struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
		}

		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return err
		}

		dataJSONStr, err := cmd.Flags().GetString("data")
		if err != nil {
			return err
		}

		topicPartitions := make([]topicPartition, 0)
		err = json.Unmarshal([]byte(dataJSONStr), &topicPartitions)
		if err != nil {
			return err
		}

		req := healer.ElectLeadersRequest{
			RequestHeader: &healer.RequestHeader{
				APIKey:     43,
				APIVersion: 0,
				ClientID:   client,
			},
		}
		for _, v := range topicPartitions {
			req.Add(v.Topic, v.Partition)
		}

		resp, err := bs.Request(&req)
		if err != nil {
			return err
		}
		glog.Info(resp)

		return nil
	},
}

func init() {
	electLeadersCmd.Flags().StringP("data", "d", "", `json format data. [{"topic":"test","partition":0},{"topic":"test","partition":2}]`)
	rootCmd.AddCommand(electLeadersCmd)
}
