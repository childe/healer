package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
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

		timeoutMS, err := cmd.Flags().GetInt32("timeout.ms")
		brokers, err := cmd.Flags().GetString("brokers")

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return err
		}

		dataJSONStr, err := cmd.Flags().GetString("data")
		if err != nil {
			return err
		}

		req := healer.NewElectLeadersRequest(timeoutMS)
		if dataJSONStr != "" {
			topicPartitions := make([]topicPartition, 0)
			err = json.Unmarshal([]byte(dataJSONStr), &topicPartitions)
			if err != nil {
				return err
			}

			for _, v := range topicPartitions {
				req.Add(v.Topic, v.Partition)
			}
		}

		resp, err := bs.ElectLeaders(&req)
		if err != nil {
			return err
		}
		s, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	electLeadersCmd.Flags().StringP("data", "d", "", `json format data. [{"topic":"test","partition":0},{"topic":"test","partition":2}]`)
	electLeadersCmd.Flags().Int32("timeout.ms", 0, `The time in ms to wait for the response.`)
	rootCmd.AddCommand(electLeadersCmd)
}
