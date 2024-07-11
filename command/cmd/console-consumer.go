package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var consoleConsumerCmd = &cobra.Command{
	Use:   "console-consumer",
	Short: "consume messages from a topic. can specify partitions or all partitions",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		consumerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		if client != "" {
			consumerConfig["client.id"] = client
		}
		partitions, err := cmd.Flags().GetIntSlice("partitions")
		if err != nil {
			return err
		}
		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}
		maxMessages, err := cmd.Flags().GetInt("max-messages")
		if err != nil {
			return err
		}
		ifJson, err := cmd.Flags().GetBool("json")
		if err != nil {
			return err
		}
		config, err := cmd.Flags().GetString("config")
		if err != nil {
			return err
		}

		json.Unmarshal([]byte(config), &consumerConfig)

		var (
			consumer *healer.Consumer
		)

		if len(partitions) == 0 {
			if consumer, err = healer.NewConsumer(consumerConfig, topic); err != nil {
				return err
			}
		} else {
			assign := map[string][]int{
				topic: partitions,
			}
			if consumer, err = healer.NewConsumer(consumerConfig); err != nil {
				return err
			}
			consumer.Assign(assign)
		}

		messages, err := consumer.Consume(nil)
		if err != nil {
			return err
		}

		for i := 0; i < maxMessages; i++ {
			message := <-messages
			if ifJson {
				b, _ := json.Marshal(message)
				fmt.Printf("%s\n", string(b))
			} else {
				fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
			}
		}
		return nil
	},
}

func init() {
	consoleConsumerCmd.Flags().String("config", "", `{"xx"="yy","aa"="zz"} refer to https://github.com/childe/healer/blob/master/config.go`)
	consoleConsumerCmd.Flags().IntSlice("partitions", nil, "partition ids, comma-separated")
	consoleConsumerCmd.Flags().Int("max-messages", math.MaxInt, "the number of messages to output")
	consoleConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
	consoleConsumerCmd.Flags().Bool("json", false, "print all attributes of message in json format")
	consoleConsumerCmd.Flags().StringP("topic", "t", "", "topic name")
}
