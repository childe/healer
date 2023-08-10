package cmd

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var consoleConsumerCmd = &cobra.Command{
	Use:   "console-consumer",
	Short: "consume messages from a topic. can specify partitions or all partitions",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		consumerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if client != "" {
			consumerConfig["client.id"] = client
		}
		partitions, err := cmd.Flags().GetIntSlice("partitions")
		topic, err := cmd.Flags().GetString("topic")
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}
		maxMessages, err := cmd.Flags().GetInt("max-messages")
		// printOffset, err := cmd.Flags().GetBool("printoffset")
		// jsonFormat, err := cmd.Flags().GetBool("json")
		config, err := cmd.Flags().GetString("config")

		for _, kv := range strings.Split(config, ",") {
			if strings.Trim(kv, " ") == "" {
				continue
			}
			t := strings.SplitN(kv, "=", 2)
			if len(t) != 2 {
				return fmt.Errorf("invalid config : %s", kv)
			}
			consumerConfig[t[0]] = t[1]
		}

		var (
			consumer *healer.Consumer
		)

		if len(partitions) == 0 {
			if consumer, err = healer.NewConsumer(consumerConfig, topic); err != nil {
				return err
			}
		} else {
			assign := make(map[string][]int)
			assign[topic] = make([]int, 0)
			for _, pid := range partitions {
				assign[topic] = append(assign[topic], pid)
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
			fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
		}
		return nil
	},
}

func init() {
	consoleConsumerCmd.Flags().String("config", "", "XX=YY,AA=ZZ. refer to https://github.com/childe/healer/blob/master/config.go")
	consoleConsumerCmd.Flags().IntSlice("partitions", nil, "partition ids, comma-separated")
	consoleConsumerCmd.Flags().Int("max-messages", math.MaxInt, "the number of messages to output")
	consoleConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
	consoleConsumerCmd.Flags().Bool("json", false, "print message in json format")
	consoleConsumerCmd.Flags().StringP("topic", "t", "", "topic name")
}
