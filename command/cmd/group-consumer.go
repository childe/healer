package cmd

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var groupConsumerCmd = &cobra.Command{
	Use:   "group-consumer",
	Short: "Use a group to consume messages from a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		consumerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if client != "" {
			consumerConfig["client.id"] = client
		}
		topic, err := cmd.Flags().GetString("topic")
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}
		group, err := cmd.Flags().GetString("group")
		if group == "" || err != nil {
			return errors.New("group must be specified")
		}
		consumerConfig["group.id"] = group

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

		consumer, err := healer.NewGroupConsumer(topic, consumerConfig)
		if err != nil {
			return err
		}

		messages, err := consumer.Consume(nil)
		if err != nil {
			return err
		}

		if maxMessages <= 0 {
			for {
				message := <-messages
				fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
			}
		} else {
			for i := 0; i < maxMessages; i++ {
				message := <-messages
				fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
			}
		}
		return nil
	},
}

func init() {
	groupConsumerCmd.Flags().StringP("topic", "t", "", "topic name")
	groupConsumerCmd.Flags().StringP("group", "g", "", "group id")
	groupConsumerCmd.Flags().String("config", "", "XX=YY,AA=ZZ. refer to https://github.com/childe/healer/blob/master/config.go")
	groupConsumerCmd.Flags().Int("max-messages", math.MaxInt, "the number of messages to output")
	groupConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
	groupConsumerCmd.Flags().Bool("json", false, "print message in json format")
}
