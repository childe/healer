package cmd

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var simpleConsumerCmd = &cobra.Command{
	Use:   "simple-consumer",
	Short: "consume message from a certain partition of a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		consumerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if client != "" {
			consumerConfig["client.id"] = client
		}
		partition, err := cmd.Flags().GetInt32("partition")
		topic, err := cmd.Flags().GetString("topic")
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}
		offset, err := cmd.Flags().GetInt64("offset")
		maxMessages, err := cmd.Flags().GetInt32("max-messages")
		stopOffset, err := cmd.Flags().GetInt64("stopoffset")
		printOffset, err := cmd.Flags().GetBool("printoffset")
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
		cConfig, err := healer.GetConsumerConfig(consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to create config: %w", err)
		}

		simpleConsumer, err := healer.NewSimpleConsumer(topic, partition, cConfig)
		if err != nil {
			return fmt.Errorf("failed to generate simple consumer: %w", err)
		}

		messages, err := simpleConsumer.Consume(offset, nil)
		if err != nil {
			glog.Errorf("consumer messages error: %s", err)
			return err
		}

		for i := int32(0); i < maxMessages; i++ {
			message := <-messages
			if message.Error != nil {
				fmt.Printf("message error:%s\n", message.Error)
				break
			}
			if stopOffset > 0 && message.Message.Offset >= stopOffset {
				break
			}
			if printOffset {
				fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
			} else {
				fmt.Printf("%s\n", message.Message.Value)
			}
		}
		return nil

	},
}

func init() {
	simpleConsumerCmd.Flags().String("config", "", "XX=YY,AA=ZZ. refer to https://github.com/childe/healer/blob/master/config.go")
	simpleConsumerCmd.Flags().Uint32("partition", 0, "partition id")
	simpleConsumerCmd.Flags().Int64("offset", -1, "the offset to consume from, -2 which means from beginning; while value -1 means from end")
	simpleConsumerCmd.Flags().Int32("max-messages", math.MaxInt32, "the number of messages to output")
	simpleConsumerCmd.Flags().Int64("stopoffset", 0, "consume messages until this point")
	simpleConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
}
