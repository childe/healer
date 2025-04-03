package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var simpleConsumerCmd = &cobra.Command{
	Use:   "simple-consumer",
	Short: "consume message from a certain partition of a topic",

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
		partition, err := cmd.Flags().GetInt32("partition")
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
		offset, err := cmd.Flags().GetInt64("offset")
		if err != nil {
			return err
		}
		maxMessages, err := cmd.Flags().GetInt32("max-messages")
		if err != nil {
			return err
		}
		stopOffset, err := cmd.Flags().GetInt64("stopoffset")
		if err != nil {
			return err
		}
		printOffset, err := cmd.Flags().GetBool("printoffset")
		if err != nil {
			return err
		}
		jsonFormat, err := cmd.Flags().GetBool("json")
		if err != nil {
			return err
		}
		config, err := cmd.Flags().GetString("config")
		if err != nil {
			return err
		}
		json.Unmarshal([]byte(config), &consumerConfig)

		simpleConsumer, err := healer.NewSimpleConsumer(topic, partition, consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to generate simple consumer: %w", err)
		}
		defer simpleConsumer.Stop()

		messages, err := simpleConsumer.Consume(offset, nil)
		if err != nil {
			klog.Errorf("consumer messages error: %s", err)
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

			if jsonFormat {
				b, err := json.Marshal(message.Message)
				if err != nil {
					fmt.Printf("marshal message error:%s\n", err)
				} else {
					fmt.Printf("%s\n", b)
				}
			} else {
				if printOffset {
					fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
				} else {
					fmt.Printf("%s\n", message.Message.Value)
				}
			}
		}
		return nil

	},
}

func init() {
	simpleConsumerCmd.Flags().String("config", "", `{"xx"="yy","aa"="zz"} refer to https://github.com/childe/healer/blob/master/config.go`)
	simpleConsumerCmd.Flags().Int32("partition", 0, "partition id")
	simpleConsumerCmd.Flags().Int64("offset", -1, "the offset to consume from, -2 which means from beginning; while value -1 means from end")
	simpleConsumerCmd.Flags().Int32("max-messages", math.MaxInt32, "the number of messages to output")
	simpleConsumerCmd.Flags().Int64("stopoffset", 0, "consume messages until this point")
	simpleConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
	simpleConsumerCmd.Flags().Bool("json", false, "print message in json format")
	simpleConsumerCmd.Flags().StringP("topic", "t", "", "topic name")
}
