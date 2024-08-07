package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var groupConsumerCmd = &cobra.Command{
	Use:   "group-consumer",
	Short: "Use a group to consume messages from a topic",

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
		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			return err
		}
		if group == "" || err != nil {
			return errors.New("group must be specified")
		}
		consumerConfig["group.id"] = group

		maxMessages, err := cmd.Flags().GetInt("max-messages")
		if err != nil {
			return err
		}
		// printOffset, err := cmd.Flags().GetBool("printoffset")
		// jsonFormat, err := cmd.Flags().GetBool("json")
		config, err := cmd.Flags().GetString("config")
		if err != nil {
			return err
		}
		json.Unmarshal([]byte(config), &consumerConfig)

		consumer, err := healer.NewGroupConsumer(topic, consumerConfig)
		if err != nil {
			return err
		}
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

		defer consumer.Close()

		messages, err := consumer.Consume(nil)
		if err != nil {
			return err
		}

		i := 0
		for {
			select {
			case <-sigChan:
				return nil
			case message := <-messages:
				fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
				i++
				if maxMessages > 0 && i >= maxMessages {
					return nil
				}
			}
		}
	},
}

func init() {
	groupConsumerCmd.Flags().StringP("topic", "t", "", "topic name")
	groupConsumerCmd.Flags().StringP("group", "g", "", "group id")
	groupConsumerCmd.Flags().String("config", "", `{"xx"="yy","aa"="zz"} refer to https://github.com/childe/healer/blob/master/config.go`)
	groupConsumerCmd.Flags().Int("max-messages", math.MaxInt, "the number of messages to output")
	groupConsumerCmd.Flags().Bool("printoffset", true, "if print offset of each message")
	groupConsumerCmd.Flags().Bool("json", false, "print message in json format")
}
