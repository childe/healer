package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

func genPartitionAssignments(assignments string) (partitionAssignments [][]int32, err error) {
	for i, brokerIDs := range strings.Split(assignments, ",") {
		if brokerIDs == "" {
			continue
		}
		partitionAssignments = append(partitionAssignments, []int32{})
		for _, brokerID := range strings.Split(brokerIDs, ":") {
			if _brokerID, err := strconv.Atoi(brokerID); err == nil {
				partitionAssignments[i] = append(partitionAssignments[i], int32(_brokerID))
			} else {
				return nil, fmt.Errorf("failed to convert %s to int: %w", brokerID, err)
			}
		}
	}
	return
}

var createPartitionsCmd = &cobra.Command{
	Use:   "create-partitions",
	Short: "create partitions",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}
		assignments, err := cmd.Flags().GetString("assignments")
		if err != nil {
			return err
		}
		timeout, err := cmd.Flags().GetUint32("timeout")
		if err != nil {
			return err
		}
		validateOnly, err := cmd.Flags().GetBool("validate-only")
		if err != nil {
			return err
		}
		count, err := cmd.Flags().GetInt32("count")
		if err != nil {
			return err
		}

		// Use the new Client.CreatePartitions method for simple cases (no assignments)
		if assignments == "" {
			// Create client instance
			healerClient, err := healer.NewClient(brokers, client)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer healerClient.Close()

			// Use the new CreatePartitions method
			resp, err := healerClient.CreatePartitions(topic, count, timeout, validateOnly)
			if err != nil {
				return fmt.Errorf("failed to create partitions: %w", err)
			}

			// Check for errors in response
			if respErr := resp.Error(); respErr != nil {
				return fmt.Errorf("create partitions failed: %w", respErr)
			}

			fmt.Printf("Successfully created partitions for topic '%s' with total count: %d\n", topic, count)
			return nil
		}

		// For complex cases with assignments, use the new CreatePartitionsWithAssignments method
		healerClient, err := healer.NewClient(brokers, client)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		defer healerClient.Close()

		partitionAssignments, err := genPartitionAssignments(assignments)
		if err != nil {
			return err
		}

		// Use the new CreatePartitionsWithAssignments method
		resp, err := healerClient.CreatePartitionsWithAssignments(topic, count, partitionAssignments, timeout, validateOnly)
		if err != nil {
			return fmt.Errorf("failed to create partitions with assignments: %w", err)
		}

		// Check for errors in response
		if respErr := resp.Error(); respErr != nil {
			return fmt.Errorf("create partitions with assignments failed: %w", respErr)
		}

		fmt.Printf("Successfully created partitions for topic '%s' with total count: %d and custom assignments\n", topic, count)
		return nil
	},
}

func init() {
	createPartitionsCmd.Flags().StringP("topic", "t", "", "The topic name.")
	createPartitionsCmd.Flags().String("assignments", "", "The new partition assignments. broker_id_for_part1_replica1:broker_id_for_part1_replica2,broker_id_for_part2_replica1:broker_id_for_part2_replica2,... ")
	createPartitionsCmd.Flags().Int32("count", 0, "The new partition count.")
	createPartitionsCmd.Flags().Uint32("timeout", 30000, "The time in ms to wait for the partitions to be created.")
	createPartitionsCmd.Flags().Bool("validate-only", true, "If true, then validate the request, but don't actually increase the number of partitions.")
}
