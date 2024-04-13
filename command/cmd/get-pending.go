package cmd

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
	helper *healer.Helper
	groups = map[string]*healer.Broker{}

	brokers *healer.Brokers
)

type By []int32

func (a By) Len() int           { return len(a) }
func (a By) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a By) Less(i, j int) bool { return a[i] < a[j] }

func getPartitions(topic, client string) ([]int32, error) {
	metadataResponse, err := brokers.RequestMetaData(client, []string{topic})

	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0)
	for _, topicMetadata := range metadataResponse.TopicMetadatas {
		for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
			partitions = append(partitions, partitionMetadata.PartitionID)
		}
	}

	sort.Sort(By(partitions))
	return partitions, nil
}

func getOffset(topic, client string) (map[int32]int64, error) {
	var (
		partitionID int32 = -1
		timestamp   int64 = -1
	)
	offsetsResponses, err := brokers.RequestOffsets(client, topic, partitionID, timestamp, 1)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, offsetsResponse := range offsetsResponses {
		for topic, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
			for _, partitionOffset := range partitionOffsets {
				if len(partitionOffset.Offsets) == 0 {
					rst[partitionOffset.Partition] = -1
					continue
				}
				if len(partitionOffset.Offsets) != 1 {
					return nil, fmt.Errorf("%s[%d] offsets length mismatch: %v", topic, partitionOffset.Partition, partitionOffset.Offsets)
				}
				rst[partitionOffset.Partition] = partitionOffset.Offsets[0]
			}
		}
	}
	return rst, nil
}

// TODO remove partitons parameters
func getCommittedOffset(topic string, partitions []int32, groupID, client string) (map[int32]int64, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(client, groupID)
		if err != nil {
			return nil, err
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return nil, err
		}
		klog.V(5).Infof("coordinator of %s: %s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	r := healer.NewOffsetFetchRequest(1, client, groupID)
	for _, p := range partitions {
		r.AddPartiton(topic, p)
	}

	resp, err := groups[groupID].RequestAndGet(r)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, t := range resp.(healer.OffsetFetchResponse).Topics {
		for _, p := range t.Partitions {
			rst[p.PartitionID] = p.Offset
		}
	}
	return rst, nil
}

// topicName -> partitionID -> memberID
func getSubscriptionsInGroup(groupID, client string) (map[string]map[int32]string, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in getSubscriptionsInGroup", r)
		}
	}()
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(client, groupID)
		if err != nil {
			return nil, fmt.Errorf("could not find coordinator of %s: %s", groupID, err)
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return nil, fmt.Errorf("get broker error: %s", err)
		}
		klog.V(5).Infof("coordinator of %s: %s", groupID, coordinator)
		groups[groupID] = coordinator
	}

	req := healer.NewDescribeGroupsRequest(client, []string{groupID})

	resp, err := groups[groupID].RequestAndGet(req)
	if err != nil {
		return nil, err
	}

	subscriptions := make(map[string]map[int32]string)
	for _, group := range resp.(healer.DescribeGroupsResponse).Groups {
		for _, memberDetail := range group.Members {
			memberID := memberDetail.MemberID
			if len(memberDetail.RawMemberAssignment) == 0 {
				continue
			}
			memberAssignment, err := healer.NewMemberAssignment(memberDetail.RawMemberAssignment)
			if err != nil {
				return nil, err
			}
			for _, p := range memberAssignment.PartitionAssignments {
				topicName := p.Topic
				if _, ok := subscriptions[topicName]; !ok {
					subscriptions[topicName] = make(map[int32]string)
				}
				for _, partitionID := range p.Partitions {
					subscriptions[topicName][partitionID] = memberID
				}
			}
		}
	}
	return subscriptions, nil
}

func getGroups(groupID string) []string {
	if groupID == "" {
		return helper.GetGroups()
	}
	if !strings.Contains(groupID, "*") {
		return []string{groupID}
	}

	var p string = strings.Replace(groupID, ".", `\.`, -1)
	p = strings.Replace(p, "*", ".*", -1)
	var groupPattern *regexp.Regexp = regexp.MustCompile("^" + p + "$")

	var (
		rst      []string = make([]string, 0)
		groupIDs []string = helper.GetGroups()
	)
	if groupIDs == nil {
		return nil
	}
	for _, g := range groupIDs {
		if groupPattern.MatchString(g) {
			rst = append(rst, g)
		}
	}
	return rst
}

var getPendingCmd = &cobra.Command{
	Use:   "get-pending",
	Short: "get pending count of a group for a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		bootstrapBrokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		groupID, err := cmd.Flags().GetString("group")
		header, err := cmd.Flags().GetBool("header")
		total, err := cmd.Flags().GetBool("total")

		brokers, err = healer.NewBrokers(bootstrapBrokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers: %w", err)
		}
		helper = healer.NewHelperFromBrokers(brokers, client)

		groupIDs := getGroups(groupID)
		for i, group := range groupIDs {
			klog.V(5).Infof("%d/%d %s", i, len(groupIDs), group)
		}

		for i, groupID := range groupIDs {
			klog.V(5).Infof("%d/%d %s", i, len(groupIDs), groupID)
			subscriptions, err := getSubscriptionsInGroup(groupID, client)
			if err != nil {
				klog.Errorf("get subscriptions of %s error: %s", groupID, err)
				continue
			}
			klog.V(5).Infof("%d topics: %v", len(subscriptions), subscriptions)

			if topic != "" {
				if _, ok := subscriptions[topic]; !ok {
					klog.V(10).Infof("topic %s not found in group %s", topic, groupID)
					continue
				}
				var topicName = topic
				timestamp := time.Now().Unix()

				offsets, err := getOffset(topicName, client)
				if err != nil {
					klog.Errorf("get offsets error: %s", err)
					continue
				}

				partitions, err := getPartitions(topicName, client)
				if err != nil {
					klog.Errorf("get partitions of %s error: %s", topicName, err)
					continue
				}
				committedOffsets, err := getCommittedOffset(topicName, partitions, groupID, client)
				if err != nil {
					klog.Errorf("get committed offsets [%s/%s] error: %s", groupID, topicName, err)
					continue
				}

				var (
					offsetSum    int64 = 0
					committedSum int64 = 0
					pendingSum   int64 = 0
				)

				if header {
					fmt.Println("timestamp  topic  groupID  pid  offset  commited  lag")
				}

				for _, partitionID := range partitions {
					pending := offsets[partitionID] - committedOffsets[partitionID]
					if offsets[partitionID] == -1 {
						pending = 0
					}
					offsetSum += offsets[partitionID]
					committedSum += committedOffsets[partitionID]
					pendingSum += pending
					fmt.Printf("%d  %s  %s  %d  %d  %d  %d\n", timestamp, topicName, groupID, partitionID, offsets[partitionID], committedOffsets[partitionID], pending)
				}
				if total {
					fmt.Printf("TOTAL  %s  %s  %d  %d  %d\n", topicName, groupID, offsetSum, committedSum, pendingSum)
				}
				continue
			}

			for topicName, v := range subscriptions {
				klog.V(5).Infof("topic: %s", topicName)
				if topic != "" && topicName != topic {
					continue
				}

				timestamp := time.Now().Unix()

				offsets, err := getOffset(topicName, client)
				if err != nil {
					klog.Errorf("get offsets error: %s", err)
					continue
				}

				partitions, err := getPartitions(topicName, client)
				if err != nil {
					klog.Errorf("get partitions of %s error: %s", topicName, err)
					continue
				}
				committedOffsets, err := getCommittedOffset(topicName, partitions, groupID, client)
				if err != nil {
					klog.Errorf("get committed offsets [%s/%s] error: %s", groupID, topicName, err)
					continue
				}

				var (
					offsetSum    int64 = 0
					committedSum int64 = 0
					pendingSum   int64 = 0
				)

				if header {
					fmt.Println("timestamp  topic  groupID  pid  offset  commited  lag  owner")
				}

				for _, partitionID := range partitions {
					pending := offsets[partitionID] - committedOffsets[partitionID]
					offsetSum += offsets[partitionID]
					committedSum += committedOffsets[partitionID]
					pendingSum += pending
					fmt.Printf("%d  %s  %s  %d  %d  %d  %d  %s\n", timestamp, topicName, groupID, partitionID, offsets[partitionID], committedOffsets[partitionID], pending, v[partitionID])
				}
				if total {
					fmt.Printf("TOTAL  %s  %s  %d  %d  %d\n", topicName, groupID, offsetSum, committedSum, pendingSum)
				}
			}
		}
		return nil
	},
}

func init() {
	getPendingCmd.Flags().StringP("group", "g", "", "group name")
	getPendingCmd.Flags().Bool("total", false, "if print total pending")
	getPendingCmd.Flags().Bool("header", true, "if print header")
	getPendingCmd.Flags().StringP("topic", "t", "", "topic name")
}
