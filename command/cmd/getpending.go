package cmd

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
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
	var metadataResponse *healer.MetadataResponse
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
				if len(partitionOffset.Offsets) != 1 {
					return nil, fmt.Errorf("%s[%d] offsets return more than 1 value", topic, partitionOffset.Partition)
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
		glog.V(5).Infof("coordinator of %s: %s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	r := healer.NewOffsetFetchRequest(1, client, groupID)
	for _, p := range partitions {
		r.AddPartiton(topic, p)
	}

	response, err := groups[groupID].Request(r)
	if err != nil {
		return nil, err
	}

	res, err := healer.NewOffsetFetchResponse(response)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, t := range res.Topics {
		for _, p := range t.Partitions {
			rst[p.PartitionID] = p.Offset
		}
	}
	return rst, nil
}

// topicName -> partitionID -> memberID
func getSubscriptionsInGroup(groupID, client string) (map[string]map[int32]string, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(client, groupID)
		if err != nil {
			glog.Fatalf("could not find coordinator: %s", err)
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			glog.Fatalf("get broker error: %s", err)
		}
		glog.V(5).Infof("coordinator of %s:%s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	req := healer.NewDescribeGroupsRequest(client, []string{groupID})

	responseBytes, err := groups[groupID].Request(req)
	if err != nil {
		return nil, err
	}

	response, err := healer.NewDescribeGroupsResponse(responseBytes)
	if err != nil {
		return nil, err
	}

	subscriptions := make(map[string]map[int32]string)
	for _, group := range response.Groups {
		for _, memberDetail := range group.Members {
			memberID := memberDetail.MemberID
			if len(memberDetail.MemberAssignment) == 0 {
				continue
			}
			memberAssignment, err := healer.NewMemberAssignment(memberDetail.MemberAssignment)
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
	Use:   "getpending",
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
		if glog.V(5) {
			for i, group := range groupIDs {
				glog.Infof("%d/%d %s", i, len(groupIDs), group)
			}
		}

		for i, groupID := range groupIDs {
			if topic != "" {
				var topicName = topic
				timestamp := time.Now().Unix()

				offsets, err := getOffset(topicName, client)
				if err != nil {
					glog.Errorf("get offsets error: %s", err)
					continue
				}

				partitions, err := getPartitions(topicName, client)
				if err != nil {
					glog.Errorf("get partitions of %s error: %s", topicName, err)
					continue
				}
				committedOffsets, err := getCommittedOffset(topicName, partitions, groupID, client)
				if err != nil {
					glog.Errorf("get committed offsets [%s/%s] error: %s", groupID, topicName, err)
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

			glog.V(5).Infof("%d/%d %s", i, len(groupIDs), groupID)
			subscriptions, err := getSubscriptionsInGroup(groupID, client)
			if err != nil {
				glog.Errorf("get subscriptions of %s error: %s", groupID, err)
				continue
			}

			glog.V(5).Infof("%d topics", len(subscriptions))

			for topicName, v := range subscriptions {
				glog.V(5).Infof("topic: %s", topicName)
				if topic != "" && topicName != topic {
					continue
				}

				timestamp := time.Now().Unix()

				offsets, err := getOffset(topicName, client)
				if err != nil {
					glog.Errorf("get offsets error: %s", err)
					continue
				}

				partitions, err := getPartitions(topicName, client)
				if err != nil {
					glog.Errorf("get partitions of %s error: %s", topicName, err)
					continue
				}
				committedOffsets, err := getCommittedOffset(topicName, partitions, groupID, client)
				if err != nil {
					glog.Errorf("get committed offsets [%s/%s] error: %s", groupID, topicName, err)
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
	getPendingCmd.Flags().String("group", "", "group name")
	getPendingCmd.Flags().Bool("total", false, "if print total pending")
	getPendingCmd.Flags().Bool("header", true, "if print header")
}
