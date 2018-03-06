package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

type Group map[string]*healer.Broker // groupID -> broker
type Tasks map[string]Group          // topic -> groupID > coordinatorAddress

var groups = map[string]*healer.Broker{}

var (
	bootstrapServers = flag.String("bootstrap.servers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic            = flag.String("topic", "", "get all topics if not given")
	clientID         = flag.String("clientID", "", "The ID of this client.")
	groupID          = flag.String("groupID", "", "get all groupID if not given")

	connectTimeout = flag.Int("connect-timeout", 30, "default 30 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 60, "default 60 Second. read timeout from connection to broker")
)

var (
	brokers *healer.Brokers
	err     error
)

func getPartitions(topic string) ([]int32, error) {
	var metadataResponse *healer.MetadataResponse
	metadataResponse, err = brokers.RequestMetaData(*clientID, []string{topic})

	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0)
	for _, topicMetadata := range metadataResponse.TopicMetadatas {
		for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
			partitions = append(partitions, partitionMetadata.PartitionID)
		}
	}

	return partitions, nil
}

func getOffset(topic string) (map[int32]int64, error) {
	var (
		partitionID int32 = -1
		timestamp   int64 = -1
	)
	offsetsResponses, err := brokers.RequestOffsets(*clientID, topic, partitionID, timestamp, 1)
	if err != nil {
		return nil, err
	}

	rst := make(map[int32]int64)
	for _, offsetsResponse := range offsetsResponses {
		for _, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
			for topic, partitionOffset := range partitionOffsets {
				if len(partitionOffset.Offsets) != 1 {
					return nil, fmt.Errorf("%s[%d] offsets return more than 1 value", topic, partitionOffset.Partition)
				}
				rst[partitionOffset.Partition] = partitionOffset.Offsets[0]
			}
		}
	}
	return rst, nil
}

// TODO remove partitons
func getCommitedOffset(topic string, partitions []int32, groupID string) (map[int32]int64, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(*clientID, groupID)
		if err != nil {
			return nil, err
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("coordinator of %s:%s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	r := healer.NewOffsetFetchRequest(1, *clientID, groupID)
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

func getAllTopics() ([]string, error) {
	var metadataResponse *healer.MetadataResponse
	metadataResponse, err = brokers.RequestMetaData(*clientID, nil)

	if err != nil {
		return nil, err
	}

	topics := make([]string, 0)
	for _, t := range metadataResponse.TopicMetadatas {
		topics = append(topics, t.TopicName)
	}

	return topics, nil
}

func getTopicsInGroup(groupID string) (map[string]bool, error) {
	if _, ok := groups[groupID]; !ok {
		coordinatorResponse, err := brokers.FindCoordinator(*clientID, groupID)
		if err != nil {
			return nil, err
		}
		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return nil, err
		}
		glog.V(5).Infof("coordinator of %s:%s", groupID, coordinator.GetAddress())
		groups[groupID] = coordinator
	}

	req := healer.NewDescribeGroupsRequest(*clientID, []string{groupID})

	responseBytes, err := groups[groupID].Request(req)
	if err != nil {
		return nil, err
	}

	response, err := healer.NewDescribeGroupsResponse(responseBytes)
	if err != nil {
		return nil, err
	}

	topics := make(map[string]bool)
	for _, group := range response.Groups {
		for _, memberDetail := range group.Members {
			memberAssignment, err := healer.NewMemberAssignment(memberDetail.MemberAssignment)
			if err != nil {
				return nil, err
			}
			for _, p := range memberAssignment.PartitionAssignments {
				topics[p.Topic] = true
			}
		}
	}
	return topics, nil
}

func getGroups() ([]string, error) {
	response, err := brokers.RequestListGroups(*clientID)

	if err != nil {
		return nil, err
	}

	groups := []string{}
	for _, group := range response.Groups {
		groups = append(groups, group.GroupID)
	}
	return groups, nil
}

func initTasks(topic, groupID string) Tasks {
	tasks := map[string]Group{}
	if topic != "" && groupID != "" {
		tasks[topic] = Group{groupID: nil}
		return tasks
	}

	if topic == "" && groupID != "" {
		topics, err := getTopicsInGroup(groupID)
		if err != nil {
			glog.Errorf("fetch topics in group[%s] error:%s", groupID, err)
			return nil
		}
		for topic := range topics {
			tasks[topic] = Group{groupID: nil}
		}
		return tasks
	}

	// group is ""
	groupIDs, err := getGroups()
	if err != nil {
		glog.Errorf("get groups error:%s", err)
		return nil
	}
	if glog.V(5) {
		for i, group := range groupIDs {
			glog.Infof("%d/%d %s", i, len(groupIDs), group)
		}
	}

	groupTopics := make(map[string]map[string]bool)
	for _, groupID := range groupIDs {
		topics, err := getTopicsInGroup(groupID)
		if err != nil {
			glog.Errorf("get topics in [%s] error:%s", groupID, err)
			return nil
		}
		groupTopics[groupID] = topics
	}

	// topic != "" && group == ""
	if topic != "" {
		tasks[topic] = Group{}
		for groupID, topics := range groupTopics {
			if _, ok := topics[topic]; ok {
				tasks[topic][groupID] = groups[groupID]
			}
		}
		return tasks
	}

	// topic != "" && group != ""
	topics, err := getAllTopics()
	if err != nil {
		glog.Errorf("get all topics error:%s", err)
		return nil
	}
	for _, topic := range topics {
		tasks[topic] = Group{}
		for groupID, topics := range groupTopics {
			if _, ok := topics[topic]; ok {
				tasks[topic][groupID] = groups[groupID]
			}
		}
	}
	return tasks
}

func main() {
	flag.Parse()

	brokers, err = healer.NewBrokers(*bootstrapServers, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	tasks := initTasks(*topic, *groupID)
	if tasks == nil {
		os.Exit(5)
	}
	fmt.Println("timestamp\ttopic\tgroupID\tpid\toffset\tcommited\tlag")
	for topicName, group := range tasks {
		partitions, err := getPartitions(topicName)
		if err != nil {
			glog.Errorf("get partitions error:%s", err)
			os.Exit(5)
		}

		timestamp := time.Now().Unix()
		offsets, err := getOffset(topicName)
		if err != nil {
			glog.Errorf("get offsets error:%s", err)
			os.Exit(5)
		}

		for groupID := range group {
			commitedOffsets, err := getCommitedOffset(topicName, partitions, groupID)
			if err != nil {
				glog.Errorf("get commitedOffsets error:%s", err)
				os.Exit(5)
			}

			var (
				offsetSum   int64 = 0
				commitedSum int64 = 0
				pendingSum  int64 = 0
			)

			for _, partitionID := range partitions {
				pending := offsets[partitionID] - commitedOffsets[partitionID]
				offsetSum += offsets[partitionID]
				commitedSum += commitedOffsets[partitionID]
				pendingSum += pending
				fmt.Printf("%d\t%s\t%s\t%d\t%d\t%d\t%d\n", timestamp, topicName, groupID, partitionID, offsets[partitionID], commitedOffsets[partitionID], pending)
			}
			fmt.Printf("total:\t%d\t%d\t%d\n", offsetSum, commitedSum, pendingSum)
		}
	}
}
