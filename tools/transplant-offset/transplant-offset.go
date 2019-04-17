package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokersList    = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic          = flag.String("topic", "", "REQUIRED")
	offsetsStorage = flag.String("offsets.storage", "kafka", "Select where offsets should be stored (zookeeper or kafka).")
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	srcGroup       = flag.String("src.group", "", "REQUIRED")
	dstGroup       = flag.String("dst.group", "", "REQUIRED")
)

var (
	brokers *healer.Brokers
	err     error
)

func getCommittedOffset(topic string, partitions []int32, groupID string) (map[int32]int64, error) {
	coordinatorResponse, err := brokers.FindCoordinator(*clientID, groupID)
	if err != nil {
		return nil, err
	}
	coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		return nil, err
	}
	glog.V(5).Infof("coordinator of %s: %s", groupID, coordinator.GetAddress())

	var apiVersion uint16
	if *offsetsStorage == "zookeeper" {
		apiVersion = 0
	} else {
		apiVersion = 1
	}
	r := healer.NewOffsetFetchRequest(apiVersion, *clientID, groupID)
	for _, p := range partitions {
		r.AddPartiton(topic, p)
	}

	response, err := coordinator.Request(r)
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

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	if *srcGroup == "" {
		flag.PrintDefaults()
		fmt.Println("need src group name")
		os.Exit(4)
	}

	if *dstGroup == "" {
		flag.PrintDefaults()
		fmt.Println("need dst group name")
		os.Exit(4)
	}

	brokers, err = healer.NewBrokers(*brokersList)
	if err != nil {
		glog.Errorf("failed to create brokers: %s", err)
		os.Exit(5)
	}

	metaDataResponse, err := brokers.RequestMetaData(*clientID, []string{*topic})
	if err != nil {
		glog.Fatalf("could not get metadata:%s", err)
	}

	b, _ := json.Marshal(metaDataResponse)
	glog.Infof("topic[%s] metadata:%s", *topic, b)

	// only one topic
	topicMetadata := metaDataResponse.TopicMetadatas[0]

	// get commited offset from src group
	var (
		partitions []int32         = make([]int32, 0)
		offsets    map[int32]int64 = make(map[int32]int64)
	)

	for _, partitionMetadata := range topicMetadata.PartitionMetadatas {
		partitions = append(partitions, partitionMetadata.PartitionID)
	}

	offsets, err = getCommittedOffset(*topic, partitions, *srcGroup)
	if err != nil {
		glog.Errorf("could not get commited offset: %s", err)
		os.Exit(5)
	}

	glog.Infof("original commited offset of %s from %s", *topic, *srcGroup)
	for pid, offset := range offsets {
		glog.Infof("%d %d", pid, offset)
	}

	// commit offset

	// 1. get coordinator
	var coordinator *healer.Broker
	coordinatorResponse, err := brokers.FindCoordinator(*clientID, *dstGroup)
	if err != nil {
		glog.Fatalf("failed to find coordinator:%s", err)
	}

	coordinator, err = brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		glog.Fatalf("could not get broker[%d]:%s", coordinatorResponse.Coordinator.NodeID, err)
	}
	glog.Infof("coordinator for group[%s]:%s", *dstGroup, coordinator.GetAddress())

	/*
		// 2. join
		var (
			protocolType   string = "consumer"
			memberID       string = ""
			generationID   int32
			sessionTimeout int32 = 30000
		)

		protocolMetadata := &healer.ProtocolMetadata{
			Version:      0,
			Subscription: []string{*topic},
			UserData:     nil,
		}

		gps := []*healer.GroupProtocol{&healer.GroupProtocol{"range", protocolMetadata.Encode()}}
		joinGroupRequest := healer.NewJoinGroupRequest(*clientID, *dstGroup, sessionTimeout, memberID, protocolType)
		for _, gp := range gps {
			joinGroupRequest.AddGroupProtocal(gp)
		}

		glog.Info("join...")
		responseBytes, err := coordinator.Request(joinGroupRequest)
		if err != nil {
			glog.Fatalf("request joingroup error:%s", err)
		}

		joinGroupResponse, err := healer.NewJoinGroupResponse(responseBytes)
		if err != nil {
			glog.Fatalf("get join group response error:%s", err)
		}

		generationID = joinGroupResponse.GenerationID
		memberID = joinGroupResponse.MemberID
		glog.Infof("generationID:%d memberID:%s", generationID, memberID)

		// 3. sync
		var groupAssignment healer.GroupAssignment = nil
		syncGroupRequest := healer.NewSyncGroupRequest(*clientID, *dstGroup, generationID, memberID, groupAssignment)

		responseBytes, err = coordinator.Request(syncGroupRequest)
		if err != nil {
			glog.Fatalf("request sync api error: %s", err)
		}

		_, err = healer.NewSyncGroupResponse(responseBytes)

		if err != nil {
			glog.Fatalf("decode sync response error: %s", err)
		}
	*/

	// 4. commit
	var (
		apiVersion uint16
	)
	if *offsetsStorage == "zookeeper" {
		apiVersion = 0
	} else {
		apiVersion = 2
	}
	offsetComimtReq := healer.NewOffsetCommitRequest(apiVersion, *clientID, *dstGroup)
	offsetComimtReq.SetMemberID("")
	offsetComimtReq.SetGenerationID(-1)
	offsetComimtReq.SetRetentionTime(-1)
	for partitionID, offset := range offsets {
		offsetComimtReq.AddPartiton(*topic, partitionID, offset, "")
		glog.Infof("commit offset [%s][%d]:%d", *topic, partitionID, offset)
	}

	payload, err := coordinator.Request(offsetComimtReq)
	if err != nil {
		glog.Infof("request commit offset api error: %s", err)
		os.Exit(5)
	}

	_, err = healer.NewOffsetCommitResponse(payload)
	if err != nil {
		glog.Errorf("decode commit offset response error: %s", err)
		os.Exit(5)
	}
}
