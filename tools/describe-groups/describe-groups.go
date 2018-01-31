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
	brokerList     = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	connectTimeout = flag.Int("connect-timeout", 10, "default 10 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 30, "default 30 Second. read timeout from connection to broker")
	groupID        = flag.String("groupID", "", "REQUIRED. groupID need to be described")
)

func main() {
	flag.Parse()

	if *groupID == "" {
		glog.Error("need groupID")
		flag.PrintDefaults()
		os.Exit(4)
	}

	brokers, err := healer.NewBrokers(*brokerList, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	coordinatorResponse, err := brokers.FindCoordinator(*clientID, *groupID)
	if err != nil {
		glog.Errorf("failed to request coordinator api for group [%s]", *groupID)
		os.Exit(5)
	}
	coordinatorBroker, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		glog.Errorf("failed to get coordinator broker[%d]", coordinatorResponse.Coordinator.NodeID)
		os.Exit(5)
	}
	glog.Infof("coordinator for group[%s]:%s", groupID, coordinatorBroker.GetAddress())

	req := healer.NewDescribeGroupsRequest(*clientID, []string{*groupID})

	responseBytes, err := coordinatorBroker.Request(req)
	if err != nil {
		glog.Errorf("failed to request describe group api:%s", err)
		os.Exit(5)
	}

	response, err := healer.NewDescribeGroupsResponse(responseBytes)

	if err != nil {
		glog.Errorf("describe groups response error:%s", err)
		os.Exit(5)
	}

	s, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal describe groups response:%s", err)
		os.Exit(5)
	}

	fmt.Println(string(s))
}
