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
	timeout        = flag.Int("timeout", 60, "default 60 Second. read timeout from connection to broker")
)

func main() {
	flag.Parse()

	brokers, err := healer.NewBrokers(*brokerList, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	response, err := brokers.RequestListGroups(*clientID)

	if err != nil {
		glog.Errorf("failed to get list_groups response:%s", err)
		os.Exit(5)
	}

	s, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal metadata response:%s", err)
		os.Exit(5)
	}

	fmt.Println(string(s))
}
