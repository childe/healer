package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokerList     = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	connectTimeout = flag.Int("connect-timeout", 10, "default 10 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 30, "default 30 Second. read timeout from connection to broker")
	groups         = flag.String("groups", "", "space splited group list")
)

func main() {
	flag.Parse()

	if *groups == "" {
		glog.Error("need groups")
		flag.PrintDefaults()
		os.Exit(4)
	}

	brokers, err := healer.NewBrokers(*brokerList, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Errorf("create brokers error:%s", err)
		os.Exit(5)
	}

	response, err := brokers.RequestDescribeGroups(*clientID, strings.Split(*groups, " "))

	if err != nil {
		glog.Errorf("failed to get describe groups response:%s", err)
		os.Exit(5)
	}

	s, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		glog.Errorf("failed to marshal describe groups response:%s", err)
		os.Exit(5)
	}

	fmt.Println(string(s))
}
