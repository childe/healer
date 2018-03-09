package main

import (
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

	// TODO config
	config := map[string]interface{}{}
	helper, err := healer.NewHelper(*brokerList, config)
	if err != nil {
		glog.Error("create helper error:%s", err)
		os.Exit(5)
	}

	groups := helper.GetGroups()
	if groups != nil {
		for _, group := range groups {
			fmt.Println(group)
		}
	}
}
