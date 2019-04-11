package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	client      = flag.String("client.ID", "healer", "client id")
	brokersList = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	config      = flag.String("config", "", "XX,YY")
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic")
		flag.PrintDefaults()
		os.Exit(4)
	}

	resource := &healer.DescribeConfigsRequestResource{
		ResourceType: healer.RESOURCETYPE_TOPIC,
		ResourceName: *topic,
		ConfigNames:  strings.Split(*config, ","),
	}

	brokers, err := healer.NewBrokers(*brokersList, *client, healer.DefaultBrokerConfig())
	if err != nil {
		glog.Errorf("could not create brokers from %s: %s", *brokersList, err)
		os.Exit(5)
	}

	describeConfigReq := healer.NewDescribeConfigsRequest(*client, []*healer.DescribeConfigsRequestResource{resource})
	payload, err := brokers.Request(describeConfigReq)
	if err != nil {
		glog.Errorf("call describe_config api error: %s", err)
		os.Exit(5)
	}

	resp, err := healer.NewDescribeConfigsResponse(payload)
	if err != nil {
		glog.Errorf("decode describe_config response error: %s", err)
		os.Exit(5)
	}

	glog.Infof("%#v", resp)
}
