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
	config      = flag.String("config", "", "XX=YY,AA=ZZ")
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic")
		flag.PrintDefaults()
		os.Exit(4)
	}

	configEntries := make([]*healer.AlterConfigsRequestConfigEntry, 0)

	for _, kv := range strings.Split(*config, ",") {
		if strings.Trim(kv, " ") == "" {
			continue
		}
		t := strings.SplitN(kv, "=", 2)
		if len(t) != 2 {
			glog.Errorf("invalid config : %s", kv)
			os.Exit(4)
		}
		configEntries = append(configEntries, &healer.AlterConfigsRequestConfigEntry{t[0], t[1]})
	}

	resource := &healer.AlterConfigsRequestResource{
		ResourceType:  healer.RESOURCETYPE_TOPIC,
		ResourceName:  *topic,
		ConfigEntries: configEntries,
	}

	brokers, err := healer.NewBrokers(*brokersList, *client, healer.DefaultBrokerConfig())
	if err != nil {
		glog.Errorf("could not create brokers from %s: %s", *brokersList, err)
		os.Exit(5)
	}

	alterConfigReq := healer.NewAlterConfigsRequest(*client, []*healer.AlterConfigsRequestResource{resource})
	payload, err := brokers.Request(alterConfigReq)
	if err != nil {
		glog.Errorf("call alter_config api error: %s", err)
		os.Exit(5)
	}

	resp, err := healer.NewAlterConfigsResponse(payload)
	if err != nil {
		glog.Errorf("decode alter_config response error: %s", err)
		os.Exit(5)
	}

	glog.Infof("%#v", resp)
}
