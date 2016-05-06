package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/childe/gokafka"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	clientID   = flag.String("clientID", "gokafka", "The ID of this client.")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	logger     = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	for _, broker := range strings.Split(*brokerList, ",") {

		pid := os.Getpid()
		metadataResponse, err := gokafka.GetMetaData(broker, *topic, int32(pid), *clientID)
		if err != nil {
			logger.Println(err)
			continue
		}

		s, err := json.MarshalIndent(metadataResponse, "", "  ")
		if err != nil {
			logger.Println(err)
			continue
		}
		fmt.Println(string(s))
		break
	}
}
