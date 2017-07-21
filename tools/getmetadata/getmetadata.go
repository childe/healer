package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/childe/healer"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	clientID   = flag.String("clientID", "healer", "The ID of this client.")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	logger     = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	correlationID := os.Getpid()
	metadataResponse, err := healer.GetMetaData(*brokerList, *topic, int32(correlationID), *clientID)
	if err != nil {
		logger.Println(err)
	}

	s, err := json.MarshalIndent(metadataResponse, "", "  ")
	if err != nil {
		logger.Println(err)
	}

	fmt.Println(string(s))
}
