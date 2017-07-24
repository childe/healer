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

	broker, err := healer.GetBroker(*brokerList)
	if err != nil {
		logger.Println(err)
		os.Exit(255)
	}

	metadataResponse, err := broker.RequestMetaData(*topic)

	if err != nil {
		logger.Println(err)
		os.Exit(255)
	}

	s, err := json.MarshalIndent(metadataResponse, "", "  ")
	if err != nil {
		logger.Println(err)
		os.Exit(255)
	}

	fmt.Println(string(s))
}
