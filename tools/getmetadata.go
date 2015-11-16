package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gokafka"
	"log"
	"os"
	"strings"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "REQUIRED: The list of hostname and port of the server to connect to.")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	logger     = log.New(os.Stderr, "", log.LstdFlags)
)

//func getMetaData(brokerHostPort string, correlationId int32) (*gokafka.MetadataResponse, error) {
//metadataRequest := gokafka.MetadataRequest{}
//metadataRequest.RequestHeader = &gokafka.RequestHeader{
//ApiKey:        gokafka.API_MetadataRequest,
//ApiVersion:    0,
//CorrelationId: correlationId,
//ClientId:      "gokafka" + string(pid),
//}
//metadataRequest.Topic = []string{*topic}

//conn, err := net.DialTimeout("tcp", brokerHostPort, time.Second*5)
//if err != nil {
//logger.Println(err)
//return nil, err
//}
//payload := metadataRequest.Encode()
//conn.Write(payload)

//responsePlayload := make([]byte, 10240)
//length, _ := conn.Read(responsePlayload)

//metadataResponse := &gokafka.MetadataResponse{}
//err = metadataResponse.Decode(responsePlayload[:length])
//if err != nil {
//return nil, err
//}
//return metadataResponse, nil
//}

func main() {
	flag.Parse()

	for _, broker := range strings.Split(*brokerList, ",") {
		//brokerHostPort := strings.Split(broker, ":")

		//var brokerHost, brokerPort string

		//if len(brokerHostPort) == 1 {
		//brokerHost, brokerPort = brokerHostPort[0], "9092"
		//} else if len(brokerHostPort) == 2 {
		//brokerHost = brokerHostPort[0]
		//brokerPort = brokerHostPort[1]
		//} else {
		//logger.Fatalln(broker + " is not a valid broker address")
		//os.Exit(1)
		//}

		pid := os.Getpid()
		//fmt.Println("pid:", pid)
		metadataResponse, err := gokafka.GetMetaData(broker, *topic, int32(pid), "gokafkashell")
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
