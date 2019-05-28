package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/childe/healer"
)

var (
	brokerList        = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	clientID          = flag.String("client.id", "healer.createtopic", "The ID of this client.")
	topic             = flag.String("topic", "", "topic name")
	partitions        = flag.Int("partitions", 0, "The number of partitions for the topic (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected)")
	replicationFactor = flag.Int("replication-factor", 0, "The replication factor for each partition in the topic being created.")
	replicaAssignment = flag.String("replica-assignment", "", "pid:[replicas],pid:[replicas]...")
	timeout           = flag.Int("timeout", 0, "The time in ms to wait for a topic to be completely created on the controller node. Values <= 0 will trigger topic creation and return immediately")
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic")
		os.Exit(4)
	}

	if *partitions != 0 && *replicaAssignment != "" {
		fmt.Println(`Option "[partitions]" can't be used with option"[partitions]"`)
		os.Exit(4)
	}

	if *replicationFactor != 0 && *replicaAssignment != "" {
		fmt.Println(`Option "[replica-assignment]" can't be used with option"[replication-factor]"`)
		os.Exit(4)
	}

	brokers, err := healer.NewBrokers(*brokerList)

	controller, err := brokers.GetBroker(brokers.Controller())
	if err != nil {
		fmt.Printf("create broker error: %s", err)
		os.Exit(5)
	}

	r := healer.NewCreateTopicsRequest(*clientID, int32(*timeout))

	replicas := make(map[int32][]int32)
	if *replicaAssignment != "" {
		for pid, nodes := range strings.Split(*replicaAssignment, ",") {
			replicas[int32(pid)] = make([]int32, 0)
			for _, node := range strings.Split(nodes, ":") {
				nodeid, err := strconv.Atoi(node)
				if err != nil {
					fmt.Println(err)
					os.Exit(4)
				}
				replicas[int32(pid)] = append(replicas[int32(pid)], int32(nodeid))
			}
		}

		*partitions = len(replicas)
		for pid, nodes := range replicas {
			if int(pid+1) != *partitions {
				if len(nodes) != len(nodes) {
					fmt.Printf("Partition %d has different replication factor\n", pid)
					os.Exit(4)
				}
				*replicationFactor = len(nodes)
			}
		}
	}

	r.AddTopic(*topic, int32(*partitions), int16(*replicationFactor))

	for pid, nodes := range replicas {
		r.AddReplicaAssignment(*topic, pid, nodes)
	}

	payload, err := controller.Request(r)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}

	_, err = healer.NewCreateTopicsResponse(payload)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}
}
