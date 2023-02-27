## Group Consumer

```
package main

import (
	"flag"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

func main() {
	flag.Parse()

	configMap := make(map[string]interface{})
	configMap["bootstrap.servers"] = "127.0.0.1:9092,127.0.0.1:9093"
	configMap["group.id"] = "mygroup"

	config, err := healer.GetConsumerConfig(configMap)
	if err != nil {
		glog.Errorf("could not create consumer config: %s", err)
	}

	c, err := healer.NewGroupConsumer("TOPICNAME", config)
	if err != nil {
		glog.Errorf("could not create GroupConsumer: %s", err)
	}
	defer c.Close()

	messages, err := c.Consume(nil)
	if err != nil {
		glog.Fatalf("failed to consume: %s", err)
	}

	for {
		message := <-messages
		fmt.Printf("%s:%d:%d:%s\n", message.TopicName, message.PartitionID, message.Message.Offset, message.Message.Value)
	}
}
```

## Producer

```
package main

import (
	"flag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

func main() {
	flag.Parse()

	configMap := make(map[string]interface{})
	configMap["bootstrap.servers"] = "127.0.0.1:9092,127.0.0.1:9093"
	config, err := healer.GetProducerConfig(configMap)

	if err != nil {
		glog.Errorf("coult not create producer config: %s", err)
		return
	}

	producer := healer.NewProducer("TOPICNAME", config)
	if producer == nil {
		glog.Error("could not create producer")
	}
	defer producer.Close()

	key := []byte("")
	msg := []byte("")
	producer.AddMessage(key, msg)
	// producer.AddMessage(nil, msg)
}

```

## Console Consumer

## Simple Consumer

## Group Consumer (assign certain parititons, do not need join group)
