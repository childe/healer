package main

import (
	"bufio"
	"flag"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic   = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	brokers = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	config  = flag.String("config", "", "XX=YY,AA=ZZ")

	producerConfig = map[string]interface{}{"bootstrap.servers": brokers}

	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile = flag.String("memprofile", "", "write mem profile to `file`")
)

func _main() int {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			glog.Fatalf("could not create CPU profile: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			glog.Fatalf("could not start CPU profile: %s", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			glog.Fatalf("could not create memory profile: %s", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			glog.Fatalf("could not write memory profile: %s", err)
		}
		f.Close()
	}

	if *topic == "" {
		flag.PrintDefaults()
		return 4
	}

	for _, kv := range strings.Split(*config, ",") {
		if strings.Trim(kv, " ") == "" {
			continue
		}
		t := strings.SplitN(kv, "=", 2)
		if len(t) != 2 {
			glog.Errorf("invalid config : %s", kv)
			return 4
		}
		producerConfig[t[0]] = t[1]
	}
	pConfig, err := healer.GetProducerConfig(producerConfig)
	if err != nil {
		glog.Errorf("config error : %s", err)
		return 4
	}
	producer := healer.NewProducer(*topic, pConfig)

	if producer == nil {
		glog.Error("could not create producer")
		return 5
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Bytes()
		producer.AddMessage(nil, line)
	}
	producer.Close()

	if err := scanner.Err(); err != nil {
		glog.Errorf("%s", err)
		return 5
	}

	return 0
}

func main() {
	flag.Parse()
	os.Exit(_main())
}
