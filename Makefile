all:
	go build -o tools/bin/getmetadata tools/getmetadata/getmetadata2.go
	go build -o tools/bin/getoffsets tools/getoffset/getoffset2.go
	go build -o tools/bin/simpleconsumer tools/simple_consumer/simple_consumer.go
	go build -o tools/bin/console-consumer tools/console-consumer/console-consumer.go
	go build -o tools/bin/list-groups tools/list_groups/list_groups.go

test:
	go test -v -args -brokers 127.0.0.1:9092 -broker 127.0.0.1:9092
