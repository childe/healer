all:
	go build -o tools/bin/getmetadata tools/getmetadata/getmetadata.go
	go build -o tools/bin/getoffsets tools/getoffset/getoffset.go
	go build -o tools/bin/simple-consumer tools/simple-consumer/simple-consumer.go
	go build -o tools/bin/console-consumer tools/console-consumer/console-consumer.go
	go build -o tools/bin/group-consumer tools/group-consumer/group-consumer.go
	go build -o tools/bin/simple-producer tools/simple-producer/simple-producer.go
	go build -o tools/bin/console-producer tools/console-producer/console-producer.go
	go build -o tools/bin/check-consumer-offset tools/check-consumer-offset/check-consumer-offset.go
	go build -o tools/bin/list-groups tools/list-groups/list-groups.go
	go build -o tools/bin/describe-groups tools/describe-groups/describe-groups.go
	go build -o tools/bin/reset-offset tools/reset-offset/reset-offset.go
	go build -o tools/bin/transplant-offset tools/transplant-offset/transplant-offset.go
	go build -o tools/bin/get-pending tools/get-pending/get-pending.go

test:
	go test -v -args -brokers 127.0.0.1:9092 -broker 127.0.0.1:9092 -logtostderr --broker 127.0.0.1:9092
