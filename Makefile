all:
	go build -o tools/bin/getmetadata tools/getmetadata/getmetadata2.go
	go build -o tools/bin/getoffsets tools/getoffset/getoffset2.go

test:
	go test -v -args -brokers 127.0.0.1:9092
