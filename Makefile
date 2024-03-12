.PHONY: all test docker

build:
	go build -o healer ./command

install:
	install healer $(GOPATH)/bin

test:
	go test -v -args -brokers 127.0.0.1:9092 -broker 127.0.0.1:9092 -logtostderr

docker:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/healer ./command
	docker build -t healer:latest docker
