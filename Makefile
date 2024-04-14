.PHONY: all test docker

build:
	go build -o healer ./command

install:
	install healer $(GOPATH)/bin

test:
	go test -v -gcflags="all=-N -l" .

coverage:
	go test -v -gcflags="all=-N -l" -coverprofile=coverage.out .
	go tool cover -html=coverage.out

docker:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/healer ./command
	docker build -t healer:latest docker
