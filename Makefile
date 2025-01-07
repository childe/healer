.PHONY: build
version := $(shell git describe --tags --always)
buildTime := $(shell git log -1 --format='%cI')

build:
	CGO_ENABLED=0 go build -ldflags "-X github.com/childe/healer/command/healer/cmd.version=$(version) -X github.com/childe/healer/command/healer/cmd.buildTime=$(buildTime)" -o healer ./command/healer

.PHONY: install
install:
	install healer $(GOPATH)/bin

.PHONY: test
test:
	for _ in {1..5} ; do go test -v -gcflags="all=-N -l" -count=1 ./... && break ; done

.PHONY: coverage
coverage:
	go test -v -gcflags="all=-N -l" -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: docker
docker:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/healer ./command/healer
	docker build -t healer:latest docker
