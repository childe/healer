default: build

swag:
	swag init -g command/healer/cmd/api.go

.PHONY: build
version := $(shell git describe --tags --always)
buildTime := $(shell git log -1 --format='%cI')

build: swag
	CGO_ENABLED=0 go build -ldflags "-X github.com/childe/healer/command/healer/cmd.version=$(version) -X github.com/childe/healer/command/healer/cmd.buildTime=$(buildTime)" -o healer ./command/healer

.PHONY: install
install: build
	install healer $(GOPATH)/bin

.PHONY: test
test: swag
	go test -v -gcflags="all=-N -l" -count=1 ./...

.PHONY: coverage
coverage:
	go test -v -gcflags="all=-N -l" -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: docker
docker: swag
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/healer ./command/healer
	docker build -t healer:latest docker
