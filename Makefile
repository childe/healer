.PHONY: build
build:
	go build -o healer ./command/healer

.PHONY: install
install:
	install healer $(GOPATH)/bin

.PHONY: test
test:
	go test -v -gcflags="all=-N -l" -count=1 ./...

.PHONY: coverage
coverage:
	go test -v -gcflags="all=-N -l" -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: docker
docker:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o docker/healer ./command/healer
	docker build -t healer:latest docker
