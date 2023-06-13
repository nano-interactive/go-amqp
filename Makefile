RACE ?= 0

.PHONY: test
test:
ifeq ($(RACE), 1)
	@CC=gcc CXX=g++ go test ./... -race -covermode=atomic -coverprofile=coverage.txt -timeout 5m
else
	@CC=gcc CXX=g++ go test ./... -covermode=atomic -coverprofile=coverage.txt -timeout 1m
endif

.PHONY: tidy
tidy:
	@rm -f go.sum
	@go mod tidy

.PHONY: clean
clean:
	@rm -rf ./bin

.PHONY: lint
lint:
	@golangci-lint run

.PHONY: fmt
fmt:
	@gofumpt -l -w .

gosec:
	@gosec ./...

.PHONY: build
build: example-consumer example-publisher

.PHONY: example-consumer
example-consumer:
	@go build -o ./bin/example-consumer ./examples/consumer

.PHONY: example-publisher
example-publisher:
	@go build -o ./bin/example-publisher ./examples/publisher