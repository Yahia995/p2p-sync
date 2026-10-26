BIN := bin/p2psync

.PHONY: build build-old test race vet clean

build:
	go build -o $(BIN) ./cmd/p2psync

build-old:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BIN)-linux-amd64 ./cmd/p2psync

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin
