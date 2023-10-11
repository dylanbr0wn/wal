BINARY_NAME=main

build: clean go
clean: 
	rm -rf ./bin
go:
	go build -o bin/$(BINARY_NAME) -v
test:
	go test -v ./...