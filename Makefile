VERSION := 0.4.0

build:
	go build -ldflags "-X tm1cli/cmd.Version=$(VERSION)" -o tm1cli

install: build
	cp tm1cli /usr/local/bin/

test:
	go test ./...

test-cover:
	go test -cover ./...

clean:
	rm -f tm1cli

.PHONY: build install test test-cover clean
