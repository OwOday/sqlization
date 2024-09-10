# Makefile
all: build

#setup:
	

build:
	go build ./src/

test: 
	go test -timeout 24h -v ./src/