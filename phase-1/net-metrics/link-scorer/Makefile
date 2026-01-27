.PHONY: build test clean docker-build

build:
	go build -o link-scorer main.go

test:
	go test -v ./...

clean:
	rm -f link-scorer

docker-build:
	docker build -t link-scorer:latest .

run-local:
	PROM_URL=http://localhost:9090 ./link-scorer
