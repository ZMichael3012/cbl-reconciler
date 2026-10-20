.PHONY: build test test-integration test-e2e vet lint fmt vuln up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration ./... -count=1

test-e2e:
	go test -tags e2e ./test/e2e/ -count=1

vet:
	go vet ./...

# lint mounts the whole services/ directory: the replace directive to ../cbl-kit
# resolves inside the container.
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src -e GOPRIVATE=github.com/cbl/* -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=url.https://github.com/ZMichael3012/.insteadOf -e GIT_CONFIG_VALUE_0=https://github.com/cbl/ golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

vuln:
	govulncheck ./...

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v