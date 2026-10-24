.PHONY: test test-unit cover vet

PKGS := $(shell go list ./... | grep -v '/examples/' | grep -v '/internal/pgtest' | paste -sd, -)

test:
	go test -race ./...

test-unit:
	go test -race -short ./...

cover:
	go test -race -coverpkg=$(PKGS) -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

vet:
	go vet ./...
