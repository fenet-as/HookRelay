.PHONY: up down migrate migrate-down sqlc test test-race vet build run clean

DB_URL ?= postgres://postgres@localhost:5432/hookrelay?sslmode=disable

up:
	docker compose up -d

down:
	docker compose down

migrate:
	goose -dir migrations postgres "$(DB_URL)" up

migrate-down:
	goose -dir migrations postgres "$(DB_URL)" down

sqlc:
	sqlc generate

test:
	go test -p 1 -v ./...

test-race:
	go test -p 1 -race -v ./...

vet:
	go vet ./...

build:
	go build -o bin/hookrelay ./cmd/hookrelay

run:
	go run ./cmd/hookrelay

clean:
	rm -rf bin/

