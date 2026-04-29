set shell := ["bash", "-c"]

run:
    set -a && source .env && set +a && go run cmd/user-service/main.go
