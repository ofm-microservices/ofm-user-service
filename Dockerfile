FROM golang:1.25.5 AS builder

WORKDIR /src

COPY ofm-common /src/ofm-common
COPY ofm-user-service /src/ofm-user-service

WORKDIR /src/ofm-user-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/user-service ./cmd/user-service

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app \
    && chown app:app /app \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/user-service /app/user-service
COPY --from=builder /src/ofm-user-service/migration /app/migration

RUN chown -R app:app /app

USER app:app

CMD ["/app/user-service"]
