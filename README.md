# OFM User Service

## Purpose

The User Service owns user profile data and its derived read model. It is the source of truth for profile fields and does not own passwords, auth credentials, or registration workflow state. Status: active.

## Interfaces and flow

Registration saga commands create or remove profiles. The service validates and persists the profile in PostgreSQL, updates its outbox, and publishes events for Redis/read-model and migration projections. Public profile reads use the service boundary; other services do not access its tables directly.

## Configuration

.env.example groups are DB_*, MIGRATIONS_*, REDIS_*, NATS/Kafka commands and consumers, listener ports, and observability. PostgreSQL is authoritative; Redis is derived; broker values select registration commands and projection events.

## Local development

    cp .env.example .env
    just run
    go test ./...

## Build and operations

Dockerfile builds ofm/user-service:<tag>. Helm deploys the service. Diagnose profile writes, outbox/CDC, Kafka lag, Redis freshness, and event correlation IDs.

