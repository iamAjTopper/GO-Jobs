# GO-Jobs

> A distributed job processing system built with **Go, Redis Streams,
> PostgreSQL, Docker, Kubernetes, Prometheus and Grafana**.

![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Redis](https://img.shields.io/badge/Redis-Streams-DC382D?logo=redis)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Deployed-326CE5?logo=kubernetes)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-F46800?logo=grafana)

------------------------------------------------------------------------

## Overview

GO-Jobs is a reliable asynchronous job processing system that
demonstrates production-inspired backend patterns.

Instead of processing long-running work inside the API, requests are
persisted in PostgreSQL, published through the Outbox Pattern into Redis
Streams, and consumed by concurrent worker pools.

The project focuses on reliability, observability, concurrency and
cloud-native deployment.

------------------------------------------------------------------------

## Features

-   REST API using Gin
-   PostgreSQL persistence with GORM
-   Redis Streams + Consumer Groups
-   Concurrent worker pool
-   Retry with exponential backoff
-   Permanent failure handling
-   Outbox Pattern
-   Crash recovery using XAUTOCLAIM
-   Graceful shutdown
-   Idempotent processing
-   Prometheus metrics
-   Grafana dashboard
-   Dockerized services
-   Kubernetes deployment

------------------------------------------------------------------------

## Architecture

``` mermaid
flowchart TD

A[Client]
-->B[Go API]

B-->C[(PostgreSQL)]

C-->D[Outbox Table]

D-->E[Outbox Processor]

E-->F[(Redis Streams)]

F-->G[Worker Pool]

G-->H[Business Logic]

G-->I[Prometheus Metrics]

I-->J[Prometheus]

J-->K[Grafana Dashboard]
```

------------------------------------------------------------------------

## Tech Stack

  Technology      Purpose
  --------------- --------------------
  Go              Backend
  Gin             REST API
  PostgreSQL      Persistent Storage
  GORM            ORM
  Redis Streams   Message Queue
  Docker          Containerization
  Kubernetes      Orchestration
  Prometheus      Metrics
  Grafana         Visualization

------------------------------------------------------------------------

## Reliability Features

### Outbox Pattern

Jobs are first stored in PostgreSQL.

Only after the database transaction succeeds are they published to Redis
Streams.

This prevents lost messages.

### Redis Streams Consumer Groups

Multiple workers consume jobs safely.

Only one worker processes each job.

### Retry Mechanism

Failed jobs retry with exponential backoff.

After the retry limit is reached they are marked as permanently failed.

### Crash Recovery

XAUTOCLAIM reclaims abandoned jobs from crashed workers.

### Graceful Shutdown

Workers finish active jobs before exiting.

### Idempotency

Already processed jobs are skipped safely.

------------------------------------------------------------------------

## Monitoring

Metrics exposed:

-   jobs_processed_total
-   jobs_failed_total
-   go_goroutines
-   go_memstats_alloc_bytes
-   process_cpu_seconds_total

Grafana Dashboard Panels:

-   Jobs Processed
-   Jobs Failed
-   Jobs Processed / Minute
-   Worker Status
-   Memory Usage
-   Goroutines
-   CPU Usage
-   Worker Uptime

------------------------------------------------------------------------

## API

### Create Job

``` http
POST /jobs
```

Example:

``` json
{
  "type":"email",
  "priority":"free"
}
```

Response

``` json
{
  "id":1,
  "status":"pending"
}
```

------------------------------------------------------------------------

## Project Structure

``` text
GO-Jobs
├── api/
├── worker/
├── workerpkg/
├── shared/
├── k8s/
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.worker
└── prometheus.yml
```

------------------------------------------------------------------------

## Running Locally

### Docker Compose

``` bash
docker compose up --build
```

### Kubernetes

``` bash
kubectl apply -f k8s/
```

------------------------------------------------------------------------

## Future Improvements

-   KEDA Autoscaling
-   Authentication
-   Job Scheduling
-   Distributed Tracing
-   Rate Limiting
-   Dead Letter Queue Dashboard

------------------------------------------------------------------------

## Interview Highlights

This project demonstrates:

-   Distributed system fundamentals
-   Concurrent worker pools
-   Event-driven architecture
-   Redis Streams Consumer Groups
-   Outbox Pattern
-   Crash recovery
-   Kubernetes deployment
-   Observability with Prometheus & Grafana
-   Graceful shutdown
-   Production-inspired backend design

------------------------------------------------------------------------

## Screenshots

Add screenshots of:

-   Grafana Dashboard
-   Prometheus Targets
-   Kubernetes Pods
-   Architecture Diagram

------------------------------------------------------------------------

## License

MIT
