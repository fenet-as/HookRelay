# Phase 9: Telemetry, Observability & Live Visualization

## Overview

In **Phase 9**, we transformed HookRelay from a functioning webhook delivery engine into a fully observable, production-grade distributed system.

When operating an outbound webhook service at scale, you cannot rely solely on log files:
- How many deliveries are currently pending in PostgreSQL?
- Are workers keeping up with incoming traffic, or is queue lag accumulating?
- What percentage of deliveries fail due to receiver timeouts versus HTTP 5xx errors?
- What are the p50, p90, and p99 delivery latencies across external endpoints?
- Has the crash recovery reaper salvaged abandoned deliveries, or has an endpoint tripped its consecutive failure circuit breaker?

Phase 9 integrates a complete **Prometheus & Grafana** observability stack:
1. An `internal/telemetry` Go package capturing real-time counters, histograms, and gauges.
2. A background `Collector` periodically querying PostgreSQL queue depth and worker lag.
3. Native `GET /metrics` exposure conforming to Prometheus / OpenMetrics standards.
4. Pre-configured Docker Compose services for Prometheus and Grafana.
5. A pre-provisioned Grafana dashboard JSON visualizing queue depth, worker throughput, latency percentiles, and circuit breaker events out of the box.

---

## Files Introduced or Modified

```
02-HookRelay/
├── internal/
│   ├── telemetry/
│   │   ├── metrics.go                           (New: Prometheus registry, metrics, and background collector)
│   │   └── metrics_test.go                      (New: unit tests for HTTP scrape endpoint and collector)
│   ├── api/
│   │   ├── server.go                            (Modified: wired optional telemetry & GET /metrics route)
│   │   ├── handlers.go                          (Modified: instrumented event ingest & delivery counts)
│   │   └── api_test.go                          (Modified: added TestMetricsEndpoint)
│   ├── delivery/
│   │   ├── pool.go                              (Modified: instrumented attempts, status codes, latency, circuit breaker)
│   │   └── reaper.go                            (Modified: instrumented recovered stuck deliveries)
│   └── store/
│       ├── deliveries.sql.go                    (Generated: CountPendingDeliveries, CountDueDeliveries)
│       └── endpoints.sql.go                     (Generated: CountActiveEndpoints)
├── queries/
│   ├── deliveries.sql                           (Modified: added count queries for pending & due deliveries)
│   └── endpoints.sql                            (Modified: added count query for active endpoints)
├── cmd/
│   └── hookrelay/
│       └── main.go                              (Modified: initialized telemetry, collector, and wired metrics)
├── deploy/
│   ├── prometheus/
│   │   └── prometheus.yml                       (New: Prometheus scraper configuration)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   │   └── datasource.yml               (New: auto-provisions Prometheus datasource)
│       │   └── dashboards/
│       │       └── dashboards.yml               (New: auto-provisions dashboard provider)
│       └── dashboards/
│           └── hookrelay.json                   (New: production Grafana dashboard definition)
├── docker-compose.yml                           (Modified: added prometheus & grafana services)
├── README.md                                    (Modified: documented metrics dictionary & visualization stack)
└── learning/
    └── phase-09-telemetry-and-observability.md   (New: this learning document)
```

---

### 1. `internal/telemetry/metrics.go`

- **Purpose:** Centralizes all Prometheus metric definitions, registry management, and background database gauge collection.
- **What it does:**
  - Instantiates an isolated `prometheus.Registry` (avoiding the global default registry to prevent collisions in tests).
  - Registers standard Go runtime collectors (`collectors.NewGoCollector()`, `collectors.NewProcessCollector()`).
  - Defines HookRelay domain metrics:
    - `EventsIngested`: CounterVec labeled by `type` and `duplicate`.
    - `DeliveriesCreated`: CounterVec labeled by `event_type`.
    - `DeliveryAttempts`: CounterVec labeled by `status` (`success`, `retry`, `dead`) and `status_code`.
    - `DeliveryDuration`: HistogramVec labeled by `status` with 11 latency buckets ranging from 5ms to 10s.
    - `ReaperRecovered`: Counter tracking un-stuck abandoned deliveries.
    - `EndpointsDeactivated`: Counter tracking auto-disabled endpoints.
    - `QueuePending`: Gauge tracking total pending deliveries in PostgreSQL.
    - `QueueDue`: Gauge tracking pending deliveries due right now (`next_run_at <= now()`).
    - `ActiveEndpoints`: Gauge tracking current active subscriber endpoints.
  - Implements `Collector`: A background worker that runs fast count queries against PostgreSQL every 5 seconds to update queue depth gauges without overwhelming the database.
- **Why this approach:**
  Isolating metrics in their own package prevents circular dependencies between `api`, `delivery`, and `fanout`. Creating an explicit `*prometheus.Registry` rather than using `prometheus.DefaultRegisterer` ensures that unit tests can be instantiated repeatedly without panicking on duplicate metric registration.

```go
type Metrics struct {
	Registry *prometheus.Registry

	EventsIngested       *prometheus.CounterVec
	DeliveriesCreated    *prometheus.CounterVec
	DeliveryAttempts     *prometheus.CounterVec
	DeliveryDuration     *prometheus.HistogramVec
	ReaperRecovered      prometheus.Counter
	EndpointsDeactivated prometheus.Counter
	QueuePending         prometheus.Gauge
	QueueDue             prometheus.Gauge
	ActiveEndpoints      prometheus.Gauge
}
```

---

### 2. `queries/deliveries.sql` & `queries/endpoints.sql`

- **Purpose:** Fast, indexed SQL count queries for database gauge sampling.
- **What it does:**
  - `CountPendingDeliveries`: `SELECT COUNT(*) FROM deliveries WHERE status = 'pending';` (utilizes partial index `deliveries_due_idx`).
  - `CountDueDeliveries`: `SELECT COUNT(*) FROM deliveries WHERE status = 'pending' AND next_run_at <= now() AND locked_at IS NULL;` (measures worker lag).
  - `CountActiveEndpoints`: `SELECT COUNT(*) FROM endpoints WHERE active = true;`.

---

### 3. `internal/api/server.go` & `internal/api/handlers.go`

- **Purpose:** Exposes the `/metrics` endpoint and instruments HTTP event ingestion.
- **What it does:**
  - `server.go` registers `s.router.GET("/metrics", gin.WrapH(s.metrics.HTTPHandler()))`.
  - In `PublishEventHandler`:
    - Increments `EventsIngested` with `result.Event.Type` and `is_duplicate`.
    - Adds to `DeliveriesCreated` with the count of fanned-out deliveries.

---

### 4. `internal/delivery/pool.go` & `internal/delivery/reaper.go`

- **Purpose:** Instruments delivery attempts, outbound latency, circuit-breaker activations, and crash recoveries.
- **What it does:**
  - In `processDelivery`:
    - On success: increments `DeliveryAttempts` (`status="success", status_code="200"`) and records request duration in `DeliveryDuration`.
    - On retryable failure: increments `DeliveryAttempts` (`status="retry"`, status code) and records duration.
    - On permanent failure: increments `DeliveryAttempts` (`status="dead"`, status code) and records duration.
  - In `checkEndpointFailureThreshold`:
    - Increments `EndpointsDeactivated` whenever an endpoint trips its 20 consecutive failure threshold.
  - In `reaper.go`:
    - Increments `ReaperRecovered` by `count` whenever stale deliveries are unlocked.

---

### 5. `deploy/` & `docker-compose.yml`

- **Purpose:** Automated provisioning of Prometheus and Grafana.
- **What it does:**
  - `deploy/prometheus/prometheus.yml`: Configures Prometheus to scrape `http://host.docker.internal:8080/metrics` or `http://hookrelay:8080/metrics` every 5 seconds.
  - `deploy/grafana/provisioning/`: Automatically creates the Prometheus datasource in Grafana on container start.
  - `deploy/grafana/dashboards/hookrelay.json`: Provisions a pre-built visual dashboard with 8 real-time panels.
  - `docker-compose.yml`: Adds `prometheus` on `:9090` and `grafana` on `:3000`.

---

## Backend Concepts Explained

### 1. The Four Golden Signals of Observability

Google's Site Reliability Engineering (SRE) handbook outlines the four fundamental signals every backend service must monitor:

1. **Latency:** The time it takes to service a request.
   - *In HookRelay:* Measured via `hookrelay_deliveries_duration_seconds` (histogram). Tracks outbound HTTP roundtrip time.
2. **Traffic:** A measure of how much demand is being placed on your system.
   - *In HookRelay:* Measured via `hookrelay_events_ingested_total` (events/sec) and `hookrelay_deliveries_created_total` (fanout deliveries/sec).
3. **Errors:** The rate of requests that fail.
   - *In HookRelay:* Measured via `hookrelay_deliveries_attempts_total` segmented by `status` (`success`, `retry`, `dead`) and `status_code` (`500`, `503`, `404`, `0` for network timeouts).
4. **Saturation:** How "full" your service is; a measure of system capacity and backlog.
   - *In HookRelay:* Measured via `hookrelay_queue_pending_deliveries` (backlog) and `hookrelay_queue_due_deliveries` (unclaimed tasks due right now). If due deliveries rise while workers are running, the system is saturated and needs more worker goroutines.

### 2. Prometheus Metric Types: Counter vs Gauge vs Histogram

Prometheus supports four core metric primitives:

| Metric Type | Characteristics | Use Case in HookRelay |
|---|---|---|
| **Counter** | Cumulative metric that can only increase or be reset to zero on restart. Use `rate()` to calculate per-second velocity. | `events_ingested_total`, `deliveries_created_total`, `deliveries_attempts_total`. |
| **Gauge** | A metric that represents a single numerical value that can arbitrarily go up or down. | `queue_pending_deliveries`, `queue_due_deliveries`, `endpoints_active_count`. |
| **Histogram** | Samples observations (usually durations or sizes) and counts them in configurable bucket intervals. | `deliveries_duration_seconds` (allows calculating p50, p90, p99 percentiles). |
| **Summary** | Calculates percentiles directly on the client (less flexible than histograms across distributed instances). | Not recommended for multi-instance services. |

### 3. Why Use Histograms for Latency (Percentiles vs Averages)

Averages are dangerous in backend engineering:
- If 99 deliveries take 10ms, but 1 delivery times out after 10,000ms, the *average* latency is:
  $$\frac{99 \times 10 + 10000}{100} = 109.9\text{ ms}$$
- The average makes it look like the system is healthy, hiding the fact that 1% of your subscribers are experiencing severe 10-second timeouts!

By using a Prometheus **Histogram**, we record observations into buckets (`0.05s`, `0.1s`, `0.5s`, `1s`, `5s`, `10s`). In Grafana, we calculate the 99th percentile ($p99$) using PromQL:

$$\text{histogram\_quantile}(0.99, \text{sum}(\text{rate}(\text{hookrelay\_deliveries\_duration\_seconds\_bucket}[1\text{m}]))\text{ by }(le))$$

This tells you the exact latency that 99% of your requests were faster than, immediately exposing outliers.

### 4. Pull-Based Metrics Scraping

Unlike push-based systems (like Datadog or StatsD) where every application worker sends UDP packets to an external daemon, Prometheus uses a **pull-based** model:
1. HookRelay holds metrics in atomic counters in memory.
2. HookRelay exposes `GET /metrics` formatted in plain text (OpenMetrics format).
3. Prometheus scrapes `http://localhost:8080/metrics` on a configured interval (e.g. every 5 seconds).
4. If Prometheus crashes or network partitions occur, HookRelay is completely unaffected (no blocked network calls, no dropped UDP buffers).

---

## Architectural Interaction & Flow

```mermaid
flowchart TD
    subgraph Upstream ["Upstream Clients"]
        Client["Client / Microservice"]
    end

    subgraph HookRelay ["HookRelay Go Service (:8080)"]
        Router["Gin Router (POST /events)"]
        WorkerPool["Worker Pool (Goroutines)"]
        Reaper["Crash Reaper"]
        
        subgraph Telemetry ["internal/telemetry"]
            Metrics["Prometheus Metrics Registry"]
            Collector["Background Queue Collector<br/>(Every 5s)"]
        end

        ScrapeEndpoint["GET /metrics<br/>(OpenMetrics Format)"]
    end

    subgraph Storage ["PostgreSQL"]
        DB[("deliveries & endpoints")]
    end

    subgraph Monitoring ["Observability Stack (Docker Compose)"]
        Prometheus["Prometheus Server (:9090)<br/>Scrapes /metrics every 5s"]
        Grafana["Grafana UI (:3000)<br/>Auto-Provisioned Dashboard"]
    end

    Client -->|Publish Event| Router
    Router -->|EventsIngested++<br/>DeliveriesCreated++| Metrics
    WorkerPool -->|DeliveryAttempts++<br/>Duration Observe| Metrics
    Reaper -->|ReaperRecovered++| Metrics
    Collector -.->|Count Pending & Due| DB
    Collector -->|QueuePending.Set()<br/>QueueDue.Set()| Metrics
    Metrics --> ScrapeEndpoint

    Prometheus -->|HTTP GET /metrics| ScrapeEndpoint
    Grafana -->|PromQL Queries| Prometheus
```

---

## Verification & Test Results

All telemetry features, metrics collection, HTTP scraping, and end-to-end delivery flows have been verified:

1. **Telemetry Unit Tests (`internal/telemetry/metrics_test.go`):**
   - Verified that `telemetry.NewMetrics()` registers all counters, gauges, and histograms.
   - Verified that scraping `m.HTTPHandler()` returns valid Prometheus plain-text metrics.
   - Verified that `Collector.Start()` shuts down cleanly when its context is canceled.
2. **API Scrape Route (`internal/api/api_test.go`):**
   - Verified that `GET /metrics` returns HTTP `200 OK` and exposes `hookrelay_events_ingested_total` and queue gauges after publishing an event.
3. **Full Race Detection (`go test -race -count=1 -p 1 ./...`):**
   - Verified that all 11 packages pass with zero data races.

```bash
$ go test -race -count=1 -p 1 ./...
ok  	github.com/fenet-as/hookrelay/cmd/testreceiver	1.079s
ok  	github.com/fenet-as/hookrelay/internal/api	2.114s
ok  	github.com/fenet-as/hookrelay/internal/crypto	1.033s
ok  	github.com/fenet-as/hookrelay/internal/delivery	3.048s
ok  	github.com/fenet-as/hookrelay/internal/fanout	1.343s
ok  	github.com/fenet-as/hookrelay/internal/id	1.040s
ok  	github.com/fenet-as/hookrelay/internal/netguard	1.082s
ok  	github.com/fenet-as/hookrelay/internal/signing	1.048s
ok  	github.com/fenet-as/hookrelay/internal/store	1.354s
ok  	github.com/fenet-as/hookrelay/internal/telemetry	1.099s
ok  	github.com/fenet-as/hookrelay/test		3.816s

go vet ./... -> 0 warnings/errors
```

---

## Learning Checklist

### Key Takeaways
- **Observability is Not an Afterthought:** Instrumenting counters, histograms, and gauges at critical system boundaries provides immediate insight into capacity bottlenecks, queue lag, and external subscriber health.
- **Isolate Your Metrics Registry:** Avoid `prometheus.DefaultRegisterer` in library code or modular applications; an explicit `*prometheus.Registry` enables deterministic unit testing without panic collisions.
- **Gauges Must Be Sampled Responsibly:** Do not run heavy `COUNT(*)` queries on every Prometheus scrape request. Use a background goroutine to sample database queue depths periodically (e.g. every 5 seconds) and update in-memory gauges.
- **Histograms Expose the Tail:** Averages hide latency spikes. Use histogram quantiles (`p90`, `p99`) to monitor true subscriber experience.
- **Provisioning as Code:** Never configure Grafana dashboards manually through the UI in production; provision datasources and dashboards as code via YAML and JSON so the observability environment boots up automatically.

### Important Terminology
- **PromQL:** Prometheus Query Language, used to aggregate and compute rates, sums, and quantiles over time-series data.
- **Scrape Interval:** The frequency at which Prometheus pulls `/metrics` from monitored targets (e.g., 5 seconds).
- **Golden Signals:** Latency, Traffic, Errors, and Saturation.
- **Histogram Buckets:** Upper bounds (`le`) into which observations are sorted to enable percentile calculations without storing raw timestamps.
- **Dashboard Provisioning:** Automatically loading datasources and dashboard definitions into Grafana on startup from config files rather than manual UI clicks.

### Questions You Should Be Able to Answer
1. Why is a pull-based metrics model more resilient than a push-based model during downstream monitoring outages?
2. What is the difference between a `Counter` and a `Gauge` in Prometheus, and which one would you use to measure queue depth?
3. Why are histograms preferred over averages when monitoring outbound webhook latency?
4. How does the background `Collector` update PostgreSQL queue metrics without impacting the performance of the `/metrics` HTTP handler?
5. What PromQL expression calculates the per-second rate of successful webhook delivery attempts over a 1-minute window?
