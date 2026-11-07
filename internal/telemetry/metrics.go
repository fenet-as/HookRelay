package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics encapsulates Prometheus metrics and registry for HookRelay.
type Metrics struct {
	Registry *prometheus.Registry

	// Event Ingest & Fanout
	EventsIngested    *prometheus.CounterVec
	DeliveriesCreated *prometheus.CounterVec

	// Delivery Dispatch
	DeliveryAttempts *prometheus.CounterVec
	DeliveryDuration *prometheus.HistogramVec

	// Operational & Reliability
	ReaperRecovered      prometheus.Counter
	EndpointsDeactivated prometheus.Counter

	// Queue State Gauges (Updated periodically)
	QueuePending    prometheus.Gauge
	QueueDue        prometheus.Gauge
	ActiveEndpoints prometheus.Gauge
}

// NewMetrics initializes and registers HookRelay Prometheus metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	// Register standard Go runtime and process metrics
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,

		EventsIngested: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "hookrelay",
				Subsystem: "events",
				Name:      "ingested_total",
				Help:      "Total number of events ingested via POST /events.",
			},
			[]string{"type", "duplicate"},
		),

		DeliveriesCreated: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "hookrelay",
				Subsystem: "deliveries",
				Name:      "created_total",
				Help:      "Total number of delivery tasks created during fan-out.",
			},
			[]string{"event_type"},
		),

		DeliveryAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "hookrelay",
				Subsystem: "deliveries",
				Name:      "attempts_total",
				Help:      "Total number of HTTP webhook delivery attempts.",
			},
			[]string{"status", "status_code"},
		),

		DeliveryDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "hookrelay",
				Subsystem: "deliveries",
				Name:      "duration_seconds",
				Help:      "Duration of outbound HTTP webhook requests in seconds.",
				Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"status"},
		),

		ReaperRecovered: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hookrelay",
				Subsystem: "reaper",
				Name:      "recovered_deliveries_total",
				Help:      "Total number of stuck deliveries recovered by the crash recovery reaper.",
			},
		),

		EndpointsDeactivated: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "hookrelay",
				Subsystem: "endpoints",
				Name:      "deactivated_total",
				Help:      "Total number of endpoints auto-deactivated due to consecutive failures.",
			},
		),

		QueuePending: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "hookrelay",
				Subsystem: "queue",
				Name:      "pending_deliveries",
				Help:      "Current count of deliveries in pending status.",
			},
		),

		QueueDue: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "hookrelay",
				Subsystem: "queue",
				Name:      "due_deliveries",
				Help:      "Current count of pending deliveries due for execution (next_run_at <= now).",
			},
		),

		ActiveEndpoints: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "hookrelay",
				Subsystem: "endpoints",
				Name:      "active_count",
				Help:      "Current count of active webhook endpoints.",
			},
		),
	}

	reg.MustRegister(
		m.EventsIngested,
		m.DeliveriesCreated,
		m.DeliveryAttempts,
		m.DeliveryDuration,
		m.ReaperRecovered,
		m.EndpointsDeactivated,
		m.QueuePending,
		m.QueueDue,
		m.ActiveEndpoints,
	)

	return m
}

// HTTPHandler returns an HTTP handler serving Prometheus metrics formatted for scraping.
func (m *Metrics) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Collector periodically samples database queue depth and active endpoints.
type Collector struct {
	queries *store.Queries
	metrics *Metrics
	logger  *slog.Logger
}

// NewCollector constructs a new background metrics collector.
func NewCollector(queries *store.Queries, metrics *Metrics, logger *slog.Logger) *Collector {
	return &Collector{
		queries: queries,
		metrics: metrics,
		logger:  logger,
	}
}

// Start runs periodic database gauge collection until ctx is canceled.
func (c *Collector) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial poll immediately
	c.collect(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(ctx)
		}
	}
}

func (c *Collector) collect(ctx context.Context) {
	if c.queries == nil {
		return
	}

	pending, err := c.queries.CountPendingDeliveries(ctx)
	if err == nil {
		c.metrics.QueuePending.Set(float64(pending))
	} else {
		c.logger.Debug("metrics collector failed to count pending deliveries", "error", err)
	}

	due, err := c.queries.CountDueDeliveries(ctx)
	if err == nil {
		c.metrics.QueueDue.Set(float64(due))
	} else {
		c.logger.Debug("metrics collector failed to count due deliveries", "error", err)
	}

	active, err := c.queries.CountActiveEndpoints(ctx)
	if err == nil {
		c.metrics.ActiveEndpoints.Set(float64(active))
	} else {
		c.logger.Debug("metrics collector failed to count active endpoints", "error", err)
	}
}
