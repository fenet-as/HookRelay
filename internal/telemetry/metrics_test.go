package telemetry_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/telemetry"
)

func TestMetrics_HTTPHandler(t *testing.T) {
	m := telemetry.NewMetrics()

	// Increment some metrics
	m.EventsIngested.WithLabelValues("order.created", "false").Inc()
	m.DeliveriesCreated.WithLabelValues("order.created").Add(3)
	m.DeliveryAttempts.WithLabelValues("success", "200").Inc()
	m.DeliveryDuration.WithLabelValues("success").Observe(0.045)
	m.ReaperRecovered.Add(2)
	m.EndpointsDeactivated.Inc()
	m.QueuePending.Set(15)
	m.QueueDue.Set(3)
	m.ActiveEndpoints.Set(5)

	ts := httptest.NewServer(m.HTTPHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("failed to scrape metrics endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	output := string(body)

	expectedMetrics := []string{
		"hookrelay_events_ingested_total",
		"hookrelay_deliveries_created_total",
		"hookrelay_deliveries_attempts_total",
		"hookrelay_deliveries_duration_seconds",
		"hookrelay_reaper_recovered_deliveries_total",
		"hookrelay_endpoints_deactivated_total",
		"hookrelay_queue_pending_deliveries",
		"hookrelay_queue_due_deliveries",
		"hookrelay_endpoints_active_count",
		"go_goroutines",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("expected metrics output to contain %q, but was missing", metric)
		}
	}
}

func TestCollector_GracefulStop(t *testing.T) {
	m := telemetry.NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	collector := telemetry.NewCollector(nil, m, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		collector.Start(ctx, 20*time.Millisecond)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Stopped cleanly
	case <-time.After(1 * time.Second):
		t.Fatalf("expected collector to stop on context cancellation")
	}
}
