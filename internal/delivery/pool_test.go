package delivery_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/delivery"
	"github.com/fenet-as/hookrelay/internal/store"
)

func TestPool_SuccessfulDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Receiver returns 200 OK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status": "ok"}`))
	}))
	defer ts.Close()

	ep, _, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := delivery.NewClient(delivery.DefaultClientConfig())
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	poolCfg := delivery.PoolConfig{
		Workers:            2,
		PollInterval:       20 * time.Millisecond,
		BatchSize:          5,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     3,
			InitialInterval: 20 * time.Millisecond,
			MaxInterval:     100 * time.Millisecond,
		},
	}

	pool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	pool.Start(ctx)
	defer pool.Stop()

	// Wait for delivery to reach succeeded
	deadline := time.Now().Add(3 * time.Second)
	var finalDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliv.ID)
		if err == nil && d.Status == store.DeliveryStatusSucceeded {
			finalDeliv = d
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalDeliv.Status != store.DeliveryStatusSucceeded {
		t.Fatalf("expected delivery to succeed, got status: %s", finalDeliv.Status)
	}
	if finalDeliv.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", finalDeliv.Attempts)
	}
}

func TestPool_FailingReceiverTransitionsToDead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Receiver always returns 500
	var callCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	ep, _, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := delivery.NewClient(delivery.DefaultClientConfig())
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	const maxAttempts = 3
	poolCfg := delivery.PoolConfig{
		Workers:            2,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          5,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     maxAttempts,
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     50 * time.Millisecond,
		},
	}

	pool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	pool.Start(ctx)
	defer pool.Stop()

	// Wait for delivery to reach dead
	deadline := time.Now().Add(4 * time.Second)
	var finalDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliv.ID)
		if err == nil && d.Status == store.DeliveryStatusDead {
			finalDeliv = d
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalDeliv.Status != store.DeliveryStatusDead {
		t.Fatalf("expected delivery to transition to dead after %d attempts, got: %s", maxAttempts, finalDeliv.Status)
	}
	if finalDeliv.Attempts != maxAttempts {
		t.Errorf("expected %d attempts, got %d", maxAttempts, finalDeliv.Attempts)
	}
}

func TestPool_NonRetryableStatusMarksDeadImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Receiver returns 404 Not Found (permanent non-retryable)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	ep, _, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := delivery.NewClient(delivery.DefaultClientConfig())
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	poolCfg := delivery.PoolConfig{
		Workers:            2,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          5,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     5,
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     50 * time.Millisecond,
		},
	}

	pool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	pool.Start(ctx)
	defer pool.Stop()

	// Should transition to dead on attempt 1 without retrying
	deadline := time.Now().Add(2 * time.Second)
	var finalDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliv.ID)
		if err == nil && d.Status == store.DeliveryStatusDead {
			finalDeliv = d
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalDeliv.Status != store.DeliveryStatusDead {
		t.Fatalf("expected non-retryable error to mark dead immediately, got: %s", finalDeliv.Status)
	}
	if finalDeliv.Attempts != 1 {
		t.Errorf("expected exactly 1 attempt before marking dead, got %d", finalDeliv.Attempts)
	}
}

func TestPool_FlakyReceiverEventuallySucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// Fails on call 1, succeeds on call 2
	var callCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := callCount.Add(1)
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ep, _, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := delivery.NewClient(delivery.DefaultClientConfig())
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	poolCfg := delivery.PoolConfig{
		Workers:            2,
		PollInterval:       10 * time.Millisecond,
		BatchSize:          5,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     5,
			InitialInterval: 20 * time.Millisecond,
			MaxInterval:     50 * time.Millisecond,
		},
	}

	pool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	pool.Start(ctx)
	defer pool.Stop()

	// Wait for delivery to reach succeeded on retry
	deadline := time.Now().Add(3 * time.Second)
	var finalDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliv.ID)
		if err == nil && d.Status == store.DeliveryStatusSucceeded {
			finalDeliv = d
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalDeliv.Status != store.DeliveryStatusSucceeded {
		t.Fatalf("expected flaky delivery to eventually succeed, got: %s", finalDeliv.Status)
	}
	if finalDeliv.Attempts != 2 {
		t.Errorf("expected 2 attempts before success, got %d", finalDeliv.Attempts)
	}
}
