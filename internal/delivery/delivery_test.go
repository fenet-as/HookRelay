package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/delivery"
	"github.com/fenet-as/hookrelay/internal/signing"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, *store.Queries, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	queries := store.New(pool)
	cleanup := func() {
		pool.Close()
	}
	return pool, queries, cleanup
}

func setupDeliveryFixtures(t *testing.T, ctx context.Context, queries *store.Queries, targetURL string) (store.Endpoint, store.Event, store.Delivery) {
	t.Helper()
	ep, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        targetURL,
		Secret:     "whsec_test_dispatcher_secret",
		EventTypes: []string{},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("create fixture endpoint failed: %v", err)
	}

	evtID := "evt_test_" + time.Now().Format("20060102150405.000000")
	evt, err := queries.CreateEvent(ctx, store.CreateEventParams{
		ID:      evtID,
		Type:    "payment.completed",
		Payload: json.RawMessage(`{"payment_id": "pay_987", "amount": 1000}`),
	})
	if err != nil {
		t.Fatalf("create fixture event failed: %v", err)
	}

	deliv, err := queries.CreateDelivery(ctx, store.CreateDeliveryParams{
		EventID:    evt.ID,
		EndpointID: ep.ID,
	})
	if err != nil {
		t.Fatalf("create fixture delivery failed: %v", err)
	}

	return ep, evt, deliv
}

func TestClient_RedirectsNotFollowed(t *testing.T) {
	// A server that attempts to redirect the client
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://malicious-internal-server.local/pwn", http.StatusFound)
	}))
	defer ts.Close()

	cfg := delivery.DefaultClientConfig()
	client := delivery.NewClient(cfg)

	req, _ := http.NewRequest("POST", ts.URL, nil)
	resp, err := client.HTTPClient().Do(req)
	if err != nil {
		t.Fatalf("unexpected request error: %v", err)
	}
	defer resp.Body.Close()

	// Should receive 302 Found without following
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 Found, got %d", resp.StatusCode)
	}
}

func TestDispatcher_SuccessfulDeliveryAndAttemptLogging(t *testing.T) {
	ctx := context.Background()
	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	var receivedHeaders http.Header
	var receivedBody []byte

	secret := "whsec_test_dispatcher_secret"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received": true}`))
	}))
	defer ts.Close()

	ep, evt, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	cfg := delivery.DefaultClientConfig()
	client := delivery.NewClient(cfg)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	result, err := dispatcher.Execute(ctx, delivery.DeliveryRequest{
		DeliveryID: deliv.ID,
		EventID:    evt.ID,
		EndpointID: ep.ID,
		URL:        ts.URL,
		Secret:     secret,
		Attempt:    1,
		Payload:    evt.Payload,
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Errorf("expected success to be true, got false")
	}
	if result.StatusCode != 200 {
		t.Errorf("expected status code 200, got %d", result.StatusCode)
	}
	if result.ResponseBody != `{"received": true}` {
		t.Errorf("expected response body, got %s", result.ResponseBody)
	}

	if len(receivedBody) == 0 {
		t.Errorf("expected non-empty received body")
	}

	// Verify standard delivery headers were received
	if receivedHeaders.Get(signing.HeaderEventID) != evt.ID {
		t.Errorf("expected event ID header %s, got %s", evt.ID, receivedHeaders.Get(signing.HeaderEventID))
	}
	if receivedHeaders.Get(signing.HeaderAttempt) != "1" {
		t.Errorf("expected attempt header '1', got %s", receivedHeaders.Get(signing.HeaderAttempt))
	}
	sigHeader := receivedHeaders.Get(signing.HeaderSignature)
	if !strings.HasPrefix(sigHeader, "sha256=") {
		t.Errorf("expected signature header to start with sha256=, got %s", sigHeader)
	}

	// Verify attempt row was recorded in DB
	attempts, err := queries.ListAttemptsByDeliveryID(ctx, deliv.ID)
	if err != nil {
		t.Fatalf("ListAttemptsByDeliveryID failed: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 recorded attempt, got %d", len(attempts))
	}
	if attempts[0].StatusCode.Int32 != 200 {
		t.Errorf("expected stored status code 200, got %d", attempts[0].StatusCode.Int32)
	}
	if attempts[0].ResponseBody.String != `{"received": true}` {
		t.Errorf("expected stored response body, got %s", attempts[0].ResponseBody.String)
	}
}

func TestDispatcher_TimeoutOnSlowReceiver(t *testing.T) {
	ctx := context.Background()
	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// Slow server that sleeps longer than client timeout
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ep, evt, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	// Short timeout of 50ms
	cfg := delivery.DefaultClientConfig()
	cfg.Timeout = 50 * time.Millisecond
	client := delivery.NewClient(cfg)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	result, err := dispatcher.Execute(ctx, delivery.DeliveryRequest{
		DeliveryID: deliv.ID,
		EventID:    evt.ID,
		EndpointID: ep.ID,
		URL:        ts.URL,
		Secret:     "whsec_timeout",
		Attempt:    1,
		Payload:    evt.Payload,
	})
	if err != nil {
		t.Fatalf("unexpected Execute error: %v", err)
	}

	if result.Success {
		t.Fatalf("expected timeout delivery to be marked as failed")
	}
	if result.Error == "" {
		t.Errorf("expected error message to be recorded for timeout")
	}

	// Verify attempt row logged in DB with error
	attempts, err := queries.ListAttemptsByDeliveryID(ctx, deliv.ID)
	if err != nil {
		t.Fatalf("ListAttemptsByDeliveryID failed: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 recorded attempt, got %d", len(attempts))
	}
	if !attempts[0].Error.Valid || attempts[0].Error.String == "" {
		t.Errorf("expected DB attempt to store error message")
	}
}

func TestDispatcher_BoundedBodyRead(t *testing.T) {
	ctx := context.Background()
	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// Server returning a large payload of 10 KB
	largePayload := strings.Repeat("A", 10*1024)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(largePayload))
	}))
	defer ts.Close()

	ep, evt, deliv := setupDeliveryFixtures(t, ctx, queries, ts.URL)
	defer queries.DeleteEndpoint(ctx, ep.ID)

	// Client with 512-byte limit
	cfg := delivery.DefaultClientConfig()
	cfg.MaxResponseBodyBytes = 512
	client := delivery.NewClient(cfg)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	result, err := dispatcher.Execute(ctx, delivery.DeliveryRequest{
		DeliveryID: deliv.ID,
		EventID:    evt.ID,
		EndpointID: ep.ID,
		URL:        ts.URL,
		Secret:     "whsec_large",
		Attempt:    1,
		Payload:    evt.Payload,
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.ResponseBody) != 512 {
		t.Errorf("expected response body to be capped at 512 bytes, got %d bytes", len(result.ResponseBody))
	}
}
