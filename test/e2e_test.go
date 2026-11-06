package test_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/api"
	"github.com/fenet-as/hookrelay/internal/delivery"
	"github.com/fenet-as/hookrelay/internal/signing"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestDatabaseURL() string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"
	}
	return url
}

func setupE2E(t *testing.T) (*api.Server, *pgxpool.Pool, *store.Queries, func()) {
	t.Helper()
	ctx := context.Background()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    20,
	})
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := api.NewServer(pool, logger)
	queries := store.New(pool)

	cleanup := func() {
		pool.Close()
	}

	return server, pool, queries, cleanup
}

type recordedDelivery struct {
	Headers    http.Header
	Body       []byte
	EventID    string
	DeliveryID string
	Attempt    string
	Signature  string
	Timestamp  string
}

// TestE2E_ConcurrentIngestFanOutAndDelivery runs a comprehensive end-to-end integration test
// verifying concurrent event ingestion, idempotency deduplication, fan-out across multiple
// wildcard and filtered endpoints, signed HTTP delivery with HMAC verification, and worker pool execution.
func TestE2E_ConcurrentIngestFanOutAndDelivery(t *testing.T) {
	server, _, queries, cleanup := setupE2E(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		mu         sync.Mutex
		receivedA  []recordedDelivery
		receivedB  []recordedDelivery
		secretAMap = make(map[string]string)
	)

	// Receiver A: Subscribes to all events ("*")
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec := recordedDelivery{
			Headers:    r.Header.Clone(),
			Body:       body,
			EventID:    r.Header.Get("X-HookRelay-Event-Id"),
			DeliveryID: r.Header.Get("X-HookRelay-Delivery-Id"),
			Attempt:    r.Header.Get("X-HookRelay-Attempt"),
			Signature:  r.Header.Get("X-HookRelay-Signature"),
			Timestamp:  r.Header.Get("X-HookRelay-Timestamp"),
		}
		mu.Lock()
		receivedA = append(receivedA, rec)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"received"}`))
	}))
	defer serverA.Close()

	// Receiver B: Subscribes only to "order.created"
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec := recordedDelivery{
			Headers:    r.Header.Clone(),
			Body:       body,
			EventID:    r.Header.Get("X-HookRelay-Event-Id"),
			DeliveryID: r.Header.Get("X-HookRelay-Delivery-Id"),
			Attempt:    r.Header.Get("X-HookRelay-Attempt"),
			Signature:  r.Header.Get("X-HookRelay-Signature"),
			Timestamp:  r.Header.Get("X-HookRelay-Timestamp"),
		}
		mu.Lock()
		receivedB = append(receivedB, rec)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"received"}`))
	}))
	defer serverB.Close()

	// 1. Register Endpoint A (all event types)
	epABody := fmt.Sprintf(`{"url": "%s", "event_types": []}`, serverA.URL)
	reqA := httptest.NewRequest("POST", "/endpoints", bytes.NewBufferString(epABody))
	reqA.Header.Set("Content-Type", "application/json")
	wA := httptest.NewRecorder()
	server.Router().ServeHTTP(wA, reqA)
	if wA.Code != http.StatusCreated {
		t.Fatalf("failed to register endpoint A: %d - %s", wA.Code, wA.Body.String())
	}
	var epARes api.EndpointCreatedResponse
	_ = json.Unmarshal(wA.Body.Bytes(), &epARes)
	secretAMap["A"] = epARes.Secret
	defer queries.DeleteEndpoint(ctx, epARes.ID)

	// 2. Register Endpoint B (only "order.created")
	epBBody := fmt.Sprintf(`{"url": "%s", "event_types": ["order.created"]}`, serverB.URL)
	reqB := httptest.NewRequest("POST", "/endpoints", bytes.NewBufferString(epBBody))
	reqB.Header.Set("Content-Type", "application/json")
	wB := httptest.NewRecorder()
	server.Router().ServeHTTP(wB, reqB)
	if wB.Code != http.StatusCreated {
		t.Fatalf("failed to register endpoint B: %d - %s", wB.Code, wB.Body.String())
	}
	var epBRes api.EndpointCreatedResponse
	_ = json.Unmarshal(wB.Body.Bytes(), &epBRes)
	defer queries.DeleteEndpoint(ctx, epBRes.ID)

	// 3. Start background delivery worker pool with 4 concurrent workers
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clientCfg := delivery.DefaultClientConfig()
	clientCfg.AllowLocal = true // Allow loopback for httptest receivers
	client := delivery.NewClient(clientCfg)
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	poolCfg := delivery.PoolConfig{
		Workers:            4,
		PollInterval:       20 * time.Millisecond,
		BatchSize:          500,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     3,
			InitialInterval: 20 * time.Millisecond,
			MaxInterval:     100 * time.Millisecond,
		},
	}
	workerPool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	workerPool.Start(ctx)
	defer workerPool.Stop()

	// 4. Ingest events concurrently (10 order.created events, 5 user.updated events)
	const (
		numOrderEvents = 10
		numUserEvents  = 5
	)
	var wg sync.WaitGroup
	var publishedOrderCount atomic.Int32
	var publishedUserCount atomic.Int32

	// Publish order.created events concurrently
	for i := 0; i < numOrderEvents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			idemKey := fmt.Sprintf("idem_e2e_order_%d_%d", idx, time.Now().UnixNano())
			eventPayload := fmt.Sprintf(`{"type": "order.created", "data": {"order_id": %d}}`, idx)
			req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(eventPayload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idemKey)
			w := httptest.NewRecorder()
			server.Router().ServeHTTP(w, req)
			if w.Code == http.StatusAccepted {
				publishedOrderCount.Add(1)
			} else {
				t.Logf("order event %d failed with code %d: %s", idx, w.Code, w.Body.String())
			}
		}(i)
	}

	// Publish user.updated events concurrently (should only go to Endpoint A)
	for i := 0; i < numUserEvents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			idemKey := fmt.Sprintf("idem_e2e_user_%d_%d", idx, time.Now().UnixNano())
			eventPayload := fmt.Sprintf(`{"type": "user.updated", "data": {"user_id": %d}}`, idx)
			req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(eventPayload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idemKey)
			w := httptest.NewRecorder()
			server.Router().ServeHTTP(w, req)
			if w.Code == http.StatusAccepted {
				publishedUserCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if publishedOrderCount.Load() != numOrderEvents {
		t.Fatalf("expected %d published order events, got %d", numOrderEvents, publishedOrderCount.Load())
	}
	if publishedUserCount.Load() != numUserEvents {
		t.Fatalf("expected %d published user events, got %d", numUserEvents, publishedUserCount.Load())
	}

	// 5. Wait for all deliveries to be processed by worker pool
	expectedA := numOrderEvents + numUserEvents
	expectedB := numOrderEvents

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		countA := len(receivedA)
		countB := len(receivedB)
		mu.Unlock()

		if countA >= expectedA && countB >= expectedB {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedA) != expectedA {
		t.Errorf("endpoint A: expected %d deliveries, got %d", expectedA, len(receivedA))
	}
	if len(receivedB) != expectedB {
		t.Errorf("endpoint B: expected %d deliveries, got %d", expectedB, len(receivedB))
	}

	// 6. Verify HMAC-SHA256 signature and delivery contract headers on all Endpoint A requests
	secretA := secretAMap["A"]
	for _, rec := range receivedA {
		if rec.EventID == "" {
			t.Errorf("missing X-HookRelay-Event-Id header")
		}
		if rec.DeliveryID == "" {
			t.Errorf("missing X-HookRelay-Delivery-Id header")
		}
		if rec.Attempt != "1" {
			t.Errorf("expected attempt 1, got %s", rec.Attempt)
		}
		if rec.Timestamp == "" {
			t.Errorf("missing X-HookRelay-Timestamp header")
		}
		if rec.Signature == "" {
			t.Errorf("missing X-HookRelay-Signature header")
		}

		// Cryptographic verification
		tsInt, err := strconv.ParseInt(rec.Timestamp, 10, 64)
		if err != nil {
			t.Errorf("invalid timestamp format: %s", rec.Timestamp)
			continue
		}
		if err := signing.Verify(secretA, tsInt, rec.Body, rec.Signature, 5*time.Minute); err != nil {
			t.Errorf("HMAC signature verification failed on delivery %s: %v", rec.DeliveryID, err)
		}
	}
}

// TestE2E_FlakyReceiverAndDeadLetterReplay verifies retry behavior on failing receivers,
// eventual success or dead state, and recovery through the manual replay endpoint.
func TestE2E_FlakyReceiverAndDeadLetterReplay(t *testing.T) {
	server, _, queries, cleanup := setupE2E(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var attemptsCount atomic.Int32
	// Receiver fails first 2 attempts with 503, succeeds on 3rd attempt
	flakyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("flakyServer received %s %s", r.Method, r.URL.Path)
		att := attemptsCount.Add(1)
		if att <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"recovered"}`))
	}))
	defer flakyServer.Close()

	// 1. Register endpoint
	epBody := fmt.Sprintf(`{"url": "%s", "event_types": ["payment.refund"]}`, flakyServer.URL)
	reqEp := httptest.NewRequest("POST", "/endpoints", bytes.NewBufferString(epBody))
	reqEp.Header.Set("Content-Type", "application/json")
	wEp := httptest.NewRecorder()
	server.Router().ServeHTTP(wEp, reqEp)
	var epRes api.EndpointCreatedResponse
	_ = json.Unmarshal(wEp.Body.Bytes(), &epRes)
	defer queries.DeleteEndpoint(ctx, epRes.ID)

	// 2. Start worker pool
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clientCfg := delivery.DefaultClientConfig()
	clientCfg.AllowLocal = true
	client := delivery.NewClient(clientCfg)
	dispatcher := delivery.NewDispatcher(client, queries, logger)

	poolCfg := delivery.PoolConfig{
		Workers:            2,
		PollInterval:       15 * time.Millisecond,
		BatchSize:          500,
		LockStaleThreshold: 1 * time.Minute,
		RetryPolicy: delivery.RetryPolicy{
			MaxAttempts:     5,
			InitialInterval: 15 * time.Millisecond,
			MaxInterval:     40 * time.Millisecond,
		},
	}
	workerPool := delivery.NewPool(poolCfg, dispatcher, queries, logger)
	workerPool.Start(ctx)
	defer workerPool.Stop()

	// 3. Ingest event
	eventPayload := `{"type": "payment.refund", "data": {"refund_id": "ref_999"}}`
	reqEvt := httptest.NewRequest("POST", "/events", bytes.NewBufferString(eventPayload))
	reqEvt.Header.Set("Content-Type", "application/json")
	wEvt := httptest.NewRecorder()
	server.Router().ServeHTTP(wEvt, reqEvt)
	if wEvt.Code != http.StatusAccepted {
		t.Fatalf("publish event failed with code %d: %s", wEvt.Code, wEvt.Body.String())
	}
	var pubRes api.PublishEventResponse
	_ = json.Unmarshal(wEvt.Body.Bytes(), &pubRes)
	t.Logf("Published event: ID=%s, deliveries_count=%d", pubRes.ID, pubRes.DeliveriesCount)

	// 4. Query delivery ID for our specific endpoint
	reqGetEvt := httptest.NewRequest("GET", "/events/"+pubRes.ID, nil)
	wGetEvt := httptest.NewRecorder()
	server.Router().ServeHTTP(wGetEvt, reqGetEvt)
	var evtDetails api.EventResponse
	_ = json.Unmarshal(wGetEvt.Body.Bytes(), &evtDetails)
	if len(evtDetails.Deliveries) == 0 {
		t.Fatalf("expected delivery created")
	}

	var deliveryID int64
	for _, d := range evtDetails.Deliveries {
		if d.EndpointID == epRes.ID {
			deliveryID = d.ID
			break
		}
	}
	if deliveryID == 0 {
		t.Fatalf("delivery for endpoint %d not found in event deliveries: %+v", epRes.ID, evtDetails.Deliveries)
	}

	// 5. Wait for delivery to succeed on attempt 3
	deadline := time.Now().Add(6 * time.Second)
	var finalDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliveryID)
		if err == nil {
			finalDeliv = d
			if d.Status == store.DeliveryStatusSucceeded {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalDeliv.Status != store.DeliveryStatusSucceeded {
		attRows, _ := queries.ListAttemptsByDeliveryID(ctx, deliveryID)
		for _, a := range attRows {
			t.Logf("Attempt #%d: status=%v, error=%v", a.Attempt, a.StatusCode.Int32, a.Error.String)
		}
		t.Fatalf("expected flaky delivery to succeed, got status=%s, attempts=%d", finalDeliv.Status, finalDeliv.Attempts)
	}
	if finalDeliv.Attempts != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", finalDeliv.Attempts)
	}

	// 6. Test Manual Replay:
	// Now mark it dead artificially and call POST /deliveries/:id/replay
	_, _ = queries.MarkDeliveryDead(ctx, deliveryID)

	reqReplay := httptest.NewRequest("POST", fmt.Sprintf("/deliveries/%d/replay", deliveryID), nil)
	wReplay := httptest.NewRecorder()
	server.Router().ServeHTTP(wReplay, reqReplay)
	if wReplay.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replay, got %d", wReplay.Code)
	}

	// Wait for worker pool to re-deliver the replayed delivery
	deadline = time.Now().Add(4 * time.Second)
	var replayedDeliv store.Delivery
	for time.Now().Before(deadline) {
		d, err := queries.GetDeliveryByID(ctx, deliveryID)
		if err == nil {
			replayedDeliv = d
			if d.Status == store.DeliveryStatusSucceeded {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	if replayedDeliv.Status != store.DeliveryStatusSucceeded {
		t.Errorf("expected replayed delivery to succeed, got status: %s", replayedDeliv.Status)
	}
}
