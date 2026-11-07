package api_test

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
	"strings"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/api"
	"github.com/fenet-as/hookrelay/internal/crypto"
	"github.com/fenet-as/hookrelay/internal/id"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/fenet-as/hookrelay/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"
}

func setupTestServer(t *testing.T) (*api.Server, *pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()
	dbURL := getTestDatabaseURL()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: dbURL,
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := api.NewServer(pool, logger, telemetry.NewMetrics())

	cleanup := func() {
		pool.Close()
	}

	return server, pool, cleanup
}

func performRequest(r http.Handler, method, path string, body []byte, headers ...map[string]string) *httptest.ResponseRecorder {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if len(headers) > 0 {
		for k, v := range headers[0] {
			req.Header.Set(k, v)
		}
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHealthz(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	w := performRequest(server.Router(), "GET", "/healthz", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var res map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if res["status"] != "ok" {
		t.Errorf("expected status ok, got %s", res["status"])
	}
}

func TestCreateEndpoint_Success(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	payload := []byte(`{
		"url": "https://example.com/webhook",
		"event_types": ["order.created", "order.paid"]
	}`)

	w := performRequest(server.Router(), "POST", "/endpoints", payload)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}

	var res api.EndpointCreatedResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if res.ID <= 0 {
		t.Errorf("expected positive ID, got %d", res.ID)
	}
	if res.Url != "https://example.com/webhook" {
		t.Errorf("expected url https://example.com/webhook, got %s", res.Url)
	}
	if !strings.HasPrefix(res.Secret, crypto.SecretPrefix) {
		t.Errorf("expected secret to have prefix %s, got %s", crypto.SecretPrefix, res.Secret)
	}
	if !res.Active {
		t.Errorf("expected endpoint to be active")
	}
	if len(res.EventTypes) != 2 {
		t.Errorf("expected 2 event types, got %d", len(res.EventTypes))
	}
}

func TestCreateEndpoint_InvalidURL(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	testCases := []struct {
		name string
		url  string
	}{
		{"empty url", ""},
		{"invalid scheme ftp", "ftp://example.com/hook"},
		{"plain text", "not-a-url"},
		{"missing host", "http://"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload, _ := json.Marshal(map[string]string{"url": tc.url})
			w := performRequest(server.Router(), "POST", "/endpoints", payload)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400 Bad Request for %q, got %d", tc.url, w.Code)
			}
		})
	}
}

func TestGetEndpoint_SecretOmitted(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	createPayload := []byte(`{"url": "https://example.com/credentials-check"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	wGet := performRequest(server.Router(), "GET", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", wGet.Code)
	}

	bodyStr := wGet.Body.String()
	if strings.Contains(bodyStr, `"secret":`) || strings.Contains(bodyStr, created.Secret) {
		t.Fatalf("GET /endpoints/:id must never expose secret, but found it in body: %s", bodyStr)
	}

	wList := performRequest(server.Router(), "GET", "/endpoints", nil)
	if wList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for list, got %d", wList.Code)
	}
	listBodyStr := wList.Body.String()
	if strings.Contains(listBodyStr, `"secret":`) || strings.Contains(listBodyStr, created.Secret) {
		t.Fatalf("GET /endpoints list must never contain secrets, but found secret in body: %s", listBodyStr)
	}
}

func TestUpdateEndpoint(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	createPayload := []byte(`{"url": "https://example.com/v1"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	newURL := "https://example.com/v2"
	active := false
	updatePayload, _ := json.Marshal(map[string]interface{}{
		"url":    newURL,
		"active": active,
	})

	wPatch := performRequest(server.Router(), "PATCH", fmt.Sprintf("/endpoints/%d", created.ID), updatePayload)
	if wPatch.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wPatch.Code, wPatch.Body.String())
	}

	var updated api.EndpointResponse
	_ = json.Unmarshal(wPatch.Body.Bytes(), &updated)
	if updated.Url != newURL {
		t.Errorf("expected updated url %s, got %s", newURL, updated.Url)
	}
	if updated.Active != false {
		t.Errorf("expected active to be false")
	}
}

func TestRotateSecret(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	createPayload := []byte(`{"url": "https://example.com/rotate-test"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	wRotate := performRequest(server.Router(), "POST", fmt.Sprintf("/endpoints/%d/rotate-secret", created.ID), nil)
	if wRotate.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", wRotate.Code, wRotate.Body.String())
	}

	var rotated api.RotateSecretResponse
	if err := json.Unmarshal(wRotate.Body.Bytes(), &rotated); err != nil {
		t.Fatalf("failed to parse rotate response: %v", err)
	}

	if rotated.Secret == created.Secret {
		t.Fatalf("expected new secret to be different from original secret")
	}
	if !strings.HasPrefix(rotated.Secret, crypto.SecretPrefix) {
		t.Errorf("expected secret to have prefix %s, got %s", crypto.SecretPrefix, rotated.Secret)
	}
}

func TestDeleteEndpoint(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	createPayload := []byte(`{"url": "https://example.com/to-delete"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	wDelete := performRequest(server.Router(), "DELETE", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", wDelete.Code)
	}

	wGet := performRequest(server.Router(), "GET", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wGet.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after delete, got %d", wGet.Code)
	}
}

func TestPublishEvent_SuccessAndFanOut(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Create matching endpoint
	epPayload := []byte(`{"url": "https://example.com/events-receiver", "event_types": ["charge.succeeded"]}`)
	wEp := performRequest(server.Router(), "POST", "/endpoints", epPayload)
	if wEp.Code != http.StatusCreated {
		t.Fatalf("failed to create endpoint: %d", wEp.Code)
	}

	// 2. Publish event
	eventPayload := []byte(`{
		"type": "charge.succeeded",
		"data": {"charge_id": "ch_123", "amount": 4200}
	}`)
	wPub := performRequest(server.Router(), "POST", "/events", eventPayload)
	if wPub.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", wPub.Code, wPub.Body.String())
	}

	var pubRes api.PublishEventResponse
	if err := json.Unmarshal(wPub.Body.Bytes(), &pubRes); err != nil {
		t.Fatalf("invalid json response: %v", err)
	}

	if !strings.HasPrefix(pubRes.ID, id.EventPrefix) {
		t.Errorf("expected event ID prefix %s, got %s", id.EventPrefix, pubRes.ID)
	}
	if pubRes.Type != "charge.succeeded" {
		t.Errorf("expected type charge.succeeded, got %s", pubRes.Type)
	}
	if pubRes.DeliveriesCount == 0 {
		t.Errorf("expected at least 1 delivery created")
	}
	if pubRes.IsDuplicate {
		t.Errorf("expected new event, not duplicate")
	}

	// 3. Query GET /events/:id
	wGetEvt := performRequest(server.Router(), "GET", "/events/"+pubRes.ID, nil)
	if wGetEvt.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /events/:id, got %d", wGetEvt.Code)
	}

	var evtRes api.EventResponse
	if err := json.Unmarshal(wGetEvt.Body.Bytes(), &evtRes); err != nil {
		t.Fatalf("invalid json for GET event: %v", err)
	}
	if evtRes.ID != pubRes.ID {
		t.Errorf("expected event ID %s, got %s", pubRes.ID, evtRes.ID)
	}
	if len(evtRes.Deliveries) == 0 {
		t.Fatalf("expected deliveries attached to event response")
	}

	// 4. Query GET /deliveries/:id
	deliveryID := evtRes.Deliveries[0].ID
	wGetDeliv := performRequest(server.Router(), "GET", fmt.Sprintf("/deliveries/%d", deliveryID), nil)
	if wGetDeliv.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /deliveries/:id, got %d", wGetDeliv.Code)
	}

	var delivRes api.DeliveryResponse
	if err := json.Unmarshal(wGetDeliv.Body.Bytes(), &delivRes); err != nil {
		t.Fatalf("invalid json for GET delivery: %v", err)
	}
	if delivRes.ID != deliveryID {
		t.Errorf("expected delivery ID %d, got %d", deliveryID, delivRes.ID)
	}
}

func TestPublishEvent_IdempotencyDeduplication(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	idemKey := "idem_api_test_" + time.Now().Format("20060102150405.000000")
	payload := []byte(`{"type": "user.signup", "data": {"user_id": "u_99"}}`)
	headers := map[string]string{"Idempotency-Key": idemKey}

	// 1. First publish
	w1 := performRequest(server.Router(), "POST", "/events", payload, headers)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on first publish, got %d: %s", w1.Code, w1.Body.String())
	}
	var res1 api.PublishEventResponse
	_ = json.Unmarshal(w1.Body.Bytes(), &res1)
	if res1.IsDuplicate {
		t.Fatalf("first publish should not be marked as duplicate")
	}

	// 2. Second publish with identical Idempotency-Key
	w2 := performRequest(server.Router(), "POST", "/events", payload, headers)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on duplicate publish, got %d: %s", w2.Code, w2.Body.String())
	}
	var res2 api.PublishEventResponse
	_ = json.Unmarshal(w2.Body.Bytes(), &res2)
	if !res2.IsDuplicate {
		t.Fatalf("second publish must be marked as duplicate")
	}
	if res2.ID != res1.ID {
		t.Errorf("expected duplicate publish to return original event ID %s, got %s", res1.ID, res2.ID)
	}
}

func TestReplayDelivery(t *testing.T) {
	server, pool, cleanup := setupTestServer(t)
	defer cleanup()

	queries := store.New(pool)
	ctx := context.Background()

	// 1. Create an endpoint
	epPayload := []byte(`{"url": "https://example.com/replay-target", "event_types": ["payment.refunded"]}`)
	wEp := performRequest(server.Router(), "POST", "/endpoints", epPayload)
	if wEp.Code != http.StatusCreated {
		t.Fatalf("failed to create endpoint: %d", wEp.Code)
	}

	// 2. Publish event
	eventPayload := []byte(`{"type": "payment.refunded", "data": {"refund_id": "ref_555"}}`)
	wPub := performRequest(server.Router(), "POST", "/events", eventPayload)
	if wPub.Code != http.StatusAccepted {
		t.Fatalf("failed to publish event: %d", wPub.Code)
	}

	var pubRes api.PublishEventResponse
	_ = json.Unmarshal(wPub.Body.Bytes(), &pubRes)

	// Get delivery ID
	wGetEvt := performRequest(server.Router(), "GET", "/events/"+pubRes.ID, nil)
	var evtRes api.EventResponse
	_ = json.Unmarshal(wGetEvt.Body.Bytes(), &evtRes)
	if len(evtRes.Deliveries) == 0 {
		t.Fatalf("expected at least 1 delivery created")
	}
	deliveryID := evtRes.Deliveries[0].ID

	// 3. Mark delivery as dead
	_, err := queries.MarkDeliveryDead(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to mark delivery dead: %v", err)
	}

	// 4. Request Replay POST /deliveries/:id/replay
	wReplay := performRequest(server.Router(), "POST", fmt.Sprintf("/deliveries/%d/replay", deliveryID), nil)
	if wReplay.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for replay, got %d: %s", wReplay.Code, wReplay.Body.String())
	}

	var replayRes api.DeliveryResponse
	if err := json.Unmarshal(wReplay.Body.Bytes(), &replayRes); err != nil {
		t.Fatalf("failed to parse replay response: %v", err)
	}

	if replayRes.Status != string(store.DeliveryStatusPending) {
		t.Errorf("expected status pending after replay, got %s", replayRes.Status)
	}
	if replayRes.Attempts != 0 {
		t.Errorf("expected attempts reset to 0 after replay, got %d", replayRes.Attempts)
	}

	// 5. Verify non-existent delivery returns 404
	wNotFound := performRequest(server.Router(), "POST", "/deliveries/999999999/replay", nil)
	if wNotFound.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent delivery replay, got %d", wNotFound.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Publish an event to trigger metric increments
	eventPayload := []byte(`{"type": "telemetry.test", "data": {"key": "val"}}`)
	wPub := performRequest(server.Router(), "POST", "/events", eventPayload)
	if wPub.Code != http.StatusAccepted {
		t.Fatalf("failed to publish event: %d", wPub.Code)
	}

	w := performRequest(server.Router(), "GET", "/metrics", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /metrics, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "hookrelay_events_ingested_total") {
		t.Errorf("expected /metrics to expose hookrelay_events_ingested_total, got:\n%s", body)
	}
	if !strings.Contains(body, "hookrelay_queue_pending_deliveries") {
		t.Errorf("expected /metrics to expose hookrelay_queue_pending_deliveries")
	}
}
