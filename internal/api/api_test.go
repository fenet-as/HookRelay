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

	"github.com/fenet-as/hookrelay/internal/api"
	"github.com/fenet-as/hookrelay/internal/crypto"
	"github.com/fenet-as/hookrelay/internal/store"
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
	server := api.NewServer(pool, logger)

	cleanup := func() {
		pool.Close()
	}

	return server, pool, cleanup
}

func performRequest(r http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
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

	// 1. Create endpoint
	createPayload := []byte(`{"url": "https://example.com/credentials-check"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	// 2. Fetch endpoint by ID
	wGet := performRequest(server.Router(), "GET", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", wGet.Code)
	}

	// Raw JSON must NOT contain `"secret":` or the raw secret token
	bodyStr := wGet.Body.String()
	if strings.Contains(bodyStr, `"secret":`) || strings.Contains(bodyStr, created.Secret) {
		t.Fatalf("GET /endpoints/:id must never expose secret, but found it in body: %s", bodyStr)
	}

	// 3. List endpoints
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

	// Create
	createPayload := []byte(`{"url": "https://example.com/v1"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	// Update URL and Active status
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

	// Create
	createPayload := []byte(`{"url": "https://example.com/rotate-test"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	// Rotate secret
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

	// Create
	createPayload := []byte(`{"url": "https://example.com/to-delete"}`)
	wCreate := performRequest(server.Router(), "POST", "/endpoints", createPayload)
	var created api.EndpointCreatedResponse
	_ = json.Unmarshal(wCreate.Body.Bytes(), &created)

	// Delete
	wDelete := performRequest(server.Router(), "DELETE", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", wDelete.Code)
	}

	// Verify 404
	wGet := performRequest(server.Router(), "GET", fmt.Sprintf("/endpoints/%d", created.ID), nil)
	if wGet.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after delete, got %d", wGet.Code)
	}
}
