package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/signing"
)

func newTestReceiver(secret string, defaultMode string) *ReceiverHandler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReceiverHandler(ReceiverConfig{
		DefaultMode:  defaultMode,
		Secret:       secret,
		SlowDuration: 50 * time.Millisecond,
	}, logger)
}

func TestReceiver_Modes(t *testing.T) {
	t.Run("default ok mode", func(t *testing.T) {
		h := newTestReceiver("", "ok")
		req := httptest.NewRequest("POST", "/webhook", bytes.NewReader([]byte(`{"test":true}`)))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
	})

	t.Run("query param mode fail", func(t *testing.T) {
		h := newTestReceiver("", "ok")
		req := httptest.NewRequest("POST", "/webhook?mode=fail", bytes.NewReader([]byte(`{}`)))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", w.Code)
		}
	})

	t.Run("header mode slow", func(t *testing.T) {
		h := newTestReceiver("", "ok")
		req := httptest.NewRequest("POST", "/webhook?delay=20ms", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("X-Test-Mode", "slow")
		w := httptest.NewRecorder()

		start := time.Now()
		h.ServeHTTP(w, req)
		elapsed := time.Since(start)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		if elapsed < 20*time.Millisecond {
			t.Errorf("expected at least 20ms delay, got %v", elapsed)
		}
	})

	t.Run("flaky mode returns both 200 and 500", func(t *testing.T) {
		h := newTestReceiver("", "flaky")
		statusCodes := make(map[int]int)

		for i := 0; i < 30; i++ {
			req := httptest.NewRequest("POST", "/webhook", bytes.NewReader([]byte(`{}`)))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			statusCodes[w.Code]++
		}

		if statusCodes[http.StatusOK] == 0 || statusCodes[http.StatusInternalServerError] == 0 {
			t.Fatalf("expected flaky mode to yield both 200 and 500 responses, got: %+v", statusCodes)
		}
	})
}

func TestReceiver_SignatureVerification(t *testing.T) {
	secret := "whsec_test_shared_secret"
	h := newTestReceiver(secret, "ok")
	body := []byte(`{"event": "invoice.paid", "amount": 5000}`)
	now := time.Now().Unix()

	t.Run("valid signature accepted", func(t *testing.T) {
		sig := signing.Sign(secret, now, body)

		req := httptest.NewRequest("POST", "/webhook", bytes.NewReader(body))
		req.Header.Set(signing.HeaderTimestamp, strconv.FormatInt(now, 10))
		req.Header.Set(signing.HeaderSignature, sig)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for valid signature, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("missing signature header rejected", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/webhook", bytes.NewReader(body))
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized for missing signature, got %d", w.Code)
		}
	})

	t.Run("tampered body rejected", func(t *testing.T) {
		sig := signing.Sign(secret, now, body)
		tamperedBody := []byte(`{"event": "invoice.paid", "amount": 0}`)

		req := httptest.NewRequest("POST", "/webhook", bytes.NewReader(tamperedBody))
		req.Header.Set(signing.HeaderTimestamp, strconv.FormatInt(now, 10))
		req.Header.Set(signing.HeaderSignature, sig)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized for tampered body, got %d", w.Code)
		}

		var res map[string]string
		_ = json.Unmarshal(w.Body.Bytes(), &res)
		if res["error"] != "invalid signature" {
			t.Errorf("expected error 'invalid signature', got %s", res["error"])
		}
	})
}
