package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/fenet-as/hookrelay/internal/signing"
)

type ReceiverConfig struct {
	Port         string
	DefaultMode  string
	Secret       string
	SlowDuration time.Duration
}

type ReceiverHandler struct {
	config       ReceiverConfig
	logger       *slog.Logger
	requestCount uint64
}

func NewReceiverHandler(cfg ReceiverConfig, logger *slog.Logger) *ReceiverHandler {
	if cfg.DefaultMode == "" {
		cfg.DefaultMode = "ok"
	}
	if cfg.SlowDuration == 0 {
		cfg.SlowDuration = 3 * time.Second
	}
	return &ReceiverHandler{
		config: cfg,
		logger: logger,
	}
}

func (h *ReceiverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	atomic.AddUint64(&h.requestCount, 1)

	// Determine operating mode: query param > header > default config
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = r.Header.Get("X-Test-Mode")
	}
	if mode == "" {
		mode = h.config.DefaultMode
	}

	// Read raw body (essential for HMAC verification)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("failed to read request body", "error", err)
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Extract HookRelay webhook headers
	eventID := r.Header.Get(signing.HeaderEventID)
	deliveryID := r.Header.Get(signing.HeaderDeliveryID)
	attemptStr := r.Header.Get(signing.HeaderAttempt)
	timestampStr := r.Header.Get(signing.HeaderTimestamp)
	signature := r.Header.Get(signing.HeaderSignature)

	h.logger.Info("received webhook delivery",
		slog.String("mode", mode),
		slog.String("event_id", eventID),
		slog.String("delivery_id", deliveryID),
		slog.String("attempt", attemptStr),
		slog.String("timestamp", timestampStr),
		slog.String("signature", signature),
		slog.Int("body_bytes", len(body)),
	)

	// Verify signature if secret is configured
	secret := r.URL.Query().Get("secret")
	if secret == "" {
		secret = h.config.Secret
	}

	if secret != "" {
		if signature == "" || timestampStr == "" {
			h.logger.Warn("missing required signature or timestamp header")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "missing signature or timestamp header",
			})
			return
		}

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			h.logger.Warn("invalid timestamp format", "timestamp", timestampStr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "invalid timestamp header",
			})
			return
		}

		if err := signing.Verify(secret, timestamp, body, signature, signing.DefaultTolerance); err != nil {
			h.logger.Warn("signature verification failed", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "invalid signature",
				"details": err.Error(),
			})
			return
		}
		h.logger.Info("signature verified successfully")
	}

	// Apply mode behavior
	w.Header().Set("Content-Type", "application/json")
	switch mode {
	case "fail":
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "simulated receiver failure",
			"mode":  "fail",
		})

	case "slow":
		delay := h.config.SlowDuration
		if dStr := r.URL.Query().Get("delay"); dStr != "" {
			if parsed, err := time.ParseDuration(dStr); err == nil {
				delay = parsed
			}
		}
		h.logger.Info("simulating slow receiver", slog.Duration("delay", delay))
		time.Sleep(delay)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"mode":     "slow",
			"duration": time.Since(start).String(),
		})

	case "flaky":
		// Fail approximately 50% of the time
		isFail := rand.IntN(2) == 0
		if isFail {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "simulated flaky error",
				"mode":  "flaky",
			})
		} else {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
				"mode":   "flaky",
			})
		}

	case "ok":
		fallthrough
	default:
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok",
			"mode":   "ok",
		})
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
