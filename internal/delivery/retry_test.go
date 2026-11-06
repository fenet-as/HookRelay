package delivery_test

import (
	"errors"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/delivery"
)

func TestIsRetryable(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		err        error
		expected   bool
	}{
		{"network error", 0, errors.New("connection refused"), true},
		{"timeout error", 0, errors.New("context deadline exceeded"), true},
		{"500 internal server error", 500, nil, true},
		{"502 bad gateway", 502, nil, true},
		{"503 service unavailable", 503, nil, true},
		{"504 gateway timeout", 504, nil, true},
		{"408 request timeout", 408, nil, true},
		{"429 rate limited", 429, nil, true},
		{"400 bad request", 400, nil, false},
		{"401 unauthorized", 401, nil, false},
		{"403 forbidden", 403, nil, false},
		{"404 not found", 404, nil, false},
		{"410 gone", 410, nil, false},
		{"422 unprocessable entity", 422, nil, false},
		{"200 ok", 200, nil, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := delivery.IsRetryable(tc.statusCode, tc.err)
			if result != tc.expected {
				t.Errorf("IsRetryable(%d, %v) = %v; expected %v", tc.statusCode, tc.err, result, tc.expected)
			}
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	policy := delivery.RetryPolicy{
		MaxAttempts:       5,
		InitialInterval:   100 * time.Millisecond,
		MaxInterval:       2 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.20,
	}

	// Attempt 1: ~100ms
	d1 := delivery.CalculateBackoff(1, policy)
	if d1 < 100*time.Millisecond || d1 > 125*time.Millisecond {
		t.Errorf("unexpected attempt 1 backoff: %v", d1)
	}

	// Attempt 2: ~200ms
	d2 := delivery.CalculateBackoff(2, policy)
	if d2 < 200*time.Millisecond || d2 > 250*time.Millisecond {
		t.Errorf("unexpected attempt 2 backoff: %v", d2)
	}

	// Attempt 3: ~400ms
	d3 := delivery.CalculateBackoff(3, policy)
	if d3 < 400*time.Millisecond || d3 > 500*time.Millisecond {
		t.Errorf("unexpected attempt 3 backoff: %v", d3)
	}

	// High attempt: capped at MaxInterval (2s + jitter)
	dHigh := delivery.CalculateBackoff(10, policy)
	if dHigh < 2*time.Second || dHigh > 2500*time.Millisecond {
		t.Errorf("unexpected capped backoff: %v", dHigh)
	}
}
