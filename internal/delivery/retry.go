package delivery

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines exponential backoff parameters and attempt limits.
type RetryPolicy struct {
	MaxAttempts       int32
	InitialInterval   time.Duration
	MaxInterval       time.Duration
	BackoffMultiplier float64
	JitterFraction    float64
}

// DefaultRetryPolicy returns standard production retry rules:
// Up to 8 attempts starting at 5s, doubling up to 1 hour with 20% random jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       8,
		InitialInterval:   5 * time.Second,
		MaxInterval:       1 * time.Hour,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.20,
	}
}

// IsRetryable determines whether a delivery failure should be retried.
// Network errors, timeouts, 5xx server errors, 408 Request Timeout, and 429 Too Many Requests are retryable.
// Other 4xx responses (400, 401, 404, 410, 422) represent permanent client rejections and must NOT be retried.
func IsRetryable(statusCode int, err error) bool {
	// Any transport/network/timeout error is transient and retryable
	if err != nil {
		return true
	}

	// 408 (Request Timeout) and 429 (Too Many Requests / Rate Limited) are retryable
	if statusCode == 408 || statusCode == 429 {
		return true
	}

	// 5xx (500 Internal Server Error, 502 Bad Gateway, 503 Unavailable, 504 Timeout) are retryable
	if statusCode >= 500 && statusCode <= 599 {
		return true
	}

	// 2xx are successful (handled separately)
	// Other 4xx are permanent failures
	return false
}

// CalculateBackoff computes the delay before the next retry using exponential backoff with full jitter:
// delay = min(MaxInterval, InitialInterval * (Multiplier ^ (attempt - 1))) + jitter
func CalculateBackoff(attempt int32, policy RetryPolicy) time.Duration {
	if attempt <= 1 {
		attempt = 1
	}

	// Multiplier exponent: 2^(attempt - 1)
	multiplier := math.Pow(policy.BackoffMultiplier, float64(attempt-1))
	baseSec := policy.InitialInterval.Seconds() * multiplier

	// Cap at MaxInterval
	if baseSec > policy.MaxInterval.Seconds() {
		baseSec = policy.MaxInterval.Seconds()
	}

	// Add random jitter to prevent the "thundering herd" problem
	jitter := 0.0
	if policy.JitterFraction > 0 {
		jitter = baseSec * policy.JitterFraction * rand.Float64()
	}

	return time.Duration((baseSec + jitter) * float64(time.Second))
}
