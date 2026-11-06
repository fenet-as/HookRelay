package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	// HeaderEventID is the stable event identifier across retries.
	HeaderEventID = "X-HookRelay-Event-Id"
	// HeaderDeliveryID is the unique identifier for a specific endpoint delivery.
	HeaderDeliveryID = "X-HookRelay-Delivery-Id"
	// HeaderAttempt is the 1-based attempt sequence number.
	HeaderAttempt = "X-HookRelay-Attempt"
	// HeaderTimestamp is the Unix epoch seconds timestamp when signed.
	HeaderTimestamp = "X-HookRelay-Timestamp"
	// HeaderSignature is the HMAC-SHA256 signature in "sha256=<hex>" format.
	HeaderSignature = "X-HookRelay-Signature"

	// SignaturePrefix is the required prefix for HookRelay signatures.
	SignaturePrefix = "sha256="

	// DefaultTolerance is the default window (5 minutes) for replay attack prevention.
	DefaultTolerance = 5 * time.Minute
)

var (
	ErrInvalidSignatureHeader = errors.New("invalid signature header: must start with 'sha256='")
	ErrInvalidHexSignature    = errors.New("signature contains invalid hex encoding")
	ErrInvalidSignature       = errors.New("signature mismatch: payload or secret does not match")
	ErrTimestampTooOld        = errors.New("timestamp is older than tolerance window (potential replay attack)")
	ErrTimestampInFuture      = errors.New("timestamp is in the future beyond tolerance window (clock skew)")
)

// ComputeHMAC computes the raw HMAC-SHA256 bytes for (timestamp + "." + body).
func ComputeHMAC(secret string, timestamp int64, body []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return mac.Sum(nil)
}

// Sign generates the full signature string formatted as "sha256=<hex_hmac>".
func Sign(secret string, timestamp int64, body []byte) string {
	macBytes := ComputeHMAC(secret, timestamp, body)
	return SignaturePrefix + hex.EncodeToString(macBytes)
}

// Verify validates that the signature matches the secret, timestamp, and raw body.
// It uses hmac.Equal for constant-time comparison to prevent timing attacks,
// and enforces a tolerance window to prevent replay attacks.
func Verify(secret string, timestamp int64, body []byte, signatureHeader string, tolerance time.Duration) error {
	// 1. Validate replay window against current time
	if tolerance > 0 {
		now := time.Now().Unix()
		tolSec := int64(tolerance.Seconds())

		if now-timestamp > tolSec {
			return ErrTimestampTooOld
		}
		if timestamp-now > tolSec {
			return ErrTimestampInFuture
		}
	}

	// 2. Validate signature format
	if !strings.HasPrefix(signatureHeader, SignaturePrefix) {
		return ErrInvalidSignatureHeader
	}

	sigHex := strings.TrimPrefix(signatureHeader, SignaturePrefix)
	providedMAC, err := hex.DecodeString(sigHex)
	if err != nil {
		return ErrInvalidHexSignature
	}

	// 3. Compute expected HMAC
	expectedMAC := ComputeHMAC(secret, timestamp, body)

	// 4. Constant-time comparison to prevent timing side-channel attacks
	if !hmac.Equal(expectedMAC, providedMAC) {
		return ErrInvalidSignature
	}

	return nil
}
