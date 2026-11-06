package signing_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/signing"
)

func TestSign_KnownVector(t *testing.T) {
	secret := "whsec_test_secret_key"
	timestamp := int64(1700000000)
	body := []byte(`{"order_id": 42, "status": "paid"}`)

	// Independently compute expected
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("1700000000."))
	mac.Write(body)
	expectedHex := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	sig := signing.Sign(secret, timestamp, body)
	if sig != expectedHex {
		t.Fatalf("expected signature %s, got %s", expectedHex, sig)
	}
}

func TestVerify_Success(t *testing.T) {
	secret := "whsec_super_secret"
	now := time.Now().Unix()
	body := []byte(`{"user_id": "usr_123"}`)

	sig := signing.Sign(secret, now, body)

	err := signing.Verify(secret, now, body, sig, 5*time.Minute)
	if err != nil {
		t.Fatalf("expected verification to succeed, got %v", err)
	}
}

func TestVerify_TamperedBody(t *testing.T) {
	secret := "whsec_super_secret"
	now := time.Now().Unix()
	originalBody := []byte(`{"amount": 100}`)
	tamperedBody := []byte(`{"amount": 999999}`)

	sig := signing.Sign(secret, now, originalBody)

	err := signing.Verify(secret, now, tamperedBody, sig, 5*time.Minute)
	if !errors.Is(err, signing.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for tampered body, got %v", err)
	}
}

func TestVerify_TamperedTimestamp(t *testing.T) {
	secret := "whsec_super_secret"
	now := time.Now().Unix()
	body := []byte(`{"action": "deploy"}`)

	sig := signing.Sign(secret, now, body)

	// Tampering the timestamp by 1 second must fail signature check
	err := signing.Verify(secret, now-1, body, sig, 5*time.Minute)
	if !errors.Is(err, signing.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for tampered timestamp, got %v", err)
	}
}

func TestVerify_WrongSecret(t *testing.T) {
	secretA := "whsec_secret_alpha"
	secretB := "whsec_secret_beta"
	now := time.Now().Unix()
	body := []byte(`{"test": true}`)

	sig := signing.Sign(secretA, now, body)

	err := signing.Verify(secretB, now, body, sig, 5*time.Minute)
	if !errors.Is(err, signing.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for wrong secret, got %v", err)
	}
}

func TestVerify_ReplayAttack_ExpiredTimestamp(t *testing.T) {
	secret := "whsec_super_secret"
	// Timestamp 10 minutes in the past
	oldTimestamp := time.Now().Add(-10 * time.Minute).Unix()
	body := []byte(`{"action": "replay_me"}`)

	sig := signing.Sign(secret, oldTimestamp, body)

	// 5 minute tolerance
	err := signing.Verify(secret, oldTimestamp, body, sig, 5*time.Minute)
	if !errors.Is(err, signing.ErrTimestampTooOld) {
		t.Fatalf("expected ErrTimestampTooOld, got %v", err)
	}
}

func TestVerify_ClockSkew_FutureTimestamp(t *testing.T) {
	secret := "whsec_super_secret"
	// Timestamp 10 minutes in the future
	futureTimestamp := time.Now().Add(10 * time.Minute).Unix()
	body := []byte(`{"action": "future"}`)

	sig := signing.Sign(secret, futureTimestamp, body)

	err := signing.Verify(secret, futureTimestamp, body, sig, 5*time.Minute)
	if !errors.Is(err, signing.ErrTimestampInFuture) {
		t.Fatalf("expected ErrTimestampInFuture, got %v", err)
	}
}

func TestVerify_InvalidSignatureFormat(t *testing.T) {
	secret := "whsec_super_secret"
	now := time.Now().Unix()
	body := []byte(`{"hello": "world"}`)

	// Missing sha256= prefix
	err := signing.Verify(secret, now, body, "invalid_prefix", 5*time.Minute)
	if !errors.Is(err, signing.ErrInvalidSignatureHeader) {
		t.Fatalf("expected ErrInvalidSignatureHeader, got %v", err)
	}

	// Invalid hex characters
	err = signing.Verify(secret, now, body, "sha256=not_valid_hex_characters!@#$", 5*time.Minute)
	if !errors.Is(err, signing.ErrInvalidHexSignature) {
		t.Fatalf("expected ErrInvalidHexSignature, got %v", err)
	}
}

func TestVerify_ZeroTolerance_SkipsTimestampCheck(t *testing.T) {
	secret := "whsec_super_secret"
	// Old timestamp
	oldTimestamp := time.Now().Add(-100 * time.Hour).Unix()
	body := []byte(`{"historical": true}`)

	sig := signing.Sign(secret, oldTimestamp, body)

	// tolerance = 0 disables time window check
	err := signing.Verify(secret, oldTimestamp, body, sig, 0)
	if err != nil {
		t.Fatalf("expected zero tolerance to skip timestamp check, got %v", err)
	}
}
