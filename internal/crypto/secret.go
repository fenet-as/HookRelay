package crypto

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const (
	SecretPrefix     = "whsec_"
	SecretRandomBytes = 24
)

// GenerateSecret produces a cryptographically secure random secret token
// formatted with the standard webhook secret prefix: whsec_<48-hex-chars>.
func GenerateSecret() (string, error) {
	b := make([]byte, SecretRandomBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random secret bytes: %w", err)
	}
	return SecretPrefix + hex.EncodeToString(b), nil
}
