package crypto_test

import (
	"strings"
	"testing"

	"github.com/fenet-as/hookrelay/internal/crypto"
)

func TestGenerateSecret(t *testing.T) {
	seen := make(map[string]bool)
	const count = 100

	for i := 0; i < count; i++ {
		secret, err := crypto.GenerateSecret()
		if err != nil {
			t.Fatalf("GenerateSecret returned error: %v", err)
		}

		if !strings.HasPrefix(secret, crypto.SecretPrefix) {
			t.Errorf("expected secret to have prefix %s, got %s", crypto.SecretPrefix, secret)
		}

		// Length: 6 ("whsec_") + 48 (24 bytes in hex) = 54
		expectedLen := len(crypto.SecretPrefix) + (crypto.SecretRandomBytes * 2)
		if len(secret) != expectedLen {
			t.Errorf("expected secret length %d, got %d", expectedLen, len(secret))
		}

		if seen[secret] {
			t.Fatalf("generated duplicate secret: %s", secret)
		}
		seen[secret] = true
	}
}
