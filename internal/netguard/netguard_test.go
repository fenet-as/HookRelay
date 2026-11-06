package netguard_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/netguard"
)

func TestIsRestrictedIP(t *testing.T) {
	testCases := []struct {
		ip       string
		expected bool
	}{
		// Loopback
		{"127.0.0.1", true},
		{"127.0.0.2", true},
		{"::1", true},

		// Cloud Metadata (AWS, GCP, Azure, DigitalOcean)
		{"169.254.169.254", true},
		{"169.254.1.1", true},

		// RFC 1918 Private networks
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"172.16.0.1", true},
		{"172.31.255.255", true},
		{"192.168.0.1", true},
		{"192.168.1.100", true},

		// IPv6 Unique Local & Link Local
		{"fc00::1", true},
		{"fe80::1", true},

		// Unspecified
		{"0.0.0.0", true},
		{"::", true},

		// Public internet IPs (Safe)
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"93.184.216.34", false},
		{"2606:4700:4700::1111", false},
	}

	for _, tc := range testCases {
		t.Run(tc.ip, func(t *testing.T) {
			parsed := net.ParseIP(tc.ip)
			if parsed == nil {
				t.Fatalf("failed to parse IP: %s", tc.ip)
			}
			result := netguard.IsRestrictedIP(parsed)
			if result != tc.expected {
				t.Errorf("IsRestrictedIP(%s) = %v; expected %v", tc.ip, result, tc.expected)
			}
		})
	}
}

func TestDialer_BlocksLocalAddressesInSecureMode(t *testing.T) {
	dialer := netguard.NewDialer(netguard.Config{
		AllowLocal:  false,
		DialTimeout: 1 * time.Second,
	})

	ctx := context.Background()

	// 1. Dialing 127.0.0.1 must be blocked
	_, err := dialer.DialContext(ctx, "tcp", "127.0.0.1:8080")
	if !errors.Is(err, netguard.ErrBlockedIPAddress) {
		t.Fatalf("expected ErrBlockedIPAddress for 127.0.0.1, got: %v", err)
	}

	// 2. Dialing cloud metadata IP 169.254.169.254 must be blocked
	_, err = dialer.DialContext(ctx, "tcp", "169.254.169.254:80")
	if !errors.Is(err, netguard.ErrBlockedIPAddress) {
		t.Fatalf("expected ErrBlockedIPAddress for cloud metadata address, got: %v", err)
	}

	// 3. Dialing localhost hostname must be blocked
	_, err = dialer.DialContext(ctx, "tcp", "localhost:8080")
	if !errors.Is(err, netguard.ErrBlockedIPAddress) {
		t.Fatalf("expected ErrBlockedIPAddress for localhost, got: %v", err)
	}
}

func TestDialer_AllowsLocalAddressesInDevMode(t *testing.T) {
	// Start local test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dialer := netguard.NewDialer(netguard.Config{
		AllowLocal:  true, // Dev mode enabled
		DialTimeout: 1 * time.Second,
	})

	ctx := context.Background()
	conn, err := dialer.DialContext(ctx, "tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("expected connection to succeed in dev mode, got: %v", err)
	}
	defer conn.Close()
}
