package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"
)

var (
	ErrBlockedIPAddress = errors.New("connection blocked: target resolves to restricted internal, private, or loopback address (SSRF protection)")
)

// Config configures the SSRF-safe dialer.
type Config struct {
	AllowLocal  bool          // If true, allows loopback/private addresses (for local development and testing)
	DialTimeout time.Duration // Timeout for establishing the TCP connection
}

// BlockedPrefixes contains standard CIDR ranges for private, loopback, link-local, and cloud metadata networks.
var blockedPrefixes = []netip.Prefix{
	// IPv4 loopback & unspecified
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("0.0.0.0/8"),

	// IPv4 RFC 1918 private networks
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),

	// IPv4 Link-local & Cloud Metadata (AWS/GCP/Azure: 169.254.169.254)
	netip.MustParsePrefix("169.254.0.0/16"),

	// IPv6 loopback, unspecified, and private (ULA, Link-local)
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
}

// IsRestrictedIP returns true if the IP belongs to a private, loopback, or link-local range.
func IsRestrictedIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true // Reject invalid IP representations
	}

	// Unwrap IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1)
	addr = addr.Unmap()

	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsUnspecified() {
		return true
	}

	for _, prefix := range blockedPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}

	return false
}

// Dialer provides an SSRF-safe DialContext implementation.
type Dialer struct {
	config Config
	netDialer *net.Dialer
}

// NewDialer constructs an SSRF-safe dialer.
func NewDialer(cfg Config) *Dialer {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	return &Dialer{
		config: cfg,
		netDialer: &net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 30 * time.Second,
		},
	}
}

// DialContext resolves the target host and verifies at dial-time that none of the resolved IPs
// point to restricted addresses before establishing the socket connection.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address format: %w", err)
	}

	// If local addresses are allowed (dev mode), bypass SSRF check
	if d.config.AllowLocal {
		return d.netDialer.DialContext(ctx, network, address)
	}

	// Resolve IP addresses at connection time to guard against DNS rebinding attacks
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve host %q: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no IP addresses resolved for host %q", host)
	}

	// Verify EVERY resolved IP address
	for _, ip := range ips {
		if IsRestrictedIP(ip) {
			return nil, fmt.Errorf("%w: host %q resolved to restricted address %s", ErrBlockedIPAddress, host, ip.String())
		}
	}

	// Dial the first verified safe IP
	targetAddr := net.JoinHostPort(ips[0].String(), port)
	return d.netDialer.DialContext(ctx, network, targetAddr)
}
