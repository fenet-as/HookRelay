package delivery

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// ClientConfig holds network and connection pool configuration for outbound webhook requests.
type ClientConfig struct {
	Timeout               time.Duration
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	MaxResponseBodyBytes  int64
}

// DefaultClientConfig returns production defaults for delivering webhooks to untrusted external systems.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Timeout:               10 * time.Second,
		DialTimeout:           3 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		MaxResponseBodyBytes:  1024, // 1 KB max response body stored
	}
}

// Client wraps an *http.Client configured with strict timeouts, connection reuse, and no redirect following.
type Client struct {
	httpClient           *http.Client
	maxResponseBodyBytes int64
}

// NewClient constructs an outbound HTTP Client with tuned transport settings.
func NewClient(cfg ClientConfig) *Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
		// Do not automatically follow redirects.
		// Webhook deliveries must target the exact URL registered by the subscriber.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &Client{
		httpClient:           httpClient,
		maxResponseBodyBytes: cfg.MaxResponseBodyBytes,
	}
}

// HTTPClient returns the underlying *http.Client.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

// CloseIdleConnections closes all idle keep-alive connections in the transport pool.
func (c *Client) CloseIdleConnections() {
	if tr, ok := c.httpClient.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}

// SafeReadBody reads up to maxBytes from r and then drains and closes it.
// Draining and closing the body is mandatory in Go to allow TCP connection reuse.
func SafeReadBody(body io.ReadCloser, maxBytes int64) (string, error) {
	if body == nil {
		return "", nil
	}
	defer body.Close()

	limitReader := io.LimitReader(body, maxBytes)
	data, err := io.ReadAll(limitReader)
	if err != nil {
		return "", fmt.Errorf("failed to read limited response body: %w", err)
	}

	// Drain any remaining bytes (up to 32 KB) so the underlying TCP connection can be returned to the pool
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 32*1024))

	return string(data), nil
}
