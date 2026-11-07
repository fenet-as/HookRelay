package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/fenet-as/hookrelay/internal/telemetry"
	"github.com/jackc/pgx/v5/pgtype"
)

// ReaperConfig configures periodic unlocking of abandoned deliveries from crashed worker processes.
type ReaperConfig struct {
	Interval       time.Duration // Frequency of reaper scans
	StaleThreshold time.Duration // Duration after which a locked delivery is deemed abandoned
}

// DefaultReaperConfig returns standard defaults: scans every 30s for deliveries locked > 5m.
func DefaultReaperConfig() ReaperConfig {
	return ReaperConfig{
		Interval:       30 * time.Second,
		StaleThreshold: 5 * time.Minute,
	}
}

// Reaper periodically finds pending deliveries stuck with expired locked_at timestamps and releases them.
type Reaper struct {
	config  ReaperConfig
	queries *store.Queries
	logger  *slog.Logger
	metrics *telemetry.Metrics
}

// NewReaper constructs a new crash recovery Reaper.
func NewReaper(cfg ReaperConfig, queries *store.Queries, logger *slog.Logger, metrics ...*telemetry.Metrics) *Reaper {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.StaleThreshold <= 0 {
		cfg.StaleThreshold = 5 * time.Minute
	}

	var m *telemetry.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}

	return &Reaper{
		config:  cfg,
		queries: queries,
		logger:  logger,
		metrics: m,
	}
}

// Start begins the background reaper loop until ctx is canceled.
func (r *Reaper) Start(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Reap(ctx)
		}
	}
}

// Reap runs a single recovery scan, returning the count of unlocked deliveries.
func (r *Reaper) Reap(ctx context.Context) int {
	staleThreshold := time.Now().Add(-r.config.StaleThreshold)
	unlocked, err := r.queries.UnlockStuckDeliveries(ctx, pgtype.Timestamptz{
		Time:  staleThreshold,
		Valid: true,
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			r.logger.Error("failed to unlock stuck deliveries", "error", err)
		}
		return 0
	}

	count := len(unlocked)
	if count > 0 {
		if r.metrics != nil {
			r.metrics.ReaperRecovered.Add(float64(count))
		}
		r.logger.Warn("reaper recovered stuck deliveries from crashed workers",
			slog.Int("count", count),
			slog.Duration("stale_threshold", r.config.StaleThreshold),
		)
	}
	return count
}
