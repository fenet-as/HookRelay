package delivery

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/fenet-as/hookrelay/internal/telemetry"
	"github.com/jackc/pgx/v5/pgtype"
)

// PoolConfig holds worker pool concurrency and polling parameters.
type PoolConfig struct {
	Workers                     int
	PollInterval                time.Duration
	BatchSize                   int32
	LockStaleThreshold          time.Duration
	RetryPolicy                 RetryPolicy
	ConsecutiveFailureThreshold int32
}

// DefaultPoolConfig returns standard production defaults for background delivery workers.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Workers:                     5,
		PollInterval:                500 * time.Millisecond,
		BatchSize:                   20,
		LockStaleThreshold:          5 * time.Minute,
		RetryPolicy:                 DefaultRetryPolicy(),
		ConsecutiveFailureThreshold: 20,
	}
}

// Pool coordinates background workers that claim due deliveries and execute them.
type Pool struct {
	config     PoolConfig
	dispatcher *Dispatcher
	queries    *store.Queries
	logger     *slog.Logger
	metrics    *telemetry.Metrics
	workChan   chan store.GetDeliveryWithDetailsRow
	wg         sync.WaitGroup
	cancel     context.CancelFunc
}

// NewPool creates a new worker Pool instance.
func NewPool(cfg PoolConfig, dispatcher *Dispatcher, queries *store.Queries, logger *slog.Logger, metrics ...*telemetry.Metrics) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.LockStaleThreshold <= 0 {
		cfg.LockStaleThreshold = 5 * time.Minute
	}

	var m *telemetry.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}

	return &Pool{
		config:     cfg,
		dispatcher: dispatcher,
		queries:    queries,
		logger:     logger,
		metrics:    m,
		workChan:   make(chan store.GetDeliveryWithDetailsRow, cfg.Workers*2),
	}
}

// Start spawns the poller goroutine and the pool of worker goroutines.
func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)

	// Spawn worker goroutines
	for i := 1; i <= p.config.Workers; i++ {
		p.wg.Add(1)
		go p.workerLoop(ctx, i)
	}

	// Spawn poller goroutine
	p.wg.Add(1)
	go p.pollerLoop(ctx)

	p.logger.Info("delivery worker pool started",
		slog.Int("workers", p.config.Workers),
		slog.Duration("poll_interval", p.config.PollInterval),
		slog.Int("batch_size", int(p.config.BatchSize)),
	)
}

// Stop initiates graceful termination: stops polling, drains the channel, and waits for workers.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	p.logger.Info("delivery worker pool stopped cleanly")
}

// pollerLoop periodically queries PostgreSQL for due deliveries using FOR UPDATE SKIP LOCKED.
func (p *Pool) pollerLoop(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(p.workChan)
			return
		case <-ticker.C:
			p.pollDueDeliveries(ctx)
		}
	}
}

func (p *Pool) pollDueDeliveries(ctx context.Context) {
	staleTime := time.Now().Add(-p.config.LockStaleThreshold)
	claimed, err := p.queries.ClaimDueDeliveries(ctx, store.ClaimDueDeliveriesParams{
		LockedAt: pgtype.Timestamptz{Time: staleTime, Valid: true},
		Limit:    p.config.BatchSize,
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			p.logger.Error("failed to claim due deliveries from store", "error", err)
		}
		return
	}

	for _, c := range claimed {
		details, err := p.queries.GetDeliveryWithDetails(ctx, c.ID)
		if err != nil {
			p.logger.Error("failed to load delivery details", "delivery_id", c.ID, "error", err)
			continue
		}

		// If endpoint was deactivated in the meantime, terminate delivery immediately
		if !details.EndpointActive {
			p.logger.Info("endpoint is inactive, marking delivery dead", "delivery_id", c.ID)
			_, _ = p.queries.MarkDeliveryDead(ctx, c.ID)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case p.workChan <- details:
		}
	}
}

// workerLoop consumes claimed deliveries from workChan and executes HTTP deliveries.
func (p *Pool) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()

	for item := range p.workChan {
		p.processDelivery(ctx, item)
	}
}

func (p *Pool) processDelivery(ctx context.Context, item store.GetDeliveryWithDetailsRow) {
	nextAttempt := item.Attempts + 1

	result, err := p.dispatcher.Execute(ctx, DeliveryRequest{
		DeliveryID: item.DeliveryID,
		EventID:    item.EventID,
		EndpointID: item.EndpointID,
		URL:        item.EndpointUrl,
		Secret:     item.EndpointSecret,
		Attempt:    nextAttempt,
		Payload:    item.EventPayload,
	})
	if err != nil {
		p.logger.Error("dispatcher failed to execute delivery", "delivery_id", item.DeliveryID, "error", err)
		return
	}

	durationSec := result.Duration.Seconds()
	statusStr := strconv.Itoa(result.StatusCode)
	if result.StatusCode == 0 {
		statusStr = "0"
	}

	// 1. Success (2xx)
	if result.Success {
		if p.metrics != nil {
			p.metrics.DeliveryAttempts.WithLabelValues("success", statusStr).Inc()
			p.metrics.DeliveryDuration.WithLabelValues("success").Observe(durationSec)
		}
		if _, err := p.queries.MarkDeliverySucceeded(ctx, item.DeliveryID); err != nil {
			p.logger.Error("failed to mark delivery succeeded", "delivery_id", item.DeliveryID, "error", err)
		}
		return
	}

	// 2. Failure: Determine if error is transient / retryable
	var netErr error
	if result.Error != "" {
		netErr = errors.New(result.Error)
	}

	if IsRetryable(result.StatusCode, netErr) {
		if nextAttempt >= p.config.RetryPolicy.MaxAttempts {
			p.logger.Warn("delivery exceeded maximum retry attempts, marking dead",
				slog.Int64("delivery_id", item.DeliveryID),
				slog.Int("attempts", int(nextAttempt)),
			)
			if p.metrics != nil {
				p.metrics.DeliveryAttempts.WithLabelValues("dead", statusStr).Inc()
				p.metrics.DeliveryDuration.WithLabelValues("dead").Observe(durationSec)
			}
			if _, err := p.queries.MarkDeliveryDead(ctx, item.DeliveryID); err != nil {
				p.logger.Error("failed to mark delivery dead", "delivery_id", item.DeliveryID, "error", err)
			}
		} else {
			backoff := CalculateBackoff(nextAttempt, p.config.RetryPolicy)
			nextRunAt := time.Now().Add(backoff)
			p.logger.Info("scheduling delivery retry with backoff",
				slog.Int64("delivery_id", item.DeliveryID),
				slog.Int("attempt", int(nextAttempt)),
				slog.Duration("backoff", backoff),
			)
			if p.metrics != nil {
				p.metrics.DeliveryAttempts.WithLabelValues("retry", statusStr).Inc()
				p.metrics.DeliveryDuration.WithLabelValues("retry").Observe(durationSec)
			}
			_, err := p.queries.ScheduleDeliveryRetry(ctx, store.ScheduleDeliveryRetryParams{
				ID:        item.DeliveryID,
				NextRunAt: pgtype.Timestamptz{Time: nextRunAt, Valid: true},
			})
			if err != nil {
				p.logger.Error("failed to schedule delivery retry", "delivery_id", item.DeliveryID, "error", err)
			}
		}
	} else {
		// Non-retryable client error (4xx) -> Permanent failure
		p.logger.Info("delivery failed with non-retryable status, marking dead",
			slog.Int64("delivery_id", item.DeliveryID),
			slog.Int("status_code", result.StatusCode),
		)
		if p.metrics != nil {
			p.metrics.DeliveryAttempts.WithLabelValues("dead", statusStr).Inc()
			p.metrics.DeliveryDuration.WithLabelValues("dead").Observe(durationSec)
		}
		if _, err := p.queries.MarkDeliveryDead(ctx, item.DeliveryID); err != nil {
			p.logger.Error("failed to mark delivery dead", "delivery_id", item.DeliveryID, "error", err)
		}
	}

	// 3. Track consecutive failures and auto-disable if threshold exceeded
	p.checkEndpointFailureThreshold(ctx, item.EndpointID)
}

func (p *Pool) checkEndpointFailureThreshold(ctx context.Context, endpointID int64) {
	if p.config.ConsecutiveFailureThreshold <= 0 {
		return
	}

	threshold := p.config.ConsecutiveFailureThreshold
	attempts, err := p.queries.GetRecentAttemptsForEndpoint(ctx, store.GetRecentAttemptsForEndpointParams{
		EndpointID: endpointID,
		Limit:      threshold,
	})
	if err != nil {
		p.logger.Error("failed to check recent attempts for endpoint", "endpoint_id", endpointID, "error", err)
		return
	}

	if int32(len(attempts)) < threshold {
		return
	}

	// Check if every attempt failed (status code < 200 or >= 300 or error is set)
	allFailed := true
	for _, a := range attempts {
		if a.StatusCode.Valid && a.StatusCode.Int32 >= 200 && a.StatusCode.Int32 < 300 {
			allFailed = false
			break
		}
	}

	if allFailed {
		p.logger.Warn("endpoint exceeded consecutive failure threshold, auto-disabling",
			slog.Int64("endpoint_id", endpointID),
			slog.Int("threshold", int(threshold)),
		)
		if p.metrics != nil {
			p.metrics.EndpointsDeactivated.Inc()
		}
		if err := p.queries.DeactivateEndpoint(ctx, endpointID); err != nil {
			p.logger.Error("failed to auto-disable endpoint", "endpoint_id", endpointID, "error", err)
		}
	}
}
