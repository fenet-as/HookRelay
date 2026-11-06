package delivery

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/fenet-as/hookrelay/internal/signing"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

// DeliveryRequest encapsulates the inputs required to deliver an event to an endpoint.
type DeliveryRequest struct {
	DeliveryID int64
	EventID    string
	EndpointID int64
	URL        string
	Secret     string
	Attempt    int32
	Payload    []byte
}

// DeliveryResult summarizes the outcome of an outbound delivery attempt.
type DeliveryResult struct {
	DeliveryID   int64
	Attempt      int32
	StatusCode   int
	Duration     time.Duration
	Error        string
	ResponseBody string
	Success      bool
}

// Dispatcher executes outbound signed webhook HTTP requests and records attempt history.
type Dispatcher struct {
	client  *Client
	queries *store.Queries
	logger  *slog.Logger
}

// NewDispatcher creates a new delivery Dispatcher.
func NewDispatcher(client *Client, queries *store.Queries, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		client:  client,
		queries: queries,
		logger:  logger,
	}
}

// Execute performs the signed HTTP delivery, bounds response reading, and records the attempt in PostgreSQL.
func (d *Dispatcher) Execute(ctx context.Context, req DeliveryRequest) (*DeliveryResult, error) {
	startedAt := time.Now()
	timestamp := startedAt.Unix()

	// 1. Sign the payload over (timestamp + "." + raw_bytes)
	signature := signing.Sign(req.Secret, timestamp, req.Payload)

	// 2. Build HTTP request with caller context
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.URL, bytes.NewReader(req.Payload))
	if err != nil {
		d.logger.Error("failed to construct HTTP request", "error", err, "delivery_id", req.DeliveryID)
		return nil, err
	}

	// 3. Attach standard HookRelay delivery contract headers
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(signing.HeaderEventID, req.EventID)
	httpReq.Header.Set(signing.HeaderDeliveryID, strconv.FormatInt(req.DeliveryID, 10))
	httpReq.Header.Set(signing.HeaderAttempt, strconv.FormatInt(int64(req.Attempt), 10))
	httpReq.Header.Set(signing.HeaderTimestamp, strconv.FormatInt(timestamp, 10))
	httpReq.Header.Set(signing.HeaderSignature, signature)
	httpReq.Header.Set("User-Agent", "HookRelay/1.0")

	// 4. Execute outbound HTTP POST
	resp, doErr := d.client.httpClient.Do(httpReq)
	duration := time.Since(startedAt)
	durationMs := int32(duration.Milliseconds())

	var statusCode int
	var errorMsg string
	var responseBody string
	var success bool

	if doErr != nil {
		errorMsg = doErr.Error()
		d.logger.Warn("outbound delivery attempt failed with network error",
			slog.Int64("delivery_id", req.DeliveryID),
			slog.Int("attempt", int(req.Attempt)),
			slog.Duration("duration", duration),
			slog.String("error", errorMsg),
		)
	} else {
		statusCode = resp.StatusCode
		success = statusCode >= 200 && statusCode < 300

		// Safely read, drain, and close response body
		bodyStr, readErr := SafeReadBody(resp.Body, d.client.maxResponseBodyBytes)
		if readErr != nil {
			d.logger.Warn("failed to read response body", "error", readErr)
		}
		responseBody = bodyStr

		d.logger.Info("outbound delivery attempt finished",
			slog.Int64("delivery_id", req.DeliveryID),
			slog.Int("attempt", int(req.Attempt)),
			slog.Int("status_code", statusCode),
			slog.Duration("duration", duration),
			slog.Bool("success", success),
		)
	}

	// 5. Record attempt in PostgreSQL delivery_attempts table
	var statusVal pgtype.Int4
	if statusCode > 0 {
		statusVal = pgtype.Int4{Int32: int32(statusCode), Valid: true}
	}

	var errVal pgtype.Text
	if errorMsg != "" {
		errVal = pgtype.Text{String: errorMsg, Valid: true}
	}

	var bodyVal pgtype.Text
	if responseBody != "" {
		bodyVal = pgtype.Text{String: responseBody, Valid: true}
	}

	_, recordErr := d.queries.CreateDeliveryAttempt(ctx, store.CreateDeliveryAttemptParams{
		DeliveryID:   req.DeliveryID,
		Attempt:      req.Attempt,
		StartedAt:    pgtype.Timestamptz{Time: startedAt, Valid: true},
		DurationMs:   pgtype.Int4{Int32: durationMs, Valid: true},
		StatusCode:   statusVal,
		Error:        errVal,
		ResponseBody: bodyVal,
	})
	if recordErr != nil {
		d.logger.Error("failed to record delivery attempt in store",
			slog.Int64("delivery_id", req.DeliveryID),
			slog.Any("error", recordErr),
		)
	}

	return &DeliveryResult{
		DeliveryID:   req.DeliveryID,
		Attempt:      req.Attempt,
		StatusCode:   statusCode,
		Duration:     duration,
		Error:        errorMsg,
		ResponseBody: responseBody,
		Success:      success,
	}, nil
}
