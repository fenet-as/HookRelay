package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fenet-as/hookrelay/internal/id"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PublishParams contains input data for publishing a new event.
type PublishParams struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey string
}

// PublishResult contains the outcome of an event publication.
type PublishResult struct {
	Event       store.Event
	Deliveries  []store.Delivery
	IsDuplicate bool
}

// Engine coordinates atomic event ingestion and transactional fan-out to endpoints.
type Engine struct {
	pool *pgxpool.Pool
}

// NewEngine creates a new fan-out Engine backed by the database connection pool.
func NewEngine(pool *pgxpool.Pool) *Engine {
	return &Engine{pool: pool}
}

// PublishEvent atomically inserts an event and fans out deliveries to all matching active endpoints.
// If an idempotency key conflict is detected, it returns the original event without creating duplicate deliveries.
func (e *Engine) PublishEvent(ctx context.Context, params PublishParams) (*PublishResult, error) {
	if params.Type == "" {
		return nil, errors.New("event type cannot be empty")
	}
	if len(params.Payload) == 0 {
		params.Payload = json.RawMessage("{}")
	}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := store.New(tx)

	var idemKey pgtype.Text
	if params.IdempotencyKey != "" {
		idemKey = pgtype.Text{String: params.IdempotencyKey, Valid: true}
	}

	eventID := id.NewEventID()

	// 1. Attempt to insert event with ON CONFLICT (idempotency_key) DO NOTHING
	event, err := qtx.CreateEvent(ctx, store.CreateEventParams{
		ID:             eventID,
		Type:           params.Type,
		Payload:        params.Payload,
		IdempotencyKey: idemKey,
	})

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) && idemKey.Valid {
			// Idempotency key already exists: retrieve the original event
			existingEvent, getErr := qtx.GetEventByIdempotencyKey(ctx, idemKey)
			if getErr != nil {
				return nil, fmt.Errorf("failed to retrieve original event on idempotency conflict: %w", getErr)
			}

			existingDeliveries, listErr := qtx.ListDeliveriesByEventID(ctx, existingEvent.ID)
			if listErr != nil {
				return nil, fmt.Errorf("failed to list existing deliveries for idempotent event: %w", listErr)
			}

			if commitErr := tx.Commit(ctx); commitErr != nil {
				return nil, fmt.Errorf("failed to commit read transaction: %w", commitErr)
			}

			return &PublishResult{
				Event:       existingEvent,
				Deliveries:  existingDeliveries,
				IsDuplicate: true,
			}, nil
		}
		return nil, fmt.Errorf("failed to insert event: %w", err)
	}

	// 2. Event successfully created: fan-out to matching active endpoints
	matchingEndpoints, err := qtx.ListActiveEndpointsForEventType(ctx, params.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to query active matching endpoints: %w", err)
	}

	deliveries := make([]store.Delivery, 0, len(matchingEndpoints))
	for _, ep := range matchingEndpoints {
		deliv, err := qtx.CreateDelivery(ctx, store.CreateDeliveryParams{
			EventID:    event.ID,
			EndpointID: ep.ID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// UNIQUE (event_id, endpoint_id) constraint triggered; skip
				continue
			}
			return nil, fmt.Errorf("failed to create delivery for endpoint %d: %w", ep.ID, err)
		}
		deliveries = append(deliveries, deliv)
	}

	// 3. Commit entire fan-out atomically
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit fan-out transaction: %w", err)
	}

	return &PublishResult{
		Event:       event,
		Deliveries:  deliveries,
		IsDuplicate: false,
	}, nil
}
