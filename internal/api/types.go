package api

import (
	"encoding/json"
	"time"
)

// CreateEndpointRequest defines the payload for registering a new webhook endpoint.
type CreateEndpointRequest struct {
	Url        string   `json:"url" binding:"required"`
	EventTypes []string `json:"event_types"`
}

// UpdateEndpointRequest defines the payload for updating an existing endpoint.
type UpdateEndpointRequest struct {
	Url        *string  `json:"url"`
	EventTypes []string `json:"event_types"`
	Active     *bool    `json:"active"`
}

// EndpointResponse represents an endpoint returned by GET and PATCH APIs.
// The signing secret is intentionally omitted to maintain credential privacy.
type EndpointResponse struct {
	ID         int64     `json:"id"`
	Url        string    `json:"url"`
	EventTypes []string  `json:"event_types"`
	Active     bool      `json:"active"`
	CreatedAt  time.Time `json:"created_at"`
}

// EndpointCreatedResponse includes the generated secret, returned strictly once upon creation.
type EndpointCreatedResponse struct {
	EndpointResponse
	Secret string `json:"secret"`
}

// RotateSecretResponse returns the newly generated secret upon rotation.
type RotateSecretResponse struct {
	ID     int64  `json:"id"`
	Secret string `json:"secret"`
}

// PublishEventRequest defines the ingest payload for POST /events.
type PublishEventRequest struct {
	Type    string          `json:"type" binding:"required"`
	Data    json.RawMessage `json:"data"`
	Payload json.RawMessage `json:"payload"`
}

// PublishEventResponse represents the immediate 202 Accepted response.
type PublishEventResponse struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	DeliveriesCount int       `json:"deliveries_count"`
	IsDuplicate     bool      `json:"is_duplicate"`
	CreatedAt       time.Time `json:"created_at"`
}

// AttemptResponse represents a single execution try for a delivery.
type AttemptResponse struct {
	ID           int64     `json:"id"`
	DeliveryID   int64     `json:"delivery_id"`
	Attempt      int32     `json:"attempt"`
	StartedAt    time.Time `json:"started_at"`
	DurationMs   *int32    `json:"duration_ms,omitempty"`
	StatusCode   *int32    `json:"status_code,omitempty"`
	Error        *string   `json:"error,omitempty"`
	ResponseBody *string   `json:"response_body,omitempty"`
}

// DeliveryResponse represents a delivery task and its historical attempts.
type DeliveryResponse struct {
	ID           int64             `json:"id"`
	EventID      string            `json:"event_id"`
	EndpointID   int64             `json:"endpoint_id"`
	Status       string            `json:"status"`
	Attempts     int32             `json:"attempts"`
	NextRunAt    time.Time         `json:"next_run_at"`
	LockedAt     *time.Time        `json:"locked_at,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	AttemptsList []AttemptResponse `json:"attempts_history,omitempty"`
}

// EventResponse represents an event with all its created deliveries.
type EventResponse struct {
	ID             string             `json:"id"`
	Type           string             `json:"type"`
	Payload        json.RawMessage    `json:"payload"`
	IdempotencyKey *string            `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time          `json:"created_at"`
	Deliveries     []DeliveryResponse `json:"deliveries"`
}

// ErrorResponse represents a standardized error envelope.
type ErrorResponse struct {
	Error string `json:"error"`
}
