package api

import "time"

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

// ErrorResponse represents a standardized error envelope.
type ErrorResponse struct {
	Error string `json:"error"`
}
