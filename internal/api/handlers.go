package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/fenet-as/hookrelay/internal/crypto"
	"github.com/fenet-as/hookrelay/internal/fanout"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func isValidURL(rawURL string) bool {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.Host == "" {
		return false
	}
	return true
}

func parseIDParam(c *gin.Context) (int64, bool) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid ID parameter"})
		return 0, false
	}
	return id, true
}

// HealthzHandler returns HTTP 200 when the server is alive.
func (s *Server) HealthzHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// CreateEndpointHandler registers a new webhook endpoint.
func (s *Server) CreateEndpointHandler(c *gin.Context) {
	var req CreateEndpointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body: url is required"})
		return
	}

	if !isValidURL(req.Url) {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid endpoint url: must be http or https with valid host"})
		return
	}

	secret, err := crypto.GenerateSecret()
	if err != nil {
		s.logger.Error("failed to generate secret", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to generate secret"})
		return
	}

	eventTypes := req.EventTypes
	if eventTypes == nil {
		eventTypes = []string{}
	}

	ep, err := s.queries.CreateEndpoint(c.Request.Context(), store.CreateEndpointParams{
		Url:        req.Url,
		Secret:     secret,
		EventTypes: eventTypes,
		Active:     true,
	})
	if err != nil {
		s.logger.Error("failed to create endpoint in store", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to create endpoint"})
		return
	}

	c.JSON(http.StatusCreated, EndpointCreatedResponse{
		EndpointResponse: EndpointResponse{
			ID:         ep.ID,
			Url:        ep.Url,
			EventTypes: ep.EventTypes,
			Active:     ep.Active,
			CreatedAt:  ep.CreatedAt.Time,
		},
		Secret: ep.Secret,
	})
}

// ListEndpointsHandler lists all registered webhook endpoints.
// Secrets are never included in the response.
func (s *Server) ListEndpointsHandler(c *gin.Context) {
	endpoints, err := s.queries.ListEndpoints(c.Request.Context())
	if err != nil {
		s.logger.Error("failed to list endpoints", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list endpoints"})
		return
	}

	res := make([]EndpointResponse, 0, len(endpoints))
	for _, ep := range endpoints {
		res = append(res, EndpointResponse{
			ID:         ep.ID,
			Url:        ep.Url,
			EventTypes: ep.EventTypes,
			Active:     ep.Active,
			CreatedAt:  ep.CreatedAt.Time,
		})
	}

	c.JSON(http.StatusOK, res)
}

// GetEndpointByIDHandler retrieves a single endpoint by ID.
// The secret is omitted from the response.
func (s *Server) GetEndpointByIDHandler(c *gin.Context) {
	id, ok := parseIDParam(c)
	if !ok {
		return
	}

	ep, err := s.queries.GetEndpointByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "endpoint not found"})
			return
		}
		s.logger.Error("failed to get endpoint", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to get endpoint"})
		return
	}

	c.JSON(http.StatusOK, EndpointResponse{
		ID:         ep.ID,
		Url:        ep.Url,
		EventTypes: ep.EventTypes,
		Active:     ep.Active,
		CreatedAt:  ep.CreatedAt.Time,
	})
}

// UpdateEndpointHandler updates an endpoint's URL, subscribed event types, or active status.
func (s *Server) UpdateEndpointHandler(c *gin.Context) {
	id, ok := parseIDParam(c)
	if !ok {
		return
	}

	var req UpdateEndpointRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid update request body"})
		return
	}

	params := store.UpdateEndpointParams{
		ID: id,
	}

	if req.Url != nil {
		if !isValidURL(*req.Url) {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid url: must be http or https with valid host"})
			return
		}
		params.Url = pgtype.Text{String: *req.Url, Valid: true}
	}

	if req.EventTypes != nil {
		params.EventTypes = req.EventTypes
	}

	if req.Active != nil {
		params.Active = pgtype.Bool{Bool: *req.Active, Valid: true}
	}

	updated, err := s.queries.UpdateEndpoint(c.Request.Context(), params)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "endpoint not found"})
			return
		}
		s.logger.Error("failed to update endpoint", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to update endpoint"})
		return
	}

	c.JSON(http.StatusOK, EndpointResponse{
		ID:         updated.ID,
		Url:        updated.Url,
		EventTypes: updated.EventTypes,
		Active:     updated.Active,
		CreatedAt:  updated.CreatedAt.Time,
	})
}

// DeleteEndpointHandler deletes a registered webhook endpoint.
func (s *Server) DeleteEndpointHandler(c *gin.Context) {
	id, ok := parseIDParam(c)
	if !ok {
		return
	}

	// Verify existence first
	_, err := s.queries.GetEndpointByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "endpoint not found"})
			return
		}
		s.logger.Error("failed to check endpoint before delete", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to delete endpoint"})
		return
	}

	if err := s.queries.DeleteEndpoint(c.Request.Context(), id); err != nil {
		s.logger.Error("failed to delete endpoint", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to delete endpoint"})
		return
	}

	c.Status(http.StatusNoContent)
}

// RotateSecretHandler generates a new secret for an endpoint and returns it once.
func (s *Server) RotateSecretHandler(c *gin.Context) {
	id, ok := parseIDParam(c)
	if !ok {
		return
	}

	// Verify endpoint exists
	_, err := s.queries.GetEndpointByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "endpoint not found"})
			return
		}
		s.logger.Error("failed to find endpoint for secret rotation", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to rotate secret"})
		return
	}

	newSecret, err := crypto.GenerateSecret()
	if err != nil {
		s.logger.Error("failed to generate new secret for rotation", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to rotate secret"})
		return
	}

	rotated, err := s.queries.RotateEndpointSecret(c.Request.Context(), store.RotateEndpointSecretParams{
		ID:     id,
		Secret: newSecret,
	})
	if err != nil {
		s.logger.Error("failed to store rotated secret", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to rotate secret"})
		return
	}

	c.JSON(http.StatusOK, RotateSecretResponse{
		ID:     rotated.ID,
		Secret: rotated.Secret,
	})
}

// PublishEventHandler accepts events, triggers transactional fan-out, and returns 202 Accepted.
func (s *Server) PublishEventHandler(c *gin.Context) {
	var req PublishEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid request body: type is required"})
		return
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = req.Data
	}
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")

	result, err := s.fanout.PublishEvent(c.Request.Context(), fanout.PublishParams{
		Type:           req.Type,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		s.logger.Error("failed to publish event", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to publish event"})
		return
	}

	c.JSON(http.StatusAccepted, PublishEventResponse{
		ID:              result.Event.ID,
		Type:            result.Event.Type,
		DeliveriesCount: len(result.Deliveries),
		IsDuplicate:     result.IsDuplicate,
		CreatedAt:       result.Event.CreatedAt.Time,
	})
}

// GetEventByIDHandler retrieves an event and its associated deliveries.
func (s *Server) GetEventByIDHandler(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "event ID is required"})
		return
	}

	event, err := s.queries.GetEventByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "event not found"})
			return
		}
		s.logger.Error("failed to get event", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to get event"})
		return
	}

	deliveries, err := s.queries.ListDeliveriesByEventID(c.Request.Context(), id)
	if err != nil {
		s.logger.Error("failed to list deliveries for event", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list deliveries"})
		return
	}

	delivResponses := make([]DeliveryResponse, 0, len(deliveries))
	for _, d := range deliveries {
		var lockedAt *time.Time
		if d.LockedAt.Valid {
			t := d.LockedAt.Time
			lockedAt = &t
		}
		delivResponses = append(delivResponses, DeliveryResponse{
			ID:         d.ID,
			EventID:    d.EventID,
			EndpointID: d.EndpointID,
			Status:     string(d.Status),
			Attempts:   d.Attempts,
			NextRunAt:  d.NextRunAt.Time,
			LockedAt:   lockedAt,
			CreatedAt:  d.CreatedAt.Time,
		})
	}

	var idemKey *string
	if event.IdempotencyKey.Valid {
		idemKey = &event.IdempotencyKey.String
	}

	c.JSON(http.StatusOK, EventResponse{
		ID:             event.ID,
		Type:           event.Type,
		Payload:        event.Payload,
		IdempotencyKey: idemKey,
		CreatedAt:      event.CreatedAt.Time,
		Deliveries:     delivResponses,
	})
}

// GetDeliveryByIDHandler retrieves a delivery and all its historical attempts.
func (s *Server) GetDeliveryByIDHandler(c *gin.Context) {
	id, ok := parseIDParam(c)
	if !ok {
		return
	}

	delivery, err := s.queries.GetDeliveryByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "delivery not found"})
			return
		}
		s.logger.Error("failed to get delivery", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to get delivery"})
		return
	}

	attempts, err := s.queries.ListAttemptsByDeliveryID(c.Request.Context(), id)
	if err != nil {
		s.logger.Error("failed to list attempts for delivery", "id", id, "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list attempts"})
		return
	}

	attemptResponses := make([]AttemptResponse, 0, len(attempts))
	for _, a := range attempts {
		var durationMs *int32
		if a.DurationMs.Valid {
			durationMs = &a.DurationMs.Int32
		}
		var statusCode *int32
		if a.StatusCode.Valid {
			statusCode = &a.StatusCode.Int32
		}
		var errorMsg *string
		if a.Error.Valid {
			errorMsg = &a.Error.String
		}
		var respBody *string
		if a.ResponseBody.Valid {
			respBody = &a.ResponseBody.String
		}

		attemptResponses = append(attemptResponses, AttemptResponse{
			ID:           a.ID,
			DeliveryID:   a.DeliveryID,
			Attempt:      a.Attempt,
			StartedAt:    a.StartedAt.Time,
			DurationMs:   durationMs,
			StatusCode:   statusCode,
			Error:        errorMsg,
			ResponseBody: respBody,
		})
	}

	var lockedAt *time.Time
	if delivery.LockedAt.Valid {
		t := delivery.LockedAt.Time
		lockedAt = &t
	}

	c.JSON(http.StatusOK, DeliveryResponse{
		ID:           delivery.ID,
		EventID:      delivery.EventID,
		EndpointID:   delivery.EndpointID,
		Status:       string(delivery.Status),
		Attempts:     delivery.Attempts,
		NextRunAt:    delivery.NextRunAt.Time,
		LockedAt:     lockedAt,
		CreatedAt:    delivery.CreatedAt.Time,
		AttemptsList: attemptResponses,
	})
}
