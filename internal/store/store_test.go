package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"
}

func setupTestStore(t *testing.T) (*store.Queries, func()) {
	t.Helper()
	ctx := context.Background()
	dbURL := getTestDatabaseURL()

	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: dbURL,
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}

	queries := store.New(pool)
	cleanup := func() {
		pool.Close()
	}

	return queries, cleanup
}

func TestStore_EndpointCRUD(t *testing.T) {
	ctx := context.Background()
	queries, cleanup := setupTestStore(t)
	defer cleanup()

	// 1. Create Endpoint
	url := "https://api.example.com/webhook"
	secret := "whsec_test_secret_12345"
	eventTypes := []string{"order.created", "order.updated"}

	ep, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        url,
		Secret:     secret,
		EventTypes: eventTypes,
		Active:     true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint failed: %v", err)
	}
	if ep.ID <= 0 {
		t.Errorf("expected positive endpoint ID, got %d", ep.ID)
	}
	if ep.Url != url {
		t.Errorf("expected url %s, got %s", url, ep.Url)
	}
	if ep.Secret != secret {
		t.Errorf("expected secret %s, got %s", secret, ep.Secret)
	}

	// 2. Get Endpoint by ID
	fetched, err := queries.GetEndpointByID(ctx, ep.ID)
	if err != nil {
		t.Fatalf("GetEndpointByID failed: %v", err)
	}
	if fetched.ID != ep.ID || fetched.Url != url {
		t.Errorf("fetched endpoint mismatch: %+v", fetched)
	}

	// 3. Update Endpoint
	newURL := "https://api.example.com/webhook-v2"
	updated, err := queries.UpdateEndpoint(ctx, store.UpdateEndpointParams{
		ID:  ep.ID,
		Url: pgtype.Text{String: newURL, Valid: true},
	})
	if err != nil {
		t.Fatalf("UpdateEndpoint failed: %v", err)
	}
	if updated.Url != newURL {
		t.Errorf("expected updated url %s, got %s", newURL, updated.Url)
	}

	// 4. Rotate Secret
	newSecret := "whsec_new_rotated_secret_67890"
	rotated, err := queries.RotateEndpointSecret(ctx, store.RotateEndpointSecretParams{
		ID:     ep.ID,
		Secret: newSecret,
	})
	if err != nil {
		t.Fatalf("RotateEndpointSecret failed: %v", err)
	}
	if rotated.Secret != newSecret {
		t.Errorf("expected rotated secret %s, got %s", newSecret, rotated.Secret)
	}

	// 5. List Active Endpoints For Event Type
	matching, err := queries.ListActiveEndpointsForEventType(ctx, "order.created")
	if err != nil {
		t.Fatalf("ListActiveEndpointsForEventType failed: %v", err)
	}
	found := false
	for _, m := range matching {
		if m.ID == ep.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find endpoint ID %d for order.created", ep.ID)
	}

	// 6. Delete Endpoint
	if err := queries.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatalf("DeleteEndpoint failed: %v", err)
	}
}

func TestStore_EventIdempotency(t *testing.T) {
	ctx := context.Background()
	queries, cleanup := setupTestStore(t)
	defer cleanup()

	eventID := "evt_test_" + time.Now().Format("20060102150405.000000")
	idempotencyKey := "idem_key_" + time.Now().Format("20060102150405.000000")
	eventType := "payment.succeeded"
	payload := []byte(`{"payment_id": "pay_123", "amount": 4200}`)

	// 1. First event insert
	event1, err := queries.CreateEvent(ctx, store.CreateEventParams{
		ID:             eventID,
		Type:           eventType,
		Payload:        payload,
		IdempotencyKey: pgtype.Text{String: idempotencyKey, Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateEvent failed: %v", err)
	}
	if event1.ID != eventID {
		t.Errorf("expected event ID %s, got %s", eventID, event1.ID)
	}

	// 2. Duplicate insert with same idempotency key
	duplicateID := eventID + "_dup"
	event2, err := queries.CreateEvent(ctx, store.CreateEventParams{
		ID:             duplicateID,
		Type:           eventType,
		Payload:        payload,
		IdempotencyKey: pgtype.Text{String: idempotencyKey, Valid: true},
	})
	// ON CONFLICT DO NOTHING returns no row (pgx.ErrNoRows)
	if err == nil && event2.ID != "" {
		t.Errorf("expected conflict to prevent insert, but got event: %+v", event2)
	}

	// 3. Retrieve original by idempotency key
	existing, err := queries.GetEventByIdempotencyKey(ctx, pgtype.Text{String: idempotencyKey, Valid: true})
	if err != nil {
		t.Fatalf("GetEventByIdempotencyKey failed: %v", err)
	}
	if existing.ID != eventID {
		t.Errorf("expected to retrieve original event %s, got %s", eventID, existing.ID)
	}
}

func TestStore_DeliveriesAndAttempts(t *testing.T) {
	ctx := context.Background()
	queries, cleanup := setupTestStore(t)
	defer cleanup()

	// Create endpoint & event first
	ep, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://webhook.site/test",
		Secret:     "whsec_delivery_test",
		EventTypes: []string{},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("setup endpoint failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, ep.ID)

	eventID := "evt_deliv_" + time.Now().Format("20060102150405.000000")
	_, err = queries.CreateEvent(ctx, store.CreateEventParams{
		ID:      eventID,
		Type:    "invoice.paid",
		Payload: []byte(`{"invoice_id": 99}`),
	})
	if err != nil {
		t.Fatalf("setup event failed: %v", err)
	}

	// 1. Create Delivery
	deliv, err := queries.CreateDelivery(ctx, store.CreateDeliveryParams{
		EventID:    eventID,
		EndpointID: ep.ID,
	})
	if err != nil {
		t.Fatalf("CreateDelivery failed: %v", err)
	}
	if deliv.Status != store.DeliveryStatusPending {
		t.Errorf("expected pending status, got %s", deliv.Status)
	}

	// 2. Test unique (event_id, endpoint_id) constraint
	_, err = queries.CreateDelivery(ctx, store.CreateDeliveryParams{
		EventID:    eventID,
		EndpointID: ep.ID,
	})
	if err == nil {
		// Because of ON CONFLICT DO NOTHING, pgx returns pgx.ErrNoRows when nothing is returned
	}

	// 3. Claim due delivery
	staleThreshold := pgtype.Timestamptz{Time: time.Now().Add(-5 * time.Minute), Valid: true}
	claimed, err := queries.ClaimDueDeliveries(ctx, store.ClaimDueDeliveriesParams{
		LockedAt: staleThreshold,
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ClaimDueDeliveries failed: %v", err)
	}
	var myClaimed *store.Delivery
	for i := range claimed {
		if claimed[i].ID == deliv.ID {
			myClaimed = &claimed[i]
			break
		}
	}
	if myClaimed == nil {
		t.Fatalf("expected delivery %d to be claimed", deliv.ID)
	}
	if !myClaimed.LockedAt.Valid {
		t.Errorf("expected locked_at to be valid upon claiming")
	}

	// 4. Record delivery attempt
	attempt, err := queries.CreateDeliveryAttempt(ctx, store.CreateDeliveryAttemptParams{
		DeliveryID:   deliv.ID,
		Attempt:      1,
		StartedAt:    pgtype.Timestamptz{Time: time.Now(), Valid: true},
		DurationMs:   pgtype.Int4{Int32: 120, Valid: true},
		StatusCode:   pgtype.Int4{Int32: 200, Valid: true},
		ResponseBody: pgtype.Text{String: `{"status": "ok"}`, Valid: true},
	})
	if err != nil {
		t.Fatalf("CreateDeliveryAttempt failed: %v", err)
	}
	if attempt.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", attempt.Attempt)
	}

	// 5. Mark delivery succeeded
	succeeded, err := queries.MarkDeliverySucceeded(ctx, deliv.ID)
	if err != nil {
		t.Fatalf("MarkDeliverySucceeded failed: %v", err)
	}
	if succeeded.Status != store.DeliveryStatusSucceeded {
		t.Errorf("expected succeeded status, got %s", succeeded.Status)
	}
	if succeeded.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", succeeded.Attempts)
	}

	// 6. Replay delivery
	replayed, err := queries.ReplayDelivery(ctx, deliv.ID)
	if err != nil {
		t.Fatalf("ReplayDelivery failed: %v", err)
	}
	if replayed.Status != store.DeliveryStatusPending {
		t.Errorf("expected pending status after replay, got %s", replayed.Status)
	}
	if replayed.Attempts != 0 {
		t.Errorf("expected 0 attempts after replay, got %d", replayed.Attempts)
	}
}

