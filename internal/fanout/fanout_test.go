package fanout_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/fanout"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getTestDatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, *store.Queries, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := store.NewPool(ctx, store.PoolConfig{
		DatabaseURL: getTestDatabaseURL(),
		MaxConns:    5,
	})
	if err != nil {
		t.Fatalf("failed to connect to test db: %v", err)
	}
	queries := store.New(pool)
	cleanup := func() {
		pool.Close()
	}
	return pool, queries, cleanup
}

func TestFanOut_MultipleSubscribersAndWildcards(t *testing.T) {
	ctx := context.Background()
	pool, queries, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Endpoint A: Subscribed to "invoice.paid"
	epA, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://example.com/invoice-handler",
		Secret:     "whsec_a",
		EventTypes: []string{"invoice.paid"},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("create epA failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, epA.ID)

	// 2. Endpoint B: Wildcard (empty event_types array -> receives everything)
	epB, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://example.com/all-events",
		Secret:     "whsec_b",
		EventTypes: []string{},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("create epB failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, epB.ID)

	// 3. Endpoint C: Subscribed to "user.created" (should NOT receive invoice.paid)
	epC, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://example.com/user-handler",
		Secret:     "whsec_c",
		EventTypes: []string{"user.created"},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("create epC failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, epC.ID)

	// 4. Endpoint D: Inactive endpoint subscribed to "invoice.paid" (should NOT receive)
	epD, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://example.com/disabled-handler",
		Secret:     "whsec_d",
		EventTypes: []string{"invoice.paid"},
		Active:     false,
	})
	if err != nil {
		t.Fatalf("create epD failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, epD.ID)

	// Publish "invoice.paid"
	engine := fanout.NewEngine(pool)
	result, err := engine.PublishEvent(ctx, fanout.PublishParams{
		Type:    "invoice.paid",
		Payload: json.RawMessage(`{"invoice_id": 1234, "amount": 9900}`),
	})
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}

	if result.IsDuplicate {
		t.Fatalf("expected new event, not duplicate")
	}

	// Verify deliveries: should deliver to epA and epB, but NOT epC or epD
	deliveredEndpoints := make(map[int64]bool)
	for _, d := range result.Deliveries {
		deliveredEndpoints[d.EndpointID] = true
	}

	if !deliveredEndpoints[epA.ID] {
		t.Errorf("expected delivery for subscribed endpoint A (%d)", epA.ID)
	}
	if !deliveredEndpoints[epB.ID] {
		t.Errorf("expected delivery for wildcard endpoint B (%d)", epB.ID)
	}
	if deliveredEndpoints[epC.ID] {
		t.Errorf("did NOT expect delivery for mismatched endpoint C (%d)", epC.ID)
	}
	if deliveredEndpoints[epD.ID] {
		t.Errorf("did NOT expect delivery for inactive endpoint D (%d)", epD.ID)
	}
}

func TestFanOut_IdempotencyKeyDeduplication(t *testing.T) {
	ctx := context.Background()
	pool, queries, cleanup := setupTestDB(t)
	defer cleanup()

	ep, err := queries.CreateEndpoint(ctx, store.CreateEndpointParams{
		Url:        "https://example.com/idem-test",
		Secret:     "whsec_idem",
		EventTypes: []string{"order.placed"},
		Active:     true,
	})
	if err != nil {
		t.Fatalf("create ep failed: %v", err)
	}
	defer queries.DeleteEndpoint(ctx, ep.ID)

	engine := fanout.NewEngine(pool)
	idempotencyKey := "idem_order_xyz_" + time.Now().Format("20060102150405.000000")

	// 1. First publish
	res1, err := engine.PublishEvent(ctx, fanout.PublishParams{
		Type:           "order.placed",
		Payload:        json.RawMessage(`{"order_id": 999}`),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("first publish failed: %v", err)
	}
	if res1.IsDuplicate {
		t.Fatalf("expected first publish to be non-duplicate")
	}
	if len(res1.Deliveries) == 0 {
		t.Fatalf("expected at least 1 delivery created")
	}

	// 2. Second publish with identical idempotency key
	res2, err := engine.PublishEvent(ctx, fanout.PublishParams{
		Type:           "order.placed",
		Payload:        json.RawMessage(`{"order_id": 999}`),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("second publish failed: %v", err)
	}
	if !res2.IsDuplicate {
		t.Fatalf("expected second publish to be detected as duplicate")
	}
	if res2.Event.ID != res1.Event.ID {
		t.Errorf("expected same event ID %s, got %s", res1.Event.ID, res2.Event.ID)
	}
	if len(res2.Deliveries) != len(res1.Deliveries) {
		t.Errorf("expected matching delivery count %d, got %d", len(res1.Deliveries), len(res2.Deliveries))
	}
}
