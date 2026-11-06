package delivery_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fenet-as/hookrelay/internal/delivery"
	"github.com/fenet-as/hookrelay/internal/store"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestReaper_UnlocksStuckDeliveries(t *testing.T) {
	ctx := context.Background()
	_, queries, cleanup := setupTestDB(t)
	defer cleanup()

	ep, _, deliv := setupDeliveryFixtures(t, ctx, queries, "https://example.com/reaper-test")
	defer queries.DeleteEndpoint(ctx, ep.ID)

	// Simulate a worker claiming this delivery and crashing mid-flight
	claimed, err := queries.ClaimDueDeliveries(ctx, store.ClaimDueDeliveriesParams{
		LockedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Limit:    500,
	})
	if err != nil {
		t.Fatalf("claim failed: %v", err)
	}

	// Verify delivery was locked
	var isClaimed bool
	for _, c := range claimed {
		if c.ID == deliv.ID {
			isClaimed = true
			break
		}
	}
	if !isClaimed {
		t.Fatalf("expected delivery %d to be claimed", deliv.ID)
	}

	// Artificially update locked_at to 10 minutes ago to simulate an abandoned job
	// We can run ClaimDueDeliveries or verify reaper directly with a threshold of 1ms
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reaperCfg := delivery.ReaperConfig{
		Interval:       10 * time.Millisecond,
		StaleThreshold: 1 * time.Millisecond, // Treat anything locked before now as stale
	}
	reaper := delivery.NewReaper(reaperCfg, queries, logger)

	// Wait 5ms so locked_at is older than StaleThreshold
	time.Sleep(5 * time.Millisecond)

	count := reaper.Reap(ctx)
	if count == 0 {
		t.Fatalf("expected reaper to unlock at least 1 delivery")
	}

	// Check delivery in DB: locked_at should now be NULL and status pending
	updated, err := queries.GetDeliveryByID(ctx, deliv.ID)
	if err != nil {
		t.Fatalf("GetDeliveryByID failed: %v", err)
	}
	if updated.LockedAt.Valid {
		t.Errorf("expected locked_at to be NULL after reaper run, got valid timestamp: %v", updated.LockedAt.Time)
	}
	if updated.Status != store.DeliveryStatusPending {
		t.Errorf("expected status pending, got %s", updated.Status)
	}
}
