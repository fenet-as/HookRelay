-- name: CreateDelivery :one
INSERT INTO deliveries (event_id, endpoint_id, status, next_run_at)
VALUES ($1, $2, 'pending', now())
ON CONFLICT (event_id, endpoint_id) DO NOTHING
RETURNING *;

-- name: GetDeliveryByID :one
SELECT * FROM deliveries
WHERE id = $1;

-- name: ListDeliveriesByEventID :many
SELECT * FROM deliveries
WHERE event_id = $1
ORDER BY id ASC;

-- name: ClaimDueDeliveries :many
UPDATE deliveries
SET locked_at = now()
WHERE id IN (
    SELECT d.id
    FROM deliveries d
    WHERE d.status = 'pending'
      AND d.next_run_at <= now()
      AND (d.locked_at IS NULL OR d.locked_at < $1)
    ORDER BY d.next_run_at ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: MarkDeliverySucceeded :one
UPDATE deliveries
SET
    status = 'succeeded',
    attempts = attempts + 1,
    locked_at = NULL
WHERE id = $1
RETURNING *;

-- name: ScheduleDeliveryRetry :one
UPDATE deliveries
SET
    status = 'pending',
    attempts = attempts + 1,
    next_run_at = $2,
    locked_at = NULL
WHERE id = $1
RETURNING *;

-- name: MarkDeliveryDead :one
UPDATE deliveries
SET
    status = 'dead',
    attempts = attempts + 1,
    locked_at = NULL
WHERE id = $1
RETURNING *;

-- name: ReplayDelivery :one
UPDATE deliveries
SET
    status = 'pending',
    attempts = 0,
    next_run_at = now(),
    locked_at = NULL
WHERE id = $1
RETURNING *;
