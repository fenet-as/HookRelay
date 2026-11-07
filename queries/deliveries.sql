-- name: CreateDelivery :one
INSERT INTO deliveries (event_id, endpoint_id, status, next_run_at)
VALUES ($1, $2, 'pending', now())
ON CONFLICT (event_id, endpoint_id) DO NOTHING
RETURNING *;

-- name: GetDeliveryByID :one
SELECT * FROM deliveries
WHERE id = $1;

-- name: GetDeliveryWithDetails :one
SELECT
    d.id AS delivery_id,
    d.event_id,
    d.endpoint_id,
    d.status,
    d.attempts,
    d.next_run_at,
    d.locked_at,
    e.type AS event_type,
    e.payload AS event_payload,
    ep.url AS endpoint_url,
    ep.secret AS endpoint_secret,
    ep.active AS endpoint_active
FROM deliveries d
JOIN events e ON d.event_id = e.id
JOIN endpoints ep ON d.endpoint_id = ep.id
WHERE d.id = $1;

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

-- name: UnlockStuckDeliveries :many
UPDATE deliveries
SET locked_at = NULL
WHERE status = 'pending'
  AND locked_at IS NOT NULL
  AND locked_at < $1
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

-- name: CountPendingDeliveries :one
SELECT COUNT(*) FROM deliveries
WHERE status = 'pending';

-- name: CountDueDeliveries :one
SELECT COUNT(*) FROM deliveries
WHERE status = 'pending'
  AND next_run_at <= now()
  AND locked_at IS NULL;

