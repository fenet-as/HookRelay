-- name: CreateEvent :one
INSERT INTO events (id, type, payload, idempotency_key)
VALUES ($1, $2, $3, $4)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;

-- name: GetEventByID :one
SELECT * FROM events
WHERE id = $1;

-- name: GetEventByIdempotencyKey :one
SELECT * FROM events
WHERE idempotency_key = $1;

