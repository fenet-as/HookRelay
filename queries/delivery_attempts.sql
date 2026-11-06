-- name: CreateDeliveryAttempt :one
INSERT INTO delivery_attempts (
    delivery_id,
    attempt,
    started_at,
    duration_ms,
    status_code,
    error,
    response_body
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

-- name: ListAttemptsByDeliveryID :many
SELECT * FROM delivery_attempts
WHERE delivery_id = $1
ORDER BY attempt ASC;

