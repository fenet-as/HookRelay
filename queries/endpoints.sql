-- name: CreateEndpoint :one
INSERT INTO endpoints (url, secret, event_types, active)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetEndpointByID :one
SELECT * FROM endpoints
WHERE id = $1;

-- name: ListEndpoints :many
SELECT * FROM endpoints
ORDER BY id ASC;

-- name: UpdateEndpoint :one
UPDATE endpoints
SET
    url = COALESCE(sqlc.narg('url'), url),
    event_types = COALESCE(sqlc.narg('event_types'), event_types),
    active = COALESCE(sqlc.narg('active'), active)
WHERE id = sqlc.arg('id')
RETURNING *;

-- name: RotateEndpointSecret :one
UPDATE endpoints
SET secret = $2
WHERE id = $1
RETURNING *;

-- name: DeleteEndpoint :exec
DELETE FROM endpoints
WHERE id = $1;

-- name: ListActiveEndpointsForEventType :many
SELECT * FROM endpoints
WHERE active = true
  AND (cardinality(event_types) = 0 OR sqlc.arg('event_type')::text = ANY(event_types))
ORDER BY id ASC;
