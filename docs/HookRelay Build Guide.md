# HookRelay: Build Guide

Project 2 of 5 in the Go backend series. Goal: learn webhooks, outbound HTTP clients, HMAC signatures, idempotency, timeouts and retries by building a service that delivers events to other people's URLs reliably.

## 1. What you're building

A service where clients register webhook endpoints (a URL plus the event types they care about) and publish events through an API. HookRelay then delivers each event to every matching endpoint as a signed HTTP POST, records every attempt, and retries failures with backoff.

Think of it as a tiny version of what Stripe or GitHub do when they send you webhooks.

### What you will learn

- Designing a webhook delivery contract (headers, payload, signature)
- HMAC-SHA256 signing and constant-time verification
- Configuring `http.Client` properly (timeouts, connection reuse, redirects)
- Idempotency on both sides: deduplicating incoming events and letting receivers deduplicate deliveries
- Retry policy: which failures to retry, backoff with jitter, when to give up
- Fan-out: one event becomes many deliveries
- Protecting yourself from abuse (SSRF, slow receivers, huge responses)

### How it differs from Job Runner

Job Runner executes your own code. HookRelay talks to servers you don't control, so the interesting problems are about untrusted, slow and unreliable remote systems. You can reuse the Postgres claim pattern (`FOR UPDATE SKIP LOCKED`) and the worker pool idea from Project 1, but the focus here is the delivery contract and failure handling.

### Out of scope for the MVP

Multi-tenant accounts and billing, a UI, per-endpoint rate limiting (that's RateGuard's job), message ordering guarantees, and payload transformation.

## 2. Architecture

One Go binary, plus a small test receiver you'll build for yourself.

1. **API server:** manages endpoints and accepts events.
2. **Fan-out step:** when an event arrives, create one `delivery` row for each subscribed endpoint, in the same transaction as the event insert.
3. **Delivery workers:** claim due deliveries from Postgres, send the HTTP request, record the attempt, and either mark success or schedule a retry.
4. **Test receiver (separate small program):** a Go server with configurable behavior (always 200, fail 50% of the time, sleep 10 seconds, return 500) and signature verification. This is how you test everything.

```text
client -> POST /events -> [events] -> fan-out -> [deliveries] <- workers -> HTTP POST -> receiver
                                                      |
                                               [delivery_attempts]
```

Postgres is the source of truth. Workers are stateless.

## 3. The delivery contract

Design this first, because receivers depend on it. Document it in the README.

Every delivery is an HTTP `POST` with a JSON body and these headers:

| Header | Purpose |
| --- | --- |
| `Content-Type: application/json` | Body format |
| `X-HookRelay-Event-Id` | Stable event ID, the same on every retry, so receivers can deduplicate |
| `X-HookRelay-Delivery-Id` | ID of this delivery (one per event and endpoint pair) |
| `X-HookRelay-Attempt` | Attempt number, starting at 1 |
| `X-HookRelay-Timestamp` | Unix seconds when this attempt was signed |
| `X-HookRelay-Signature` | `sha256=<hex>` HMAC of `timestamp + "." + raw body` using the endpoint's secret |

Body example:

```json
{
  "id": "evt_01HZ...",
  "type": "order.created",
  "created_at": "2026-11-05T10:00:00Z",
  "data": { "order_id": 42 }
}
```

Success means any `2xx` response. Everything else is a failure.

### Why sign timestamp + body

Signing only the body lets an attacker capture a valid request and replay it forever. Including the timestamp lets receivers reject requests older than, say, five minutes. Receivers must verify against the **raw bytes** they received, not a re-serialized JSON object.

## 4. Data model

```sql
CREATE TABLE endpoints (
  id          BIGSERIAL PRIMARY KEY,
  url         TEXT        NOT NULL,
  secret      TEXT        NOT NULL,
  event_types TEXT[]      NOT NULL DEFAULT '{}',   -- empty = all events
  active      BOOLEAN     NOT NULL DEFAULT true,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE events (
  id              TEXT PRIMARY KEY,                 -- e.g. evt_<ulid>
  type            TEXT        NOT NULL,
  payload         JSONB       NOT NULL,
  idempotency_key TEXT UNIQUE,                      -- client-supplied, optional
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE delivery_status AS ENUM ('pending', 'succeeded', 'dead');

CREATE TABLE deliveries (
  id          BIGSERIAL PRIMARY KEY,
  event_id    TEXT   NOT NULL REFERENCES events(id),
  endpoint_id BIGINT NOT NULL REFERENCES endpoints(id),
  status      delivery_status NOT NULL DEFAULT 'pending',
  attempts    INT         NOT NULL DEFAULT 0,
  next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at   TIMESTAMPTZ,
  UNIQUE (event_id, endpoint_id)
);

CREATE INDEX deliveries_due_idx ON deliveries (next_run_at) WHERE status = 'pending';

CREATE TABLE delivery_attempts (
  id            BIGSERIAL PRIMARY KEY,
  delivery_id   BIGINT NOT NULL REFERENCES deliveries(id),
  attempt       INT    NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  duration_ms   INT,
  status_code   INT,
  error         TEXT,
  response_body TEXT                                 -- truncated, e.g. first 1 KB
);
```

The `UNIQUE (event_id, endpoint_id)` constraint is a safety net: even if fan-out runs twice, you can't create duplicate deliveries.

## 5. API design

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/endpoints` | Register a URL and event types. The response returns the secret once. |
| GET | `/endpoints` | List endpoints |
| PATCH | `/endpoints/:id` | Update URL, event types, or active flag |
| DELETE | `/endpoints/:id` | Remove an endpoint |
| POST | `/endpoints/:id/rotate-secret` | Generate a new secret |
| POST | `/events` | Publish an event (`type`, `data`, optional `Idempotency-Key` header) |
| GET | `/events/:id` | Event details with its deliveries |
| GET | `/deliveries/:id` | A delivery with all its attempts |
| POST | `/deliveries/:id/replay` | Manually retry a dead delivery |

`POST /events` returns `202 Accepted` quickly. It never waits for deliveries.

### Idempotency on ingest

If a client retries `POST /events` because of a network error, they must not create a duplicate event. With an `Idempotency-Key` header, insert with `ON CONFLICT (idempotency_key) DO NOTHING` and, on conflict, return the original event. Think through the edge case where two identical requests arrive at the same instant. The unique constraint is what saves you, not an application-level check.

## 6. Milestones

Treat each as a commit-worthy checkpoint.

### Milestone 1: Foundations and the test receiver (day 1)

- Repo layout, Docker Compose with Postgres, migrations, SQLC queries.
- Build the test receiver first. Give it modes through query parameters or an env var: `ok`, `fail`, `slow`, `flaky`. Make it log every request with headers.
- Add endpoint CRUD in the API with validation: URL must be `http` or `https`, and the secret is generated with `crypto/rand`.

**Check:** you can register an endpoint with `curl` and see it in the database.

### Milestone 2: Events and fan-out (day 1-2)

- Implement `POST /events` with idempotency key support.
- In one transaction: insert the event, find matching active endpoints, insert one delivery per endpoint.
- Write a test where one event fans out to three endpoints, and another where two endpoints don't match the event type.

**Check:** publishing the same event twice with the same idempotency key creates exactly one event and one set of deliveries.

### Milestone 3: Signing and a single delivery (day 2-3)

- Write `Sign(secret, timestamp, body)` as a pure function and unit test it, including a known test vector.
- Write the matching `Verify` used by your test receiver, and use `hmac.Equal` for constant-time comparison. Understand why `==` on signatures is a timing leak.
- Build the delivery function: marshal once, sign, set headers, send with `http.NewRequestWithContext`.
- Record an attempt row for every try, including how long it took.

**Check:** the test receiver accepts a valid signature and rejects a tampered body and an old timestamp.

### Milestone 4: The HTTP client done properly (day 3)

This is the most important Go lesson in the project. The default `http.Client` has no timeout, so one hung receiver can stall a worker forever.

- Set an overall `Timeout` (for example 10 seconds) and tune the `Transport`: `DialContext` timeout, `TLSHandshakeTimeout`, `ResponseHeaderTimeout`, and idle connection settings.
- Reuse one client for all deliveries so connections are pooled.
- Limit how much of the response body you read (`io.LimitReader`), and always drain and close the body, otherwise connections can't be reused.
- Decide on redirects. Most webhook senders do not follow them. Set `CheckRedirect` to return `http.ErrUseLastResponse`.

**Check:** point an endpoint at the `slow` receiver and confirm the attempt fails by timeout at the time you configured, without blocking other deliveries.

### Milestone 5: Workers and retries (day 3-4)

- Reuse the claim query from Job Runner, adapted for `deliveries` (`status = 'pending' AND next_run_at <= now()` with `FOR UPDATE SKIP LOCKED`).
- On success, mark `succeeded`.
- On failure, increment attempts and set `next_run_at` using exponential backoff with jitter. After the maximum attempts (for example 8 over roughly a day), mark `dead`.
- Decide which responses are retryable: network errors, timeouts, `5xx`, `408`, `429` (and honor `Retry-After` if present) are retryable. Most other `4xx` responses are the receiver's permanent "no", so consider whether to retry them at all. Write your decision down.

**Check:** against the `flaky` receiver, deliveries eventually succeed. Against the `fail` receiver, they end up `dead` after exactly the maximum attempts.

### Milestone 6: Safety and operations (day 4-5)

- **SSRF protection:** a malicious user could register `http://localhost:5432` or a cloud metadata address and use your server to probe your internal network. Resolve the hostname and reject loopback, private and link-local IP ranges. Do the check at connection time in a custom `DialContext` (checking only at registration time is vulnerable to DNS changes). Allow an env flag to disable this in local development so your test receiver works.
- **Auto-disable:** if an endpoint fails continuously for a threshold (for example 20 consecutive failures), mark it inactive and stop creating work for it.
- **Replay:** implement `POST /deliveries/:id/replay`.
- **Graceful shutdown and crash recovery:** same approach as Job Runner. Finish in-flight deliveries, and have a reaper that requeues deliveries stuck with an old `locked_at`.

**Check:** registering `http://127.0.0.1` is rejected in production mode, and `kill -9` mid-delivery leads to the delivery being retried later.

### Milestone 7: Tests, docs and polish (day 5)

- Integration tests with a real Postgres and `httptest.Server` as the receiver.
- Run everything with `-race`.
- README with the delivery contract, a signature verification example for receivers (in Go, and ideally one more language), architecture diagram, curl examples, and a failure-modes section.

## 7. Suggested project layout

```text
hookrelay/
  cmd/hookrelay/main.go          # the service
  cmd/testreceiver/main.go       # your fake receiver
  internal/
    api/                         # Gin handlers
    signing/                     # Sign and Verify, with tests
    delivery/                    # HTTP client, worker pool, retry policy
    fanout/                      # event to deliveries logic
    netguard/                    # SSRF-safe dialer
    store/                       # SQLC code
  migrations/
  queries/
  docker-compose.yml
  Makefile
  README.md
```

## 8. Concepts to understand before you move on

1. Why can a delivery be sent more than once, and what must the receiver do about it?
2. Why does the event ID stay the same across retries while the attempt number changes?
3. Why sign the timestamp together with the body?
4. What is the difference between an idempotency key on `POST /events` and the event ID header on deliveries?
5. Which status codes should be retried, and why not all of them?
6. Why check IP addresses at dial time rather than at registration time?
7. What happens to your connection pool if you don't close the response body?
8. Why is the unique constraint on `(event_id, endpoint_id)` important even though fan-out is transactional?

## 9. Common pitfalls

- Using `http.DefaultClient` (no timeout) for deliveries.
- Forgetting to close or drain response bodies, which leaks connections.
- Verifying signatures against re-serialized JSON instead of raw bytes.
- Comparing signatures with `==` instead of `hmac.Equal`.
- Holding a database transaction open while making the HTTP call. Claim, commit, deliver, then update.
- Retrying forever with no cap, or retrying instantly with no backoff, which hammers a struggling receiver.
- Returning the secret on every `GET /endpoints`. Show it once at creation and on rotation.
- Storing unlimited response bodies. Truncate them.

## 10. Definition of done

- [ ] Endpoints can be created, updated, deleted, and have their secrets rotated
- [ ] Publishing an event fans out to all matching active endpoints
- [ ] Duplicate `POST /events` calls with the same idempotency key create one event
- [ ] Every delivery is signed, and your receiver verifies the signature and timestamp
- [ ] HTTP client has explicit timeouts, bounded body reads, and no redirect following
- [ ] Failures retry with exponential backoff and jitter, then become `dead`
- [ ] Every attempt is recorded with status code, duration, and error
- [ ] SSRF protection blocks private and loopback addresses outside dev mode
- [ ] Dead deliveries can be replayed
- [ ] Tests pass with `-race`; the project runs with one `docker compose up`
- [ ] README documents the delivery contract and receiver verification

## 11. Stretch goals

Pick one or two after the checklist is complete.

- Per-endpoint concurrency limit so one slow receiver can't occupy all workers
- Circuit breaker per endpoint (stop trying for a cooling-off period after repeated failures)
- Support for multiple active secrets during rotation, sending two signatures
- Event type wildcards (for example `order.*`)
- Delivery logs endpoint with filtering by status and date
- Prometheus metrics for attempts, successes, latency and retry counts (feeds into Project 5)

## 12. Suggested first day

1. Create the repo and Docker Compose with Postgres.
2. Write the migrations from section 4.
3. Build the test receiver with `ok`, `fail`, `slow` and `flaky` modes.
4. Implement endpoint registration and write one test for it.
5. Commit. Next up: events and fan-out.
