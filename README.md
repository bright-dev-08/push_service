# Push Service

This repository contains the Push Service — an asynchronous microservice that listens to a RabbitMQ `push.queue`, resolves notification payloads (template or inline), and delivers push notifications via Firebase Cloud Messaging (FCM).

This README covers:
- What the service does
- How it works (architecture & dataflow)
- Environment variables and configuration
- Local development & testing
- Deployment checklist

---

## What it does

- Consumes notification events from RabbitMQ `push.queue`.
- Supports resolving payloads using a Template Service or inline payloads.
- Sends mobile push notifications via Firebase Cloud Messaging.
- Implements resilience: idempotency, retries with exponential backoff, DLQ, circuit breaker, rate limiting.

---

## Architecture & Data Flow

1. Client / API Gateway publishes a push event to `push.queue`.
2. The Push Worker consumes the event and validates it using the `NotificationEvent` schema.
3. Idempotency check ensures the event is only processed once (Redis key: `push:event:{event_id}`).
4. The worker resolves the push payload:
   - If `payload` exists in event, use inline payload.
   - Else if `template_code` exists, call Template Service `/v1/templates/render` and substitute variables.
5. The worker fetches the user's device token (Redis or User Service).
6. The worker sends the notification via FCM.
7. On failure, a retry schedule runs (exponential backoff). After retries exhausted, message moves to `failed.queue` (DLQ).
8. Circuit breaker opens after consecutive FCM failures, preventing further immediate sends.

---

## Important files

- `main.py` — FastAPI app and startup lifecycle (initializes Redis, FCM, RabbitMQ consumer).
- `core/worker.py` — Consumer logic: idempotency, rate-limiting, template resolution, FCM send, retries, DLQ.
- `core/template_resolver.py` — Fetches & renders templates from Template Service.
- `core/providers/fcm.py` — Wrapper for Firebase Admin SDK (send_notification).
- `config.py` — Environment variable parsing and helpers.
- `schemas.py` — Pydantic models used to validate messages.
- `scripts/` — Helper scripts for testing and validation.

---

## Environment variables

Required:
- `RABBITMQ_URL` — AMQP connection (e.g., `amqp://guest:guest@localhost:5672/`).
- `UPSTASH_REDIS_REST_URL` — Upstash REST endpoint (if using Upstash).
- `UPSTASH_REDIS_REST_TOKEN` — Upstash token for Redis REST.

Optional / Recommended:
- `TEMPLATE_SERVICE_URL` — Template Service base URL (default: `http://template_service:8002`).
- `INTERNAL_API_SECRET` — Shared secret for service-to-service requests.
- `FIREBASE_CREDENTIALS_JSON` — Service account JSON as a string (or set `GOOGLE_APPLICATION_CREDENTIALS` path).
- `PUSH_QUEUE` — Name of the push queue (default: `push.queue`).
- `FAILED_QUEUE` — Name of the DLQ (default: `failed.queue`).

**Security note:** Never commit secrets. Keep `.env` in `.gitignore`.

---

## Quick start (local)

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Set environment variables (example)

```bash
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
export UPSTASH_REDIS_REST_URL=https://<your-upstash>.upstash.io
export UPSTASH_REDIS_REST_TOKEN=<token>
export FIREBASE_CREDENTIALS_JSON='{"type": "service_account", ...}'
export TEMPLATE_SERVICE_URL=http://localhost:8001
export INTERNAL_API_SECRET=secret
```

3. Start the service

```bash
python main.py
# or run via uvicorn for FastAPI
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

4. (Optional) Start the local Template Service stub

```bash
uvicorn scripts.template_service_stub:app --port 8001 --reload
```

5. Publish a test event

```bash
python scripts/publish_push_event.py \
  --event-id test_001 \
  --user-id user_123 \
  --template-code WELCOME \
  --message-data '{"user":{"name":"Test"},"app_name":"Demo"}'
```

Watch the logs for processing and delivery messages.

---

## Testing

- Unit tests: `pytest test_template_resolver.py -v`
- Integration tests (requires Upstash or proper env): `pytest sample_integration_test.py -v`
- Smoke test (remote services): `python scripts/integration_test_remote.py --dry-run`.
- Pre-push validator: `python scripts/pre_push_validator.py`

---

## Deployment checklist

See `PRE_PUSH_CHECKLIST.md` and `PRODUCTION_READY.md` for a full checklist. In short:

1. Run `python scripts/pre_push_validator.py` and fix issues.
2. Ensure environment variables are configured in your deployment platform.
3. Deploy the service (container or host).
4. Verify `/health` and run an integration smoke test.

---

## Troubleshooting

- If RabbitMQ connection fails: run `python scripts/check_rabbit_queue.py`.
- If template render fails: ensure `TEMPLATE_SERVICE_URL` and `INTERNAL_API_SECRET` are correct.
- If FCM fails: verify `FIREBASE_CREDENTIALS_JSON` is valid.

---

## Contributing

1. Create a new branch: `git checkout -b feature/your-feature`.
2. Run tests and linters.
3. Open a PR and request review.

---

## License

MIT
