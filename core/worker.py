import json
import asyncio
from aiormq.abc import DeliveredMessage
from redis.asyncio import Redis
import time

from schemas import NotificationEvent
from datetime import datetime
from config import IDEMPOTENCY_KEY_PREFIX, RATE_LIMIT_PREFIX, TOKEN_METADATA_PREFIX
from core.providers.fcm import FCMProvider
from core.template_resolver import resolve_push_payload
from config import PUSH_QUEUE, FAILED_QUEUE

# The global Redis client and FCM provider are passed in from main.py
redis_client: Redis | None = None
fcm_provider: FCMProvider | None = None


def set_redis_client(client: Redis):
    """Setter function to inject the Redis client into the worker module."""
    global redis_client
    redis_client = client


def set_fcm_provider(provider: FCMProvider):
    """Setter function to inject the FCM provider into the worker module."""
    global fcm_provider
    fcm_provider = provider


async def get_device_token(user_id: int | str) -> str:
    """
    Get the user's device token from Redis.
    In a real implementation, this would call the User Service.
    """
    # For now, we'll mock this with Redis
    token_key = f"{TOKEN_METADATA_PREFIX}user:{user_id}"
    token_data = await redis_client.hgetall(token_key)
    # hgetall returns bytes keys/values when using redis-py asyncio client; convert if necessary
    if not token_data:
        return None
    # token_data may be a dict of bytes; handle both str and bytes
    token = token_data.get("token") or token_data.get(b"token")
    if isinstance(token, bytes):
        try:
            token = token.decode()
        except Exception:
            token = token.decode('utf-8', errors='ignore')
    return token


# Circuit breaker keys and defaults (stored in Redis so multiple workers share state)
CIRCUIT_FAILURES_KEY = "circuit:fcm:failures"
CIRCUIT_OPEN_KEY = "circuit:fcm:open_until"
CIRCUIT_FAILURE_THRESHOLD = 5
CIRCUIT_OPEN_SECONDS = 60


async def _is_circuit_open() -> bool:
    """Return True if circuit breaker for FCM is open."""
    try:
        open_until = await redis_client.get(CIRCUIT_OPEN_KEY)
        if not open_until:
            return False
        # stored as timestamp float string
        if float(open_until) > time.time():
            return True
        # expired; clear keys
        await redis_client.delete(CIRCUIT_OPEN_KEY)
        await redis_client.delete(CIRCUIT_FAILURES_KEY)
        return False
    except Exception:
        # If Redis is flaky, be conservative and treat circuit as closed
        return False


async def process_push_event(message: DeliveredMessage):
    """
    Handles incoming messages from the push.queue. Implements Idempotency and Resilience.
    """
    print(f"📨 Received message from queue. Delivery tag: {message.delivery_tag}")
    
    if not redis_client:
        print("❌ Worker Error: Redis client not initialized.")
        await message.channel.basic_reject(message.delivery_tag, requeue=True)
        return

    if not fcm_provider:
        print("❌ Worker Error: FCM provider not initialized.")
        await message.channel.basic_reject(message.delivery_tag, requeue=True)
        return

    try:
        body = message.body.decode()
        print(f"📦 Message body: {body[:100]}...")  # Log first 100 chars
        # keep raw dict to support retry counting
        body_json = json.loads(body)
        event = NotificationEvent.model_validate_json(body)

        # --- 1. Idempotency Check (NFR) ---
        idempotency_key = IDEMPOTENCY_KEY_PREFIX + str(event.event_id)

        if not await redis_client.set(idempotency_key, "processed", nx=True, ex=604800):
            print(f"🟠 Idempotent Skip: Event ID {event.event_id} already processed.")
            await message.channel.basic_ack(message.delivery_tag)
            return

        print(f"➡️ Processing Push Event: {event.event_id} for user {event.user_id}")

        # --- 2. Get User's Device Token ---
        device_token = await get_device_token(event.user_id)
        if not device_token:
            # Fallback: allow the producer to include a device token in the message
            # useful for quick tests where registration isn't available.
            device_token = body_json.get("device_token")
            if device_token:
                print(f"ℹ️ Using device_token from message for user {event.user_id}")
                # Persist this mapping in Redis for future messages (best-effort)
                try:
                    token_key = f"{TOKEN_METADATA_PREFIX}user:{event.user_id}"
                    # Store token and registration timestamp
                    await redis_client.hset(token_key, mapping={"token": device_token, "registered_at": datetime.utcnow().isoformat() + "Z"})
                except Exception as ex:
                    print(f"⚠️ Failed to persist token in Redis: {ex}")
            else:
                print(f"❌ No device token found for user {event.user_id}")
                await message.channel.basic_ack(message.delivery_tag)  # Don't requeue if no token
                return

        # --- 3. Rate Limit Check (NFR) ---
        rate_key = RATE_LIMIT_PREFIX + str(event.user_id)
        current_count = await redis_client.incr(rate_key)
        if current_count == 1:
            await redis_client.expire(rate_key, 60)

        if current_count > 5:
            print(f"🛑 Rate Limit Exceeded for user {event.user_id}. NACKing message for future retry.")
            await message.channel.basic_nack(message.delivery_tag, requeue=True)
            # Rollback Idempotency Key
            await redis_client.delete(idempotency_key)
            return

        # --- 4. Circuit Breaker Check ---
        if await _is_circuit_open():
            # Circuit open: schedule a later retry with exponential backoff and ack original
            print(f"⚠️ Circuit open for FCM. Scheduling retry for event {event.event_id}.")
            retry_count = int(body_json.get("_retry_count", 0))
            new_body = dict(body_json)
            new_body["_retry_count"] = retry_count + 1
            delay = 10 * (2 ** retry_count)

            async def _schedule_republish(channel, payload, routing_key, delay_seconds):
                await asyncio.sleep(delay_seconds)
                try:
                    await channel.basic_publish(json.dumps(payload).encode(), routing_key=routing_key)
                    print(f"🔁 Republished message to {routing_key} after {delay_seconds}s (attempt {payload.get('_retry_count')})")
                except Exception as ex:
                    print(f"❌ Failed to republish message while circuit open: {ex}")

            asyncio.create_task(_schedule_republish(message.channel, new_body, PUSH_QUEUE, delay))
            # Roll back idempotency so it can be retried later
            await redis_client.delete(idempotency_key)
            await message.channel.basic_ack(message.delivery_tag)
            return

        # --- 5. Resolve Payload (inline or template) ---
        payload = await resolve_push_payload(body_json)
        if not payload:
            print(f"❌ Could not resolve payload for event {event.event_id}. Missing template and inline payload.")
            await message.channel.basic_ack(message.delivery_tag)  # Don't retry; invalid event
            return

        # --- 6. Send Push via FCM ---
        success = await fcm_provider.send_notification(
            token=device_token,
            title=payload.get("title", "New Notification"),
            body=payload.get("body", ""),
            data=payload.get("data"),
            image=payload.get("image")
        )

        # --- 7. Delivery Status Update ---
        if success:
            print(f"✅ Push delivered successfully for {event.event_id}")
            # reset circuit failure counter on success
            try:
                await redis_client.delete(CIRCUIT_FAILURES_KEY)
            except Exception:
                pass
            await message.channel.basic_ack(message.delivery_tag)
        else:
            # implement retry / DLQ behavior
            # field-based retry counter on the message body: `_retry_count`
            retry_count = int(body_json.get("_retry_count", 0))
            MAX_RETRIES = 3
            base_delay = 2  # seconds

            if retry_count < MAX_RETRIES:
                new_body = dict(body_json)
                new_body["_retry_count"] = retry_count + 1
                delay = base_delay * (2 ** retry_count)

                async def _schedule_republish(channel, payload, routing_key, delay_seconds):
                    await asyncio.sleep(delay_seconds)
                    try:
                        await channel.basic_publish(json.dumps(payload).encode(), routing_key=routing_key)
                        print(f"🔁 Republished message to {routing_key} after {delay_seconds}s (attempt {payload.get('_retry_count')})")
                    except Exception as ex:
                        print(f"❌ Failed to republish message: {ex}")

                # schedule republish and ack original
                asyncio.create_task(_schedule_republish(message.channel, new_body, PUSH_QUEUE, delay))
                print(f"↩️ Scheduled retry #{new_body['_retry_count']} for {event.event_id} in {delay}s")
                # rollback idempotency so message can be processed again
                await redis_client.delete(idempotency_key)
                # Increment circuit failure counter (windowed)
                try:
                    failures = await redis_client.incr(CIRCUIT_FAILURES_KEY)
                    # set a TTL for the failures counter so that successes can reset it over time
                    if failures == 1:
                        await redis_client.expire(CIRCUIT_FAILURES_KEY, 60)
                    if failures >= CIRCUIT_FAILURE_THRESHOLD:
                        open_until = time.time() + CIRCUIT_OPEN_SECONDS
                        await redis_client.set(CIRCUIT_OPEN_KEY, str(open_until))
                        print(f"🚨 Circuit opened for FCM for {CIRCUIT_OPEN_SECONDS}s after {failures} failures")
                except Exception:
                    pass

                await message.channel.basic_ack(message.delivery_tag)
                return
            else:
                # move to dead-letter queue
                try:
                    await message.channel.basic_publish(json.dumps(body_json).encode(), routing_key=FAILED_QUEUE)
                    print(f"⚠️ Moved message {event.event_id} to DLQ ({FAILED_QUEUE}) after {retry_count} attempts")
                except Exception as ex:
                    print(f"❌ Failed to publish to DLQ: {ex}")
                # rollback idempotency for visibility and ack original
                await redis_client.delete(idempotency_key)
                # on moving to DLQ, also reset circuit failure counter for FCM so we don't stay open forever
                try:
                    await redis_client.delete(CIRCUIT_FAILURES_KEY)
                except Exception:
                    pass
                await message.channel.basic_ack(message.delivery_tag)
                return

    except Exception as e:
        # If we couldn't parse event or some unexpected error occurred
        try:
            event_id = event.event_id if 'event' in locals() and getattr(event, 'event_id', None) else 'unknown'
        except Exception:
            event_id = 'unknown'
        print(f"❌ Critical Failure processing {event_id}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        # Reject message, request broker to requeue (True)
        await message.channel.basic_reject(message.delivery_tag, requeue=True)