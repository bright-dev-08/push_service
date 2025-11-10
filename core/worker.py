import json
import asyncio
from aiormq.abc import DeliveredMessage
from redis.asyncio import Redis

from schemas import NotificationEvent
from config import IDEMPOTENCY_KEY_PREFIX, RATE_LIMIT_PREFIX

# The global Redis client is passed in from main.py
redis_client: Redis | None = None

def set_redis_client(client: Redis):
    """Setter function to inject the Redis client into the worker module."""
    global redis_client
    redis_client = client

async def process_push_event(message: DeliveredMessage):
    """
    Handles incoming messages from the push.queue. Implements Idempotency and Resilience.
    """
    if not redis_client:
        print("❌ Worker Error: Redis client not initialized.")
        # Attempt to reject and requeue if Redis is down
        await message.channel.basic_reject(message.delivery_tag, requeue=True)
        return

    try:
        body = message.body.decode()
        event = NotificationEvent.model_validate_json(body)
        
        # --- 1. Idempotency Check (NFR) ---
        idempotency_key = IDEMPOTENCY_KEY_PREFIX + str(event.event_id)
        
        # SETNX returns 1 if key is new (we proceed), 0 if exists (we skip)
        if not await redis_client.set(idempotency_key, "processed", nx=True, ex=604800):
            print(f"🟠 Idempotent Skip: Event ID {event.event_id} already processed.")
            await message.channel.basic_ack(message.delivery_tag)
            return

        print(f"➡️ Processing Push Event: {event.event_id} for user {event.user_id}")
        
        # --- 2. Resolve Dependencies (Sync/Async Calls) ---
        # NOTE: Implement async HTTP calls (e.g., using 'httpx') here to Template Service and User Service
        device_token = "mock_device_token_xyz" # Mock call to User Service
        # rendered_content = await httpx.get(TEMPLATE_SERVICE_URL/render...) # Mock call to Template Service

        # --- 3. Rate Limit Check (NFR) ---
        rate_key = RATE_LIMIT_PREFIX + str(event.user_id)
        # Limit: 5 pushes per user per minute (TTL=60s)
        current_count = await redis_client.incr(rate_key)
        if current_count == 1:
            await redis_client.expire(rate_key, 60) # Set TTL on first push
            
        if current_count > 5:
            print(f"🛑 Rate Limit Exceeded for user {event.user_id}. NACKing message for future retry.")
            await message.channel.basic_nack(message.delivery_tag, requeue=True)
            # Rollback Idempotency Key
            await redis_client.delete(idempotency_key)
            return
            
        # --- 4. External Delivery (FCM/APNs) ---
        if device_token == "mock_device_token_xyz":
            await asyncio.sleep(0.05) # Simulate external provider latency
            success = True
        else:
            success = False # Simulate failure

        # --- 5. Delivery Status Update ---
        if success:
            # Update Status Store (PostgreSQL) here
            print(f"✅ Push delivered successfully for {event.event_id}")
            await message.channel.basic_ack(message.delivery_tag)
        else:
            raise Exception(f"External Push Provider failed to send for {event.event_id}")

    except Exception as e:
        print(f"❌ Critical Failure processing {event.event_id}: {e}")
        # Reject message, request broker to requeue (True)
        await message.channel.basic_reject(message.delivery_tag, requeue=True)