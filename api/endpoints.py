from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from redis.asyncio import Redis
from aiormq import Connection

from schemas import HealthStatus, TokenValidation
from config import TOKEN_METADATA_PREFIX

# Global clients will be set by the main application file
redis_client: Redis | None = None
rabbit_connection: Connection | None = None

router = APIRouter(tags=["Monitoring & Token Management"])

def set_global_clients(r_client: Redis, r_conn: Connection):
    """Setter function to inject the Redis and RabbitMQ clients."""
    global redis_client, rabbit_connection
    redis_client = r_client
    rabbit_connection = r_conn

@router.get("/health", response_model=HealthStatus)
async def get_health():
    """Provides the current health and connection status of the service."""
    # Check RabbitMQ connection
    rabbit_status = False
    if rabbit_connection is not None:
        try:
            # Test if connection is alive by checking if it's not closed
            rabbit_status = not rabbit_connection.is_closed
        except Exception:
            rabbit_status = False
    
    # Check Redis connection
    redis_status = False
    if redis_client:
        try:
            # Ping Redis to confirm connection health
            redis_status = await redis_client.ping()
        except Exception:
            redis_status = False

    # Overall status: OK if both connected, DEGRADED if one is down, UNHEALTHY if none are up
    if rabbit_status and redis_status:
        overall_status = "OK"
    elif rabbit_status or redis_status:
        overall_status = "DEGRADED"
    else:
        overall_status = "INITIALIZING"  # Both disconnected - service may still be starting up
    
    return HealthStatus(
        status=overall_status,
        rabbit_connected=rabbit_status,
        redis_connected=redis_status,
        timestamp=datetime.now()
    )

@router.post("/push/validate_token", response_model=TokenValidation)
async def validate_token(token: str, user_id: int | None = None):
    """
    Validates a push token and updates its metadata in Redis.

    If `user_id` is provided the endpoint also stores a mapping from
    `push:token:user:{user_id}` -> { token: <token> } so the Push worker
    can look up a user's device token by user_id.
    """
    if not redis_client:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Redis connection failed.")

    # Mock validation logic: assume valid unless token contains 'expired'
    is_valid = "expired" not in token.lower()
    
    token_key = TOKEN_METADATA_PREFIX + token
    
    token_metadata = {
        "is_valid": str(is_valid), # Redis typically stores strings
        "last_validated": datetime.now().isoformat()
    }
    
    # Store token metadata keyed by token
    await redis_client.hmset(token_key, token_metadata)

    # If caller provided a user_id, also store reverse mapping so worker can fetch token by user
    if user_id is not None:
        user_key = TOKEN_METADATA_PREFIX + f"user:{user_id}"
        await redis_client.hset(user_key, "token", token)

    return TokenValidation(
        token=token,
        is_valid=is_valid,
        last_validated=datetime.now()
    )