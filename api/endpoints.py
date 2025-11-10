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
    rabbit_status = rabbit_connection is not None and not rabbit_connection.is_closed
    redis_status = False
    
    try:
        if redis_client:
            # Ping Redis to confirm connection health
            redis_status = await redis_client.ping()
    except Exception:
        redis_status = False

    overall_status = "OK" if rabbit_status and redis_status else "DEGRADED"
    
    return HealthStatus(
        status=overall_status,
        rabbit_connected=rabbit_status,
        redis_connected=redis_status,
        timestamp=datetime.now()
    )

@router.post("/push/validate_token", response_model=TokenValidation)
async def validate_token(token: str):
    """
    Validates a push token and updates its metadata in Redis.
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
    
    # Store token metadata
    await redis_client.hmset(token_key, token_metadata)
    
    return TokenValidation(
        token=token,
        is_valid=is_valid,
        last_validated=datetime.now()
    )