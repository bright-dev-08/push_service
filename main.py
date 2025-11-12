import os
import asyncio
from fastapi import FastAPI
import redis.asyncio as redis
from aiormq import connect, Connection
from aiormq.exceptions import AMQPConnectionError

from config import get_redis_url, RABBITMQ_URL, PUSH_QUEUE
from api.endpoints import router as api_router, set_global_clients
from core.worker import process_push_event, set_redis_client, set_fcm_provider
from core.providers.fcm import FCMProvider

# --- State and Client Holders ---
rabbit_connection: Connection | None = None
redis_client: redis.Redis | None = None
fcm_provider: FCMProvider | None = None

app = FastAPI(
    title="Push Service (Service D)",
    description="Asynchronous worker for mobile notification delivery and token management."
)

# --- Connection Handlers ---

async def init_redis():
    """Initializes the Redis client."""
    redis_url = get_redis_url()
    if not redis_url:
        print("🔴 Redis URL configuration is missing.")
        return

    global redis_client
    try:
        # Use decode_responses=True for easy string handling
        redis_client = redis.from_url(redis_url, decode_responses=True)
        # Attempt a simple ping to verify connection
        await redis_client.ping()
        print("🟢 Redis connection established successfully.")
    except Exception as e:
        print(f"🔴 ERROR connecting to Redis: {e}")
        redis_client = None

async def init_rabbitmq_consumer():
    """Initializes and starts the non-blocking RabbitMQ consumer."""
    if not RABBITMQ_URL:
        print("🔴 RABBITMQ_URL is not set. Consumer will not start.")
        return

    global rabbit_connection
    try:
        # Connect to RabbitMQ with default parameters
        # The retry loop below will handle reconnection on failure
        rabbit_connection = await connect(RABBITMQ_URL)
        print(f"📡 RabbitMQ connection established. Connection ID: {id(rabbit_connection)}")
        
        channel = await rabbit_connection.channel()
        print(f"📡 RabbitMQ channel created. Channel ID: {id(channel)}")
        
        # Set QoS: prefetch_count=1 ensures the worker only takes one message at a time
        await channel.basic_qos(prefetch_count=1)
        print(f"📡 QoS set to prefetch_count=1")
        
        # Start consuming from the push.queue
        print(f"📡 Starting to consume from queue: {PUSH_QUEUE}")
        await channel.basic_consume(PUSH_QUEUE, process_push_event, no_ack=False)
        print(f"🟢 RabbitMQ Consumer started successfully on queue: {PUSH_QUEUE}")
        print(f"📡 Waiting for messages... Press Ctrl+C to quit.")
        
        # NOW that we're connected, update the API router with the connection
        set_global_clients(redis_client, rabbit_connection)
        
        # Start a connection monitor task to keep the connection alive
        asyncio.create_task(monitor_rabbitmq_connection())
        
        # Keep the consumer running indefinitely in a loop
        # This ensures messages are continuously processed
        while True:
            try:
                await asyncio.sleep(1)  # Yield control to allow message processing
                if rabbit_connection.is_closed:
                    print("⚠️ RabbitMQ connection closed. Reinitializing...")
                    await init_rabbitmq_consumer()
                    return
            except asyncio.CancelledError:
                print("📡 RabbitMQ consumer task cancelled")
                break
            except Exception as e:
                print(f"⚠️ Error in consumer loop: {e}")
                await asyncio.sleep(1)
        
    except AMQPConnectionError as e:
        print(f"🔴 ERROR connecting to RabbitMQ: {e}. Retrying consumer initialization in 10s...")
        rabbit_connection = None
        await asyncio.sleep(10)
        await init_rabbitmq_consumer() # Retry connection
    except Exception as e:
        print(f"🔴 Unhandled ERROR during RabbitMQ setup: {e}")
        rabbit_connection = None

async def monitor_rabbitmq_connection():
    """Monitor RabbitMQ connection and attempt to keep it alive by periodic checks."""
    global rabbit_connection
    
    while True:
        try:
            await asyncio.sleep(15)  # Check every 15 seconds (more frequent)
            
            if rabbit_connection and not rabbit_connection.is_closed:
                # Perform a lightweight operation to keep connection alive
                # This prevents server-side idle timeouts
                try:
                    channel = await rabbit_connection.channel()
                    # Declare the queue passively (just check it exists)
                    await channel.queue_declare(PUSH_QUEUE, passive=True)
                    # Don't print anything to avoid spam, just silently keep alive
                except Exception as e:
                    print(f"⚠️ RabbitMQ keepalive check failed: {e}")
                    rabbit_connection = None
        except asyncio.CancelledError:
            print("RabbitMQ monitor task cancelled")
            break
        except Exception as e:
            print(f"⚠️ Error in RabbitMQ monitor task: {e}")
            await asyncio.sleep(5)

# --- FastAPI Lifecycle Hooks ---

async def init_fcm():
    """Initialize Firebase Cloud Messaging provider."""
    global fcm_provider
    
    try:
        fcm_provider = FCMProvider()
        print("🟢 FCM provider initialized successfully.")
    except Exception as e:
        print(f"🔴 ERROR initializing FCM provider: {e}")
        fcm_provider = None

@app.on_event("startup")
async def startup_event():
    """FastAPI event hook to start connections and the consumer."""
    await init_redis()
    await init_fcm()
    
    # Inject clients into other modules
    if redis_client:
        set_redis_client(redis_client)
    if fcm_provider:
        set_fcm_provider(fcm_provider)
        
    # Start the async consumer logic in a background task
    # Note: set_global_clients will be called from init_rabbitmq_consumer() once connected
    asyncio.create_task(init_rabbitmq_consumer())


@app.on_event("shutdown")
async def shutdown_event():
    """FastAPI event hook to close connections."""
    if rabbit_connection:
        await rabbit_connection.close()
    if redis_client:
        await redis_client.close()
    print("🔴 Shut down connections complete.")

# --- Register Endpoints ---
app.include_router(api_router)

# --- Execution Entry Point ---
if __name__ == "__main__":
    import uvicorn
    # Make sure to create the .env file with your credentials before running
    print("Starting FastAPI server...")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)