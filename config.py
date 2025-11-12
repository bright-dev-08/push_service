import os
from dotenv import load_dotenv
from urllib.parse import urlparse # <-- Added this standard library import for robust URL parsing

# Load environment variables from .env file
load_dotenv()

# --- Connection Details ---
RABBITMQ_URL = os.getenv("RABBITMQ_URL")

# Redis Configuration (Using Upstash URL and Token)
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# RabbitMQ Contract Details
PUSH_QUEUE = "push.queue"

# Dead Letter Queue for failed notifications
FAILED_QUEUE = "failed.queue"

# Redis Keys Prefixes
IDEMPOTENCY_KEY_PREFIX = "push:event:"
RATE_LIMIT_PREFIX = "push:rate:"
TOKEN_METADATA_PREFIX = "push:token:"

# FCM Configuration
# GOOGLE_APPLICATION_CREDENTIALS should be set in .env pointing to your service account JSON file

# Redis connection string for async redis client
def get_redis_url():
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    
    # Construct the rediss URL using the token and hostname/port.
    try:
        # Use urlparse for a reliable, standard-library approach 
        # to extract the hostname from the REST URL, eliminating fragile string splitting.
        parsed_url = urlparse(UPSTASH_REDIS_URL)
        hostname_only = parsed_url.hostname

        if not hostname_only:
            # If parsing fails to yield a hostname, raise an error
            raise ValueError("Could not parse hostname from UPSTASH_REDIS_REST_URL.")

        # Construct the final secure Redis URI (rediss://) 
        # using the token as the password and standard Redis port (6379).
        return f"rediss://:{UPSTASH_REDIS_TOKEN}@{hostname_only}:6379"
    except Exception as e:
        # Catch generic exceptions and the ValueError raised above
        print(f"Error building Redis URL: {e}. Check UPSTASH_REDIS_REST_URL format.")
        return None