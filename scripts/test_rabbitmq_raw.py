#!/usr/bin/env python3
"""
Direct test of RabbitMQ connection to diagnose protocol issues.
This script attempts to connect to RabbitMQ using aiormq directly,
with detailed logging to help identify protocol-level issues.
"""
import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import config
from aiormq import connect
from aiormq.exceptions import AMQPConnectionError


async def test_rabbitmq_connection():
    """Test RabbitMQ connection with detailed logging."""
    print(f"Testing RabbitMQ connection to: {config.RABBITMQ_URL}")
    
    try:
        print("[1] Attempting initial connection...")
        connection = await connect(config.RABBITMQ_URL)
        print("[2] ✅ Connection established!")
        
        print("[3] Creating channel...")
        channel = await connection.channel()
        print("[4] ✅ Channel created!")
        
        print("[5] Declaring queue (passive)...")
        await channel.queue_declare("push.queue", passive=True)
        print("[6] ✅ Queue verified!")
        
        print("[7] Sleeping for 60 seconds to monitor connection stability...")
        await asyncio.sleep(60)
        
        print("[8] Checking if connection still alive...")
        if not connection.is_closed:
            print("[9] ✅ Connection still alive after 60 seconds!")
        else:
            print("[9] ❌ Connection was closed!")
        
        print("[10] Closing connection...")
        await connection.close()
        print("[11] ✅ Test completed successfully!")
        return True
        
    except AMQPConnectionError as e:
        print(f"❌ AMQP Connection Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unhandled Error: {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_rabbitmq_connection())
    sys.exit(0 if success else 1)
