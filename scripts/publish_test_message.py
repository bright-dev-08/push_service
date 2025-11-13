#!/usr/bin/env python3
"""
publish_test_message.py

Publish a test message to RabbitMQ `push.queue` that includes the device token.

Usage examples:

# Using env variable for RabbitMQ URL (recommended):
# export RABBITMQ_URL="amqps://user:pass@host:5671"
python scripts/publish_test_message.py --user-id test-user --token "egDlYQ9_AcdYNP6FztpnXu:APA91bG..."

# Or specify RabbitMQ URL directly:
python scripts/publish_test_message.py --rabbit-url "amqps://user:pass@host:5671" --user-id test-user --token "..."

The script will ensure the queue `push.queue` exists (durable) and publish a persistent message.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from uuid import uuid4

try:
    import pika
except Exception as e:
    print("Missing dependency: pika. Install with: pip install pika")
    raise

DEFAULT_TOKEN = (
    "egDlYQ9_AcdYNP6FztpnXu:APA91bGWaY3Leh-sDjc0rcExzoxF2FaiY0gJ6W0wv--3wivjuvVOrxBOnsC-"
    "c7fZ1pAkAySOx5Yj_fC_bo0Qo5YVRc7BjXneF4BrF08zooPeMbpvqx1niLE"
)

DEFAULT_QUEUE = "push.queue"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("publish_test_message")


def build_message_payload(user_id: str, token: str, title: str | None, message: str | None, template: str | None):
    event = {
        "event_id": str(uuid4()),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "user_id": user_id,
        # include token in the message body so the consumer can test token handling
        "device_token": token,
    }

    # Support either template-based or inline payload
    if template:
        event.update({
            "template_id": template,
            "template_context": {"user_name": user_id},
        })
    else:
        event.update({
            "payload": {
                "title": title or "Test Notification",
                "body": message or "This is a test notification sent by publish_test_message.py",
            }
        })

    return event


def publish(rabbit_url: str, queue: str, body: dict, persistent: bool = True):
    params = pika.URLParameters(rabbit_url)
    # Allow pika to handle amqps if URL is amqps://

    conn = None
    try:
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        # Try declaring the queue; if the broker rejects due to existing
        # queue arguments (e.g. an x-dead-letter-exchange already set),
        # re-declare using the known dead-letter-exchange value.
        try:
            ch.queue_declare(queue=queue, durable=True)
        except pika.exceptions.ChannelClosedByBroker as e:
            # Handle 406 PRECONDITION_FAILED about mismatched queue args
            msg = str(e)
            if "x-dead-letter-exchange" in msg:
                log.warning(
                    "Queue '%s' exists with mismatched arguments. Re-declaring with DLX 'notifications.dlx'...",
                    queue,
                )
                # Close broken connection and open a fresh one to retry declaration
                try:
                    if conn and not conn.is_closed:
                        conn.close()
                except Exception:
                    pass

                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.queue_declare(queue=queue, durable=True, arguments={"x-dead-letter-exchange": "notifications.dlx"})
            else:
                # Unknown broker rejection — re-raise
                raise

        properties = pika.BasicProperties(delivery_mode=2) if persistent else None

        ch.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(body).encode("utf-8"),
            properties=properties,
        )

        log.info("Published message to queue '%s' with event_id=%s", queue, body.get("event_id"))

    except Exception as exc:
        log.exception("Failed to publish message: %s", exc)
        raise
    finally:
        if conn and not conn.is_closed:
            conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish a test push event to RabbitMQ push.queue")
    parser.add_argument("--rabbit-url", help="RabbitMQ URL (amqp:// or amqps://). If omitted, uses RABBITMQ_URL env var.")
    parser.add_argument("--queue", default=DEFAULT_QUEUE, help="Queue name (default: push.queue)")
    parser.add_argument("--user-id", default="test-user", help="User ID to include in the message")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Device token to include in the message")
    parser.add_argument("--template", help="Template id to use instead of inline payload")
    parser.add_argument("--title", help="Inline payload title (when not using template)")
    parser.add_argument("--message", help="Inline payload body (when not using template)")
    parser.add_argument("--no-persistent", dest="persistent", action="store_false", help="Send non-persistent message")

    args = parser.parse_args(argv)

    # Use CLI arg -> env var -> provided default URL
    rabbit_url = (
        args.rabbit_url
        or os.environ.get("RABBITMQ_URL")
        or "amqp://adTdgQXQfnuCeyUJ:ZdsYLhbIOhdSky1g-MDAqY67hsI~E4JN@shinkansen.proxy.rlwy.net:43969"
    )
    if args.rabbit_url is None and os.environ.get("RABBITMQ_URL") is None:
        log.info("No RabbitMQ URL provided; using built-in default RABBITMQ_URL")

    payload = build_message_payload(args.user_id, args.token, args.title, args.message, args.template)

    log.info("Connecting to RabbitMQ at %s", rabbit_url)
    publish(rabbit_url, args.queue, payload, persistent=args.persistent)
    log.info("Done. Watch your Push Service logs to confirm processing.")


if __name__ == "__main__":
    main()
