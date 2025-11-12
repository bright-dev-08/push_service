from datetime import datetime, timezone, timedelta
from test_message import build_test_message


def test_build_test_message_structure():
    msg = build_test_message(user_id=42)

    assert isinstance(msg, dict)
    assert msg["user_id"] == 42
    assert "event_id" in msg and isinstance(msg["event_id"], str)
    assert msg["template_id"] == "test_notification"
    assert "payload" in msg and isinstance(msg["payload"], dict)
    assert "created_at" in msg

    # parse created_at and ensure it's timezone-aware UTC
    dt = datetime.fromisoformat(msg["created_at"])
    assert dt.tzinfo is not None
    # utcoffset for UTC should be zero
    assert dt.utcoffset() == timedelta(0)
