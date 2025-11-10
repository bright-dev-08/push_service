from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class NotificationEvent(BaseModel):
    """Schema for the message consumed from the push.queue."""
    event_id: UUID = Field(description="Unique ID for tracking the notification.")
    user_id: int
    template_id: str
    payload: dict
    created_at: datetime
    
class TokenValidation(BaseModel):
    token: str = Field(description="Device push token (FCM/APNs).")
    is_valid: bool
    last_validated: datetime

class HealthStatus(BaseModel):
    service: str = "Push Service"
    status: str
    rabbit_connected: bool
    redis_connected: bool
    timestamp: datetime