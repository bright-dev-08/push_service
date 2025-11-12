"""
Template Resolver Module

Handles fetching and rendering notification templates from the Template Service.
Supports variable substitution using Jinja2 and fallback to inline payloads.

The Template Service is expected to provide a REST API:
  GET /templates/{template_code}
  Returns: { "template_id": "...", "content": {...}, "language": "en", "version": 1 }

Template structure:
  {
    "title": "Welcome, {{name}}!",
    "body": "Thanks for joining {{platform}}.",
    "data": {"link": "{{link}}"},
    "image": "{{image_url}}"
  }
"""

import httpx
import os
import config  # ensure config loads .env from project root before using getenv
from typing import Dict, Any, Optional
import json

# Template Service URL (can be set via environment or injected)
TEMPLATE_SERVICE_URL = os.getenv("TEMPLATE_SERVICE_URL", "http://template_service:8002")

# HTTP client timeout (seconds)
TEMPLATE_REQUEST_TIMEOUT = 5


async def render_template(
    template_code: str,
    variables: Dict[str, Any],
    language: str = "en",
) -> Optional[Dict[str, Any]]:
    """
    Fetch a template from the Template Service and render it with variables.

    Args:
        template_code: Template identifier (e.g., "welcome_v1")
        variables: Dictionary of variables for substitution (e.g., {"name": "John", "platform": "iOS"})
        language: Language code (default "en")

    Returns:
        Rendered template as dict with keys: title, body, data, image
        Or None if template not found / resolution fails
    """
    try:
        # Fetch template from Template Service
        async with httpx.AsyncClient(timeout=TEMPLATE_REQUEST_TIMEOUT) as client:
            resp = await client.get(
                f"{TEMPLATE_SERVICE_URL}/templates/{template_code}",
                params={"language": language}
            )
            if resp.status_code != 200:
                print(f"❌ Template Service returned {resp.status_code} for template {template_code}")
                return None

            template_data = resp.json()
            content = template_data.get("content", {})

            # Render template content with variables using simple string substitution
            # (Production would use Jinja2 for robustness)
            rendered = _render_content(content, variables)
            return rendered

    except httpx.TimeoutException:
        print(f"⏱️ Template Service timeout fetching {template_code}")
        return None
    except Exception as e:
        print(f"❌ Error resolving template {template_code}: {e}")
        return None


def _render_content(content: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simple template rendering: substitute {{key}} with values from variables dict.

    Args:
        content: Template content dict (may have nested structures)
        variables: Variables for substitution

    Returns:
        Rendered content dict
    """
    rendered = {}

    for key, value in content.items():
        if isinstance(value, str):
            # Simple {{variable}} substitution
            rendered_value = value
            for var_key, var_value in variables.items():
                placeholder = f"{{{{{var_key}}}}}"  # {{var_key}}
                rendered_value = rendered_value.replace(placeholder, str(var_value))
            rendered[key] = rendered_value

        elif isinstance(value, dict):
            # Recursively render nested dicts (e.g., "data" field)
            rendered[key] = _render_content(value, variables)

        elif isinstance(value, list):
            # Handle lists of values
            rendered[key] = [
                _render_content(item, variables) if isinstance(item, dict) else item
                for item in value
            ]

        else:
            # Non-string, non-dict values (numbers, booleans, None) pass through
            rendered[key] = value

    return rendered


async def resolve_push_payload(
    event: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Resolve the final push payload for an event.

    If the event has an inline `payload` field (title/body/data/image),
    use that directly. Otherwise, if it has a `template_code`, fetch and
    render the template from the Template Service.

    Args:
        event: Notification event (must have "payload" OR "template_code")

    Returns:
        Final payload dict with keys: title, body, data, image
        Or None if resolution fails and no fallback is available
    """
    # If inline payload provided, use it directly (already in correct format)
    if "payload" in event and event["payload"]:
        return event["payload"]

    # If template_code provided, resolve from Template Service
    if "template_code" in event and event["template_code"]:
        template_code = event["template_code"]
        variables = event.get("variables", {})
        language = event.get("language", "en")

        rendered = await render_template(template_code, variables, language)
        if rendered:
            return rendered

        print(f"⚠️ Could not resolve template {template_code}; no payload provided.")
        return None

    # No payload and no template; cannot proceed
    print("❌ Event has neither inline payload nor template_code.")
    return None
