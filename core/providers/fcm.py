import json
import os
import asyncio
import firebase_admin
from firebase_admin import credentials, messaging
from typing import Optional, Dict, Any

class FCMProvider:
    """Firebase Cloud Messaging provider for sending push notifications."""
    
    @staticmethod
    def _get_credentials():
        """Get Firebase credentials from environment or file."""
        # First try to get credentials from environment variable
        firebase_creds_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
        if firebase_creds_json:
            try:
                # Parse the JSON string from environment variable
                creds_dict = json.loads(firebase_creds_json)
                return credentials.Certificate(creds_dict)
            except Exception as e:
                print(f"Failed to parse FIREBASE_CREDENTIALS_JSON: {e}")
        
        # Fallback to file-based credentials
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_path and os.path.exists(creds_path):
            return credentials.Certificate(creds_path)
            
        raise ValueError("No Firebase credentials found in environment or file")

    def __init__(self):
        """Initialize FCM with credentials from environment or file."""
        try:
            cred = self._get_credentials()
            firebase_admin.initialize_app(cred)
            print("🔥 FCM Provider initialized successfully")
        except Exception as e:
            print(f"❌ FCM initialization failed: {e}")
            raise

    async def send_notification(
        self,
        token: str,
        title: str,
        body: str,
        data: Optional[Dict[str, Any]] = None,
        image: Optional[str] = None
    ) -> bool:
        """
        Send a push notification to a specific device token.
        
        Args:
            token (str): The FCM registration token
            title (str): Notification title
            body (str): Notification body text
            data (dict, optional): Additional data payload
            image (str, optional): URL of an image to display
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            # Construct notification
            notification = messaging.Notification(
                title=title,
                body=body,
                image=image
            )

            # Create message
            message = messaging.Message(
                notification=notification,
                data=data or {},
                token=token
            )

            # messaging.send is synchronous/blocking; run it in a threadpool to avoid blocking the event loop
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, messaging.send, message)
            print(f"✅ Successfully sent notification: {response}")
            return True

        except Exception as e:
            # messaging.ApiCallError and other failures will be captured here
            print(f"❌ FCM send failed: {e}")
            return False