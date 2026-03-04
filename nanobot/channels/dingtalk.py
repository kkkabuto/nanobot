"""DingTalk/DingDing channel implementation using Stream Mode."""

import asyncio
import json
import time
from typing import Any

from loguru import logger
import httpx

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import DingTalkConfig

try:
    from dingtalk_stream import (
        DingTalkStreamClient,
        Credential,
        CallbackHandler,
        CallbackMessage,
        AckMessage,
    )
    from dingtalk_stream.chatbot import ChatbotMessage

    DINGTALK_AVAILABLE = True
except ImportError:
    DINGTALK_AVAILABLE = False
    # Fallback so class definitions don't crash at module level
    CallbackHandler = object  # type: ignore[assignment,misc]
    CallbackMessage = None  # type: ignore[assignment,misc]
    AckMessage = None  # type: ignore[assignment,misc]
    ChatbotMessage = None  # type: ignore[assignment,misc]


class NanobotDingTalkHandler(CallbackHandler):
    """
    Standard DingTalk Stream SDK Callback Handler.
    Parses incoming messages and forwards them to the Nanobot channel.
    Supports both private (1:1) and group chat messages.
    """

    def __init__(self, channel: "DingTalkChannel"):
        super().__init__()
        self.channel = channel

    async def process(self, message: CallbackMessage):
        """Process incoming stream message."""
        try:
            # Parse using SDK's ChatbotMessage for robust handling
            chatbot_msg = ChatbotMessage.from_dict(message.data)

            # Extract text content; fall back to raw dict if SDK object is empty
            content = ""
            if chatbot_msg.text:
                content = chatbot_msg.text.content.strip()
            if not content:
                content = message.data.get("text", {}).get("content", "").strip()

            if not content:
                logger.warning(
                    "Received empty or unsupported message type: {}",
                    chatbot_msg.message_type,
                )
                return AckMessage.STATUS_OK, "OK"

            sender_id = chatbot_msg.sender_staff_id or chatbot_msg.sender_id
            sender_name = chatbot_msg.sender_nick or "Unknown"

            # Extract conversation info for group chat support
            # conversationType: 1 = private chat, 2 = group chat
            # Note: DingTalk may return conversationType=1 even for group chats,
            # so we treat any message with conversation_id as potential group message
            conversation_id = getattr(chatbot_msg, "conversation_id", None)
            conversation_type = getattr(chatbot_msg, "conversation_type", None)
            conversation_title = getattr(chatbot_msg, "conversation_title", None)

            # Determine if this is a group chat:
            # - conversation_type == 2: definitely group
            # - has conversation_id: might be group (try group API first)
            # - conversation_title is not None: likely group
            is_group = conversation_type == 2 or conversation_id is not None

            # Check if bot is mentioned (@) in group chat
            is_at = getattr(chatbot_msg, "is_in_at_list", False)

            logger.info(
                "Received DingTalk {} message (convType={}, convId={}, title={}) from {} ({}): {}",
                "group" if is_group else "private",
                conversation_type,
                conversation_id[:20] + "..." if conversation_id and len(conversation_id) > 20 else conversation_id,
                conversation_title,
                sender_name,
                sender_id,
                content,
            )

            # Forward to Nanobot via _on_message (non-blocking).
            # Store reference to prevent GC before task completes.
            task = asyncio.create_task(
                self.channel._on_message(
                    content=content,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    conversation_id=conversation_id,
                    is_group=is_group,
                    is_at=is_at,
                )
            )
            self.channel._background_tasks.add(task)
            task.add_done_callback(self.channel._background_tasks.discard)

            return AckMessage.STATUS_OK, "OK"

        except Exception as e:
            logger.error("Error processing DingTalk message: {}", e)
            # Return OK to avoid retry loop from DingTalk server
            return AckMessage.STATUS_OK, "Error"


class DingTalkChannel(BaseChannel):
    """
    DingTalk channel using Stream Mode.

    Uses WebSocket to receive events via `dingtalk-stream` SDK.
    Uses direct HTTP API to send messages (SDK is mainly for receiving).

    Supports both private (1:1) chat and group chat:
    - Private chat: uses /v1.0/robot/oToMessages/batchSend API
    - Group chat: uses /v1.0/robot/groupMessages/send API

    For group chat, the message must @mention the bot to receive it.
    """

    name = "dingtalk"

    def __init__(self, config: DingTalkConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: DingTalkConfig = config
        self._client: Any = None
        self._http: httpx.AsyncClient | None = None

        # Access Token management for sending messages
        self._access_token: str | None = None
        self._token_expiry: float = 0

        # Hold references to background tasks to prevent GC
        self._background_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Start the DingTalk bot with Stream Mode."""
        try:
            if not DINGTALK_AVAILABLE:
                logger.error(
                    "DingTalk Stream SDK not installed. Run: pip install dingtalk-stream"
                )
                return

            if not self.config.client_id or not self.config.client_secret:
                logger.error("DingTalk client_id and client_secret not configured")
                return

            self._running = True
            self._http = httpx.AsyncClient()

            logger.info(
                "Initializing DingTalk Stream Client with Client ID: {}...",
                self.config.client_id,
            )
            credential = Credential(self.config.client_id, self.config.client_secret)
            self._client = DingTalkStreamClient(credential)

            # Register standard handler
            handler = NanobotDingTalkHandler(self)
            self._client.register_callback_handler(ChatbotMessage.TOPIC, handler)

            logger.info("DingTalk bot started with Stream Mode")

            # Reconnect loop: restart stream if SDK exits or crashes
            while self._running:
                try:
                    await self._client.start()
                except Exception as e:
                    logger.warning("DingTalk stream error: {}", e)
                if self._running:
                    logger.info("Reconnecting DingTalk stream in 5 seconds...")
                    await asyncio.sleep(5)

        except Exception as e:
            logger.exception("Failed to start DingTalk channel: {}", e)

    async def stop(self) -> None:
        """Stop the DingTalk bot."""
        self._running = False
        # Close the shared HTTP client
        if self._http:
            await self._http.aclose()
            self._http = None
        # Cancel outstanding background tasks
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

    async def _get_access_token(self) -> str | None:
        """Get or refresh Access Token."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        data = {
            "appKey": self.config.client_id,
            "appSecret": self.config.client_secret,
        }

        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot refresh token")
            return None

        try:
            resp = await self._http.post(url, json=data)
            resp.raise_for_status()
            res_data = resp.json()
            self._access_token = res_data.get("accessToken")
            # Expire 60s early to be safe
            self._token_expiry = time.time() + int(res_data.get("expireIn", 7200)) - 60
            return self._access_token
        except Exception as e:
            logger.error("Failed to get DingTalk access token: {}", e)
            return None

    async def _compress_image(self, file_path: str, max_size: int = 1024 * 1024) -> str | None:
        """Compress image to be under max_size (default 1MB).

        Returns path to temporary compressed file, or None if compression fails.
        """
        import os

        try:
            from PIL import Image
            import io

            # Check current file size
            file_size = os.path.getsize(file_path)
            if file_size <= max_size:
                return file_path  # No need to compress

            # Open image
            with Image.open(file_path) as img:
                # Convert RGBA to RGB if necessary
                if img.mode == 'RGBA':
                    img = img.convert('RGB')

                # Start with original size and quality
                quality = 85
                max_dimension = 1920  # Max width/height

                # Resize if image is too large
                width, height = img.size
                if width > max_dimension or height > max_dimension:
                    ratio = min(max_dimension / width, max_dimension / height)
                    new_size = (int(width * ratio), int(height * ratio))
                    img = img.resize(new_size, Image.Resampling.LANCZOS)

                # Try progressively lower quality until under max_size
                import os
                temp_path = f"/tmp/compressed_{os.path.basename(file_path)}"
                while quality >= 40:
                    buffer = io.BytesIO()
                    img.save(buffer, format='JPEG', quality=quality, optimize=True)
                    size = buffer.tell()

                    if size <= max_size:
                        with open(temp_path, 'wb') as f:
                            f.write(buffer.getvalue())
                        logger.debug(
                            "Image compressed: {} -> {} bytes (quality={})",
                            file_size, size, quality
                        )
                        return temp_path

                    quality -= 10

                # If still too large, try with smaller dimensions
                while quality >= 40 and (width > 800 or height > 800):
                    ratio = 0.8
                    width = int(width * ratio)
                    height = int(height * ratio)
                    img = img.resize((width, height), Image.Resampling.LANCZOS)

                    buffer = io.BytesIO()
                    img.save(buffer, format='JPEG', quality=quality, optimize=True)
                    size = buffer.tell()

                    if size <= max_size:
                        with open(temp_path, 'wb') as f:
                            f.write(buffer.getvalue())
                        logger.debug(
                            "Image compressed with resize: {} -> {} bytes ({}x{})",
                            file_size, size, width, height
                        )
                        return temp_path

            logger.warning("Could not compress image under {} bytes", max_size)
            return None
        except ImportError:
            logger.warning("PIL not available for image compression")
            return None
        except Exception as e:
            logger.error("Error compressing image: {}", e)
            return None

    async def _upload_image(self, file_path: str, token: str) -> str | None:
        """Upload image to DingTalk and return mediaId.

        Uses the robot message file upload API:
        https://open.dingtalk.com/document/robots/upload-media-files
        DingTalk has a 2MB limit for image uploads.
        """
        import mimetypes
        import os

        url = "https://oapi.dingtalk.com/media/upload"
        headers = {"x-acs-dingtalk-access-token": token}

        # Check file size (DingTalk limit is 2MB, use 1.5MB to be safe)
        file_size = os.path.getsize(file_path)
        max_upload_size = 1.5 * 1024 * 1024  # 1.5MB

        upload_path = file_path
        if file_size > max_upload_size:
            logger.info("Image too large ({} bytes), attempting compression", file_size)
            compressed = await self._compress_image(file_path, max_size=max_upload_size)
            if compressed and compressed != file_path:
                upload_path = compressed
            else:
                logger.error("Image compression failed, cannot upload")
                return None

        # Guess content type
        content_type, _ = mimetypes.guess_type(upload_path)
        content_type = content_type or "image/png"

        try:
            with open(upload_path, "rb") as f:
                files = {
                    "media": (os.path.basename(upload_path), f, content_type),
                }
                # Old API uses access_token as query param
                upload_url = f"{url}?access_token={token}&type=image"
                resp = await self._http.post(upload_url, files=files)

            if resp.status_code != 200:
                logger.error("DingTalk image upload failed: {}", resp.text)
                return None

            result = resp.json()
            media_id = result.get("media_id")
            if media_id:
                logger.debug("DingTalk image uploaded: media_id={}", media_id)
            return media_id
        except Exception as e:
            logger.error("Error uploading image to DingTalk: {}", e)
            return None
        finally:
            # Clean up temp file if we created one
            if upload_path != file_path and upload_path.startswith("/tmp/"):
                try:
                    import os
                    os.remove(upload_path)
                except:
                    pass

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through DingTalk.

        Supports both private (1:1) and group chat:
        - Group chat: when metadata contains is_group=True, uses groupMessages API
        - Private chat: uses oToMessages/batchSend API

        For media (images), uploads first then sends with sampleImage msgKey.
        """
        token = await self._get_access_token()
        if not token:
            return

        headers = {"x-acs-dingtalk-access-token": token}
        # Determine if this is a group chat:
        # 1. Check metadata from MessageTool (if available)
        # 2. Fallback: DingTalk conversation IDs start with "cid" (e.g., cid5/xxx)
        is_group = msg.metadata.get("is_group", False)
        if not is_group and msg.chat_id.startswith("cid"):
            is_group = True

        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot send")
            return

        # Handle media (images) - upload and send
        if msg.media:
            for media_path in msg.media:
                media_id = await self._upload_image(media_path, token)
                if media_id:
                    # Send image embedded in markdown ( DingTalk group message supports markdown images)
                    markdown_text = f"![image]({media_id})"
                    if msg.content:
                        markdown_text = msg.content + "\n" + markdown_text

                    if is_group:
                        url = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
                        data = {
                            "robotCode": self.config.client_id,
                            "openConversationId": msg.chat_id,
                            "msgKey": "sampleMarkdown",
                            "msgParam": json.dumps({
                                "title": "Nanobot Image",
                                "text": markdown_text,
                            }, ensure_ascii=False),
                        }
                    else:
                        url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
                        data = {
                            "robotCode": self.config.client_id,
                            "userIds": [msg.chat_id],
                            "msgKey": "sampleMarkdown",
                            "msgParam": json.dumps({
                                "title": "Nanobot Image",
                                "text": markdown_text,
                            }, ensure_ascii=False),
                        }
                    try:
                        resp = await self._http.post(url, json=data, headers=headers)
                        if resp.status_code != 200:
                            logger.error("DingTalk image send failed: {}", resp.text)
                        else:
                            logger.debug(
                                "DingTalk {} image sent to {}",
                                "group" if is_group else "private",
                                msg.chat_id,
                            )
                    except Exception as e:
                        logger.error("Error sending DingTalk image: {}", e)
                else:
                    logger.error("Failed to upload image: {}", media_path)

        # Send text message if content exists (and wasn't already sent with images)
        content_sent_with_images = bool(msg.media and msg.content)
        if msg.content and not content_sent_with_images:
            if is_group:
                # Group chat: use groupMessages/send API
                # https://open.dingtalk.com/document/orgapp/the-robot-sends-a-group-message
                url = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
                data = {
                    "robotCode": self.config.client_id,
                    "openConversationId": msg.chat_id,  # conversation_id from group message
                    "msgKey": "sampleMarkdown",
                    "msgParam": json.dumps({
                        "text": msg.content,
                        "title": "Nanobot Reply",
                    }, ensure_ascii=False),
                }
                # Optional: @mention users in group
                at_user_ids = msg.metadata.get("at_user_ids", [])
                if at_user_ids:
                    data["atUserIds"] = at_user_ids
            else:
                # Private chat: use oToMessages/batchSend API
                # https://open.dingtalk.com/document/orgapp/robot-batch-send-messages
                url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
                data = {
                    "robotCode": self.config.client_id,
                    "userIds": [msg.chat_id],  # chat_id is the user's staffId
                    "msgKey": "sampleMarkdown",
                    "msgParam": json.dumps({
                        "text": msg.content,
                        "title": "Nanobot Reply",
                    }, ensure_ascii=False),
                }

            try:
                resp = await self._http.post(url, json=data, headers=headers)
                if resp.status_code != 200:
                    logger.error("DingTalk send failed: {}", resp.text)
                else:
                    logger.debug(
                        "DingTalk {} message sent to {}",
                        "group" if is_group else "private",
                        msg.chat_id,
                    )
            except Exception as e:
                logger.error("Error sending DingTalk message: {}", e)

    async def _on_message(
        self,
        content: str,
        sender_id: str,
        sender_name: str,
        conversation_id: str | None = None,
        is_group: bool = False,
        is_at: bool = False,
    ) -> None:
        """Handle incoming message (called by NanobotDingTalkHandler).

        Delegates to BaseChannel._handle_message() which enforces allow_from
        permission checks before publishing to the bus.

        Args:
            content: Message text content.
            sender_id: Sender's staff ID.
            sender_name: Sender's display name.
            conversation_id: Group conversation ID (None for private chat).
            is_group: Whether this is a group message.
            is_at: Whether the bot was @mentioned in group chat.
        """
        try:
            # For group chat, use conversation_id as chat_id; for private, use sender_id
            chat_id = conversation_id if is_group else sender_id

            # Debug logging
            logger.info(
                "DingTalk _on_message: is_group={}, conversation_id={}, sender_id={}, chat_id={}",
                is_group, conversation_id, sender_id, chat_id,
            )

            logger.info(
                "DingTalk inbound ({}): {} from {}",
                "group" if is_group else "private",
                content,
                sender_name,
            )
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=str(content),
                metadata={
                    "sender_name": sender_name,
                    "platform": "dingtalk",
                    "is_group": is_group,
                    "is_at": is_at,
                    "conversation_id": conversation_id,
                },
            )
        except Exception as e:
            logger.error("Error publishing DingTalk message: {}", e)
