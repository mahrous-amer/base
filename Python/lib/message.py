import asyncio
import logging
import json
import hashlib
import msgpack

from typing import Optional, Dict, Any, Union

logging.basicConfig(level=logging.INFO)


class Message:
    """
    Represents a message that can be either an event or an RPC.
    """

    def __init__(
        self,
        stream: str = "",
        action: str = "",
        rpc: str = "",
        message_id: Optional[str] = None,
        transport_id: Optional[str] = None,
        who: Optional[str] = None,
        deadline: Optional[int] = None,
        data: Optional[Dict[str, Any]] = None,
        stash: Optional[Dict[str, Any]] = None,
        response: Optional[Dict[str, Any]] = None,
        trace: Optional[Dict[str, Any]] = None,
    ):
        self.stream = stream
        self.action = action
        self.rpc = rpc
        self.message_id = message_id or self._generate_message_id()
        self.transport_id = transport_id
        self.who = who or data.get("who") if data else None
        self.deadline = deadline or int(asyncio.get_event_loop().time() + 30)
        self.args = data or {}
        self.stash = stash or {}
        self.response = response or {}
        self.trace = trace or {}

    @staticmethod
    def _generate_message_id() -> str:
        """
        Generates a unique message ID using UUID4.
        """
        import uuid
        return str(uuid.uuid4())

    def validate_schema(self) -> None:
        """
        Validates the message schema to ensure consistency.
        """
        # Check required fields based on whether it's an RPC or an event
        if self.rpc:
            required_fields = ["rpc", "message_id", "who", "deadline", "args"]
        else:
            required_fields = ["message_id", "who", "deadline", "args"]

        for field in required_fields:
            if not getattr(self, field, None):
                raise ValueError(f"Message must have a '{field}' defined.")

    def as_dict(self) -> Dict[str, Any]:
        """
        Returns the message data as a dictionary.
        """
        return {
            "stream": self.stream,
            "action": self.action,
            "rpc": self.rpc,
            "message_id": self.message_id,
            "transport_id": self.transport_id,
            "who": self.who,
            "deadline": self.deadline,
            "args": self.args,
            "stash": self.stash,
            "response": self.response,
            "trace": self.trace,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Message":
        """
        Creates a Message instance from a dictionary.
        """
        return Message(
            stream=data.get("stream", ""),
            action=data.get("action", ""),
            rpc=data.get("rpc", ""),
            message_id=data.get("message_id", ""),
            transport_id=data.get("transport_id"),
            who=data.get("who", ""),
            deadline=data.get("deadline", int(asyncio.get_event_loop().time()) + 30),
            data=data.get("args", {}),
            stash=data.get("stash", {}),
            response=data.get("response", {}),
            trace=data.get("trace", {}),
        )

    def as_json(self) -> str:
        """
        Returns the message data as a JSON string.
        """
        return json.dumps(self.as_dict())

    @staticmethod
    def from_json(data: str) -> "Message":
        """
        Creates a Message instance from a JSON string.
        """
        return Message.from_dict(json.loads(data))

    def serialize(self, format: str = "json") -> Union[str, bytes]:
        """
        Serializes the message data into the specified format.
        """
        if format == "json":
            return self.as_json()
        elif format == "msgpack":
            return msgpack.packb(self.as_dict(), use_bin_type=True)
        else:
            raise ValueError("Unsupported serialization format. Use 'json' or 'msgpack'.")

    @staticmethod
    def deserialize(data: Union[str, bytes], format: str = "json") -> "Message":
        """
        Deserializes the message data from the specified format.
        """
        if format == "json":
            return Message.from_json(data)
        elif format == "msgpack":
            return Message.from_dict(msgpack.unpackb(data, raw=False))
        else:
            raise ValueError("Unsupported deserialization format. Use 'json' or 'msgpack'.")

    def passed_deadline(self) -> bool:
        """
        Checks if the message deadline has passed.
        """
        return asyncio.get_event_loop().time() > self.deadline

    def apply_encoding(self, encoding: str = "utf-8") -> None:
        """
        Encodes specific fields into JSON strings.
        """
        try:
            for field in ["args", "response", "stash", "trace"]:
                if getattr(self, field, None):
                    setattr(self, field, json.dumps(getattr(self, field)))
        except Exception as e:
            raise ValueError(f"Error encoding fields: {e}")

    def apply_decoding(self, encoding: str = "utf-8") -> None:
        """
        Decodes specific fields from JSON strings into Python objects.
        """
        try:
            for field in ["args", "response", "stash", "trace"]:
                if getattr(self, field, None):
                    setattr(self, field, json.loads(getattr(self, field)))
        except Exception as e:
            raise ValueError(f"Error decoding fields: {e}")

    async def publish(self, redis_conn, format: str = "json", maxlen: Optional[int] = 1000) -> str:
        """
        Publishes the message to a Redis stream.
        """
        logging.info(f"Publishing to stream: {self.stream} - Action: {self.action} - Data: {self.as_dict()}")
        try:
            self.validate_schema()
            serialized_data = self.serialize(format)
            return await redis_conn.xadd(self.stream, {"data": serialized_data}, maxlen=maxlen)
        except Exception as e:
            logging.error(f"Error publishing to {self.stream}: {e}")
            raise RuntimeError("Failed to publish event") from e

    async def generate_hash(self, redis_conn: Any) -> Optional[str]:
        """
        Generates a SHA-256 hash for the message data and stores it in Redis.
        """
        try:
            # Serialize the message to JSON
            serialized_data = json.dumps(self.as_dict(), default=str)
            # Compute the hash
            unique_hash = hashlib.sha256(serialized_data.encode()).hexdigest()
            # Store in Redis (optional)
            await redis_conn.set(unique_hash, serialized_data, ex=3600)
            return unique_hash
        except Exception as e:
            logging.error(f"Error generating hash for {self.stream}: {e}")
            raise

    async def receive_message(self, channel_name: str, redis_conn: Any) -> "Message":
        """
        Receives a message from a Redis pub/sub channel and returns a Message instance.
        """
        pubsub = await redis_conn.pubsub(ignore_subscribe_messages=True)  # RESP3 compatibility
        await pubsub.subscribe(channel_name)
        try:
            timeout = 5  # seconds
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < timeout:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message.get("type") == "message":
                    data = message["data"].decode("utf-8")
                    return Message.from_json(data)
            raise TimeoutError(f"No message received on channel {channel_name} within {timeout} seconds")
        except TimeoutError as e:
            logging.error(f"Error receiving message from {channel_name}: {e}")
            raise RuntimeError("Failed to receive message") from e
        finally:
            await pubsub.unsubscribe(channel_name)
