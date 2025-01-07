import hashlib
import json
import logging
import msgpack
from typing import Optional, Dict, Any, Union

logging.basicConfig(level=logging.INFO)


class Event:
    """
    Represents an event to be published or consumed via a Redis stream.
    """

    def __init__(self, stream: str = "", action: str = "", data: Optional[Dict[str, Any]] = None, event: Optional[Any] = None):
        self.stream = stream
        self.action = action
        self.data = data or {}
        self.event_id = None
        if event:
            self.parse_event(event)

    def validate_schema(self) -> None:
        """
        Validates the event schema to ensure consistency.
        """
        if not self.stream or not self.action:
            raise ValueError("Event must have a 'stream' and 'action' defined.")

    def serialize(self, format: str = "json") -> Union[str, bytes]:
        """
        Serializes the event data into the specified format.
        """
        if format == "json":
            return json.dumps({"stream": self.stream, "action": self.action, "data": self.data})
        elif format == "msgpack":
            return msgpack.packb({"stream": self.stream, "action": self.action, "data": self.data}, use_bin_type=True)
        else:
            raise ValueError("Unsupported serialization format. Use 'json' or 'msgpack'.")

    @staticmethod
    def deserialize(data: Union[str, bytes], format: str = "json") -> "Event":
        """
        Deserializes the event data from the specified format.
        """
        if format == "json":
            obj = json.loads(data)
        elif format == "msgpack":
            obj = msgpack.unpackb(data, raw=False)
        else:
            raise ValueError("Unsupported deserialization format. Use 'json' or 'msgpack'.")
        return Event(stream=obj["stream"], action=obj["action"], data=obj["data"])

    async def publish(self, redis_conn, format: str = "json") -> str:
        """
        Publishes the event to a Redis stream.
        """
        logging.info(f"Publishing to stream: {self.stream} - Action: {self.action} - Data: {self.data}")
        body = {"action": self.action, **{k: json.dumps(v, default=str) for k, v in self.data.items()}}
        try:
            self.validate_schema()
            serialized_data = self.serialize(format)
            return await redis_conn.xadd(self.stream, {"data": serialized_data}, maxlen=1000)
        except Exception as e:
            logging.error(f"Error publishing to {self.stream}: {e}")
            raise RuntimeError("Failed to publish event") from e

    async def generate_hash(self, redis_conn) -> Optional[str]:
        """
        Generates a SHA-256 hash for the event data and stores it in Redis.
        """
        try:
            unique_hash = hashlib.sha256(json.dumps(self.data, default=str).encode()).hexdigest()
            await redis_conn.set(unique_hash, json.dumps(self.data, default=str), ex=3600)
            return unique_hash
        except Exception as e:
            logging.error(f"Error generating hash for {self.stream}: {e}")
            return None

    async def receive_message(self, channel_name: str, redis_conn) -> "Event":
        """
        Receives a message from a Redis pub/sub channel.
        """
        pubsub = redis_conn.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(channel_name)
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message.get("type") == "message":
                    data = message["data"].decode('utf-8')
                    return Event(data=json.loads(data))
        except Exception as e:
            logging.error(f"Error receiving message from {channel_name}: {e}")
            raise RuntimeError("Failed to receive message") from e
        finally:
            await pubsub.unsubscribe(channel_name)
