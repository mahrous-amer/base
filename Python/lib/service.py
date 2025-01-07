import asyncio
import logging
import uuid
from functools import wraps
from typing import List, Dict, Optional, Any

from lib.event import Event

logging.basicConfig(level=logging.INFO)


class Service:
    """
    Base service class for microservices with Redis-based communication.
    """

    pending_event_timeout: int = 30000
    worker_timeout: int = 30000

    def __init__(self, name: str, streams: List[str], actions: List[str], redis_conn: Any):
        self.name = name
        self.streams = streams
        self.actions = actions
        self.redis = redis_conn
        self.rpcs = []
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.worker_id: Optional[str] = None

    def generate_worker_id(self) -> str:
        """
        Generates and sets a unique worker ID.
        """
        self.worker_id = f"{self.name}-{uuid.uuid4()}"
        return self.worker_id

    @staticmethod
    def rpc(func):
        """
        Decorator for marking methods as RPC handlers.
        """
        @wraps(func)
        async def wrapper(self, args: Dict[str, Any]):
            try:
                res = await func(self, args)
                await self.redis.publish(args["auth"], str(res))
                logging.info(f"Published result {res} on {args['auth']}")
            except Exception as e:
                logging.error(f"Error in RPC function: {e}")
                raise RuntimeError("RPC function execution failed") from e
        return wrapper

    async def send_event(self, action: str, data: Optional[Dict[str, Any]] = None) -> None:
        """
        Sends an event to a Redis stream.
        """
        data = data or {}
        try:
            event = Event(stream=self.name, action=action, data=data)
            await event.publish(self.redis)
        except Exception as e:
            logging.error(f"Error sending event: {e}")
            raise RuntimeError("Failed to send event") from e

    async def create_consumer_group(self) -> None:
        """
        Creates a consumer group for each stream.
        """
        for stream in self.streams:
            try:
                groups = await self.redis.xinfo_groups(stream)
                if self.name not in [group['name'] for group in groups]:
                    await self.redis.xgroup_create(stream, self.name, id="$", mkstream=True)
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    logging.error(f"Error creating consumer group for {stream}: {e}")
                    raise RuntimeError("Failed to create consumer group") from e

    async def process_and_ack_event(self, event: Event, retries: int = 3) -> None:
        """
        Processes and acknowledges an event.
        """
        if event.action in self.actions:
            try:
                for attempt in range(retries):
                    try:
                        await self.process_event(event)
                        break
                    except Exception as e:
                        logging.warning(f"Retry {attempt + 1}/{retries} for event {event.event_id}: {e}")
                        if attempt == retries - 1:
                            await self.handle_dead_letter(event)
                            return
                await self.redis.xack(event.stream, self.name, event.event_id)
            except Exception as e:
                logging.error(f"Error processing event: {e}")
                raise RuntimeError("Failed to process and acknowledge event") from e

    async def listen(self) -> None:
        """
        Listens to events from Redis streams.
        """
        await self.create_consumer_group()
        self.generate_worker_id()
        await self.claim_and_handle_pending_events()

        while True:
            try:
                streams = {stream: ">" for stream in self.streams}
                events = await self.redis.xreadgroup(
                    self.name, self.worker_id, streams, count=1, block=0
                )
                for stream, messages in events:
                    for msg_id, raw_data in messages:
                        serialized_data = raw_data[b"data"].decode("utf-8")
                        event = Event.deserialize(serialized_data, format="json")
                        await self.process_and_ack_event(event)
            except asyncio.CancelledError:
                logging.info("Listener task cancelled.")
                await self.graceful_shutdown()
                break
            except Exception as e:
                logging.error(f"Error in listener loop: {e}")
                await asyncio.sleep(1)

    async def handle_dead_letter(self, event: Event) -> None:
        """
        Handles events that could not be processed after retries.
        """
        try:
            dead_letter_stream = f"{event.stream}-dead-letter"
            await self.redis.xadd(dead_letter_stream, {"data": event.serialize()}, maxlen=1000)
            logging.error(f"Moved event {event.event_id} to dead-letter queue: {dead_letter_stream}")
        except Exception as e:
            logging.error(f"Failed to handle dead-letter event {event.event_id}: {e}")

    async def claim_and_handle_pending_events(self, retries: int = 3) -> None:
        """
        Claims and handles pending events in streams.
        """
        for stream in self.streams:
            try:
                pending = await self.redis.xpending_range(stream, self.name, min="-", max="+", count=100)
                if pending:
                    event_ids = [entry["message_id"] for entry in pending]
                    await self.redis.xclaim(stream, self.name, self.worker_id, self.pending_event_timeout, event_ids)
                    for msg_id in event_ids:
                        event_data = await self.redis.xread(stream, {msg_id: 1})
                        if event_data:
                            try:
                                logging.info(f"Raw event data structure: {event_data}")
                                serialized_data = event_data[0][1][0][1][b"data"].decode("utf-8")
                                event = Event.deserialize(serialized_data, format="json")
                                await self.process_and_ack_event(event, retries=retries)
                            except Exception as e:
                                logging.error(f"Failed to process pending event {msg_id}: {e}")
            except Exception as e:
                logging.error(f"Error handling pending events: {e}")
                raise RuntimeError("Failed to handle pending events") from e

    async def clear_idle_workers(self) -> None:
        """
        Clears idle workers from the consumer group.
        """
        for stream in self.streams:
            try:
                consumers = await self.redis.xinfo_consumers(stream, self.name)
                for consumer in consumers:
                    if consumer["idle"] > self.worker_timeout:
                        await self.redis.xgroup_delconsumer(stream, self.name, consumer["name"])
                        logging.info(f"Removed idle worker: {consumer['name']} from stream: {stream}")
            except Exception as e:
                logging.error(f"Error clearing idle workers: {e}")
                raise RuntimeError("Failed to clear idle workers") from e

    async def graceful_shutdown(self) -> None:
        """
        Performs a graceful shutdown of the service.
        """
        try:
            await self.pubsub.close()
            logging.info("Graceful shutdown completed.")
        except Exception as e:
            logging.error(f"Error during graceful shutdown: {e}")
            raise RuntimeError("Failed to perform graceful shutdown") from e

    async def process_event(self, event: Event) -> None:
        """
        Processes an event based on the action.
        """
        if event.action in self.rpcs:
            try:
                method = getattr(self, event.action, None)
                if method:
                    await method(event.data)
                else:
                    logging.warning(f"No RPC method found for action: {event.action}")
            except Exception as e:
                logging.error(f"Error processing RPC action: {e}")
                raise RuntimeError("Failed to process RPC action") from e
        else:
            logging.info(f"Unhandled event action: {event.action}")
