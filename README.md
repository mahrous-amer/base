# Base

This repository provides a foundational implementation for inter-service communication using Redis. It includes two key components: the `Event` class and the `Service` class, which together enable scalable and reliable communication in distributed systems.

---

## Protocol Overview

### Event Class

The `Event` class represents a message or action to be published or consumed via Redis. It provides the following features:
- **Serialization/Deserialization**: Supports JSON and MsgPack formats.
- **Publishing**: Sends events to Redis streams.
- **Hashing**: Generates unique hashes for event data.
- **Receiving**: Listens for messages on Redis Pub/Sub channels.

#### Event Lifecycle
1. **Creation**: An event is instantiated with a stream, action, and data.
2. **Serialization**: The event is serialized into the desired format (e.g., JSON).
3. **Publishing**: The serialized event is published to a Redis stream.
4. **Consumption**: Services consume the event, deserialize it, and process the action.

---

### Service Class

The `Service` class acts as a consumer and producer of events. It provides the following features:
- **Consumer Groups**: Manages Redis consumer groups for stream processing.
- **Event Processing**: Handles events based on predefined actions.
- **Retries and Dead-Letter Queues**: Ensures reliability by retrying failed events and moving unprocessable events to a dead-letter queue.
- **Graceful Shutdown**: Cleans up resources during shutdown.

#### Service Workflow
1. **Initialization**: The service connects to Redis and sets up consumer groups.
2. **Listening**: The service listens to Redis streams for new events.
3. **Processing**: Events are deserialized and processed based on their actions.
4. **Acknowledgment**: Successfully processed events are acknowledged in Redis.
5. **Retries**: Failed events are retried up to a maximum limit.
6. **Dead-Letter Queue**: Unprocessable events are moved to a dead-letter queue.

---

## Architecture Diagram

Below is a high-level diagram `Service` class:

```plaintext
+-------------------+       +-------------------+
|                   |       |                   |
|   Producer        |       |   Consumer        |
|                   |       |                   |
+-------------------+       +-------------------+
          |                         ^
          |                         |
          v                         |
+-------------------+       +-------------------+
|                   |       |                   |
|   Redis Stream    | <---- |   Redis Consumer  |
|                   |       |                   |
+-------------------+       +-------------------+
```

---

## Usage

### Event Class Example

```python
from lib.event import Event
import asyncio
import redis.asyncio as redis

async def main():
    redis_conn = redis.Redis()
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    await event.publish(redis_conn)

asyncio.run(main())
```

### Service Class Example

```python
from lib.service import Service
import asyncio
import redis.asyncio as redis

class MyService(Service):
    async def process_event(self, event):
        print(f"Processing event: {event.action} with data {event.data}")

async def main():
    redis_conn = redis.Redis()
    service = MyService(name="test_service", streams=["test_stream"], actions=["test_action"], redis_conn=redis_conn)
    await service.listen()

asyncio.run(main())
```

---

## Running Tests

To run the unit tests, use the following command:

```bash
pytest -v
```

---