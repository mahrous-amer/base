# Base

This repository provides a foundational implementation for inter-service communication using Redis. It includes two key components: the `Event` class and the `Service` class, which together enable scalable and reliable communication in distributed systems.

---

## Protocol Overview

### Message Class

The `Message` class represents a message that can be either an event or an RPC. It provides the following features:
- **Serialization/Deserialization**: Supports JSON and MsgPack formats.
- **Publishing**: Sends messages to Redis streams.
- **Hashing**: Generates SHA-256 hashes for message data.
- **Receiving**: Listens for messages on Redis Pub/Sub channels.
- **Validation**: Ensures the message schema is consistent.
- **Encoding/Decoding**: Encodes and decodes fields like `args`, `response`, `stash`, and `trace`.

#### Message Lifecycle
1. **Creation**: A message is instantiated with attributes like `stream`, `action`, and `data`.
2. **Serialization**: The message is serialized into the desired format (e.g., JSON or MsgPack).
3. **Publishing**: The serialized message is published to a Redis stream.
4. **Consumption**: Services consume the message, deserialize it, and process the action.
5. **Hashing**: A unique hash is generated for the message data and stored in Redis.
6. **Receiving**: Messages are received from Redis Pub/Sub channels and deserialized.

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
|     Producer      |       |     Consumer      |
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

### Message Class Example

```python
from lib.message import Message
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

## Python Directory Overview

The `Python` directory contains the core implementation of the repository. It is structured as follows:

- **`lib/`**: Contains the main library files:
  - `message.py`: Handles message related operations.
  - `service.py`: Implements the service logic for event processing.
- **`tests/`**: Includes unit tests for the library:
  - `test_message.py`: Tests for `message.py`.
  - `test_service.py`: Tests for `service.py`.
  - `test_integration.py`: Integration tests for the overall functionality.
- **Configuration Files**:
  - `requirements.txt`: Lists the Python dependencies.
  - `pytest.ini`: Configuration for the pytest framework.

This directory is designed to provide a modular and testable implementation of the repository's functionality.

---

## Running Tests

To run the unit tests, use the following command:

```bash
cd Python && \
docker build -t python-base-test -f Dockerfile.test . && \
docker run --rm -it --entrypoint /bin/bash -v ./:/app/ python-base-test
```

---
