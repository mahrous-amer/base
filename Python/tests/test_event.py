import asyncio
import json
import msgpack
import pytest
from unittest.mock import AsyncMock
from lib.event import Event


@pytest.mark.asyncio
async def test_event_publish():
    redis_mock = AsyncMock(return_value=None)
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    event_id = "12345"
    redis_mock.xadd.return_value = event_id

    result = await event.publish(redis_mock)
    assert result == event_id
    redis_mock.xadd.assert_called_once_with(
        "test_stream", {"data": event.serialize()}, maxlen=1000
    )


@pytest.mark.asyncio
async def test_event_generate_hash():
    redis_mock = AsyncMock()
    event = Event(data={"key": "value"})
    hash_value = "dummy_hash"
    redis_mock.set.return_value = True

    result = await event.generate_hash(redis_mock)
    assert result is not None
    redis_mock.set.assert_called_once()


def test_event_serialize_json():
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    serialized = event.serialize(format="json")
    assert json.loads(serialized) == {
        "stream": "test_stream",
        "action": "test_action",
        "data": {"key": "value"},
    }


def test_event_serialize_msgpack():
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    serialized = event.serialize(format="msgpack")
    deserialized = msgpack.unpackb(serialized, raw=False)
    assert deserialized == {
        "stream": "test_stream",
        "action": "test_action",
        "data": {"key": "value"},
    }


def test_event_deserialize_json():
    data = json.dumps(
        {"stream": "test_stream", "action": "test_action", "data": {"key": "value"}}
    )
    event = Event.deserialize(data, format="json")
    assert event.stream == "test_stream"
    assert event.action == "test_action"
    assert event.data == {"key": "value"}


def test_event_deserialize_msgpack():
    data = msgpack.packb(
        {"stream": "test_stream", "action": "test_action", "data": {"key": "value"}},
        use_bin_type=True,
    )
    event = Event.deserialize(data, format="msgpack")
    assert event.stream == "test_stream"
    assert event.action == "test_action"
    assert event.data == {"key": "value"}
