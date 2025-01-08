import pytest
import json
import msgpack
import asyncio  # Import added for deadline tests
from unittest.mock import AsyncMock
from lib.message import Message


@pytest.mark.asyncio
async def test_publish_to_stream():
    redis_mock = AsyncMock()
    message = Message(
        stream="test_stream",
        action="test_action",
        rpc="test_rpc",
        who="user123",
        data={"key": "value"}
    )
    redis_mock.xadd.return_value = "event_id"

    result = await message.publish(redis_mock, format="json", maxlen=1000)
    assert result == "event_id"
    redis_mock.xadd.assert_called_once_with(
        "test_stream",
        {"data": message.as_json()},
        maxlen=1000,
    )


@pytest.mark.asyncio
async def test_generate_hash():
    redis_mock = AsyncMock()
    message = Message(
        rpc="rpc_method",
        message_id="msg_id",
        who="user",
        data={"key": "value"}
    )
    hash_value = await message.generate_hash(redis_mock)
    assert hash_value is not None
    redis_mock.set.assert_called_once_with(
        hash_value,
        json.dumps(message.as_dict(), default=str),
        ex=3600
    )


def test_serialize_json():
    message = Message(
        rpc="rpc_method",
        message_id="msg_id",
        who="user",
        data={"key": "value"}
    )
    json_data = message.serialize(format="json")
    assert json.loads(json_data) == {
        "stream": "",
        "action": "",
        "rpc": "rpc_method",
        "message_id": "msg_id",
        "transport_id": None,
        "who": "user",
        "deadline": message.deadline,
        "args": {"key": "value"},
        "stash": {},
        "response": {},
        "trace": {},
    }


def test_serialize_msgpack():
    message = Message(
        rpc="rpc_method",
        message_id="msg_id",
        who="user",
        data={"key": "value"}
    )
    packed_data = message.serialize(format="msgpack")
    deserialized_data = msgpack.unpackb(packed_data, raw=False)
    assert deserialized_data == {
        "stream": "",
        "action": "",
        "rpc": "rpc_method",
        "message_id": "msg_id",
        "transport_id": None,
        "who": "user",
        "deadline": message.deadline,
        "args": {"key": "value"},
        "stash": {},
        "response": {},
        "trace": {},
    }


def test_deserialize_json():
    json_data = json.dumps({
        "stream": "test_stream",
        "action": "test_action",
        "rpc": "rpc_method",
        "message_id": "msg_id",
        "transport_id": None,
        "who": "user",
        "deadline": 123456,
        "args": {"key": "value"},
        "stash": {},
        "response": {},
        "trace": {},
    })
    message = Message.deserialize(json_data, format="json")
    assert message.rpc == "rpc_method"
    assert message.args == {"key": "value"}


def test_deserialize_msgpack():
    packed_data = msgpack.packb({
        "stream": "test_stream",
        "action": "test_action",
        "rpc": "rpc_method",
        "message_id": "msg_id",
        "transport_id": None,
        "who": "user",
        "deadline": 123456,
        "args": {"key": "value"},
        "stash": {},
        "response": {},
        "trace": {},
    }, use_bin_type=True)
    message = Message.deserialize(packed_data, format="msgpack")
    assert message.rpc == "rpc_method"
    assert message.args == {"key": "value"}


def test_invalid_serialization_format():
    message = Message(rpc="rpc_method", message_id="msg_id", who="user", data={"key": "value"})
    with pytest.raises(ValueError, match="Unsupported serialization format"):
        message.serialize(format="invalid_format")


def test_invalid_deserialization_format():
    data = json.dumps({"rpc": "rpc_method", "message_id": "msg_id", "who": "user"})
    with pytest.raises(ValueError, match="Unsupported deserialization format"):
        Message.deserialize(data, format="invalid_format")


@pytest.mark.asyncio
async def test_passing_deadline():
    message = Message(
        rpc="rpc_method",
        message_id="msg_id",
        who="user",
        deadline=int(asyncio.get_event_loop().time()) - 1,  # Past deadline
    )
    assert message.passed_deadline() is True

    future_message = Message(
        rpc="rpc_method",
        message_id="msg_id",
        who="user",
        deadline=int(asyncio.get_event_loop().time()) + 10,  # Future deadline
    )
    assert future_message.passed_deadline() is False

