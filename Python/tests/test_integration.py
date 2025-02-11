import pytest
import json
import hashlib

from unittest.mock import AsyncMock, patch

from lib.service import Service
from lib.message import Message

@pytest.mark.asyncio
async def test_event_publish():
    redis_mock = AsyncMock()
    message = Message(
        stream="test_stream",
        action="test_action",
        rpc="test_rpc",
        message_id="msg123",
        who="test_user",
        args={"key1": "value1", "key2": "value2"},
    )
    redis_mock.xadd = AsyncMock(return_value="event_id")


    result = await message.publish(redis_mock, maxlen=1000)
    assert result == "event_id"
    redis_mock.xadd.assert_called_once_with(
        "test_stream",
        {
            "data": message.serialize(format="json"),
        },
        maxlen=1000,
    )


@pytest.mark.asyncio
async def test_event_generate_hash():
    redis_mock = AsyncMock()
    message = Message(
        stream="test_stream",
        action="test_action",
        rpc="test_rpc",
        message_id="msg123",
        who="test_user",
        args={"key1": "value1", "key2": "value2"},
    )
    unique_hash = await message.generate_hash(redis_mock)

    expected_data = json.dumps(message.as_dict(), default=str)
    expected_hash = hashlib.sha256(expected_data.encode()).hexdigest()

    assert unique_hash == expected_hash

@pytest.mark.asyncio
async def test_service_send_event():
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value="event_id")

    service = Service(name="test_service", streams=[], actions=[], redis_conn=redis_mock)

    await service.send_event(
        action="test_action",
        data={"key": "value"},
    )

    redis_mock.xadd.assert_called_once()

@pytest.mark.asyncio
async def test_service_process_and_ack_event():
    redis_mock = AsyncMock()
    redis_mock.xack = AsyncMock(return_value="ack_id")

    service = Service(name="test_service", streams=[], actions=["test_action"], redis_conn=redis_mock)
    message = Message(
        stream="test_stream",
        action="test_action",
        rpc="test_rpc",
        message_id="msg123",
        who="test_user",
        args={"key": "value"},
    )
    message.event_id = "event_id"

    with patch.object(service, "process_event", new=AsyncMock(return_value=None)) as mock_process_event:
        await service.process_and_ack_event(message)
        mock_process_event.assert_called_once_with(message)
        redis_mock.xack.assert_called_once_with("test_stream", "test_service", "event_id")
