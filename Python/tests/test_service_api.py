import pytest
from unittest.mock import AsyncMock, patch
from lib.service import Service
from lib.message import Message


@pytest.mark.asyncio
async def test_send_event():
    redis_mock = AsyncMock()
    service = Service(name="test_service", streams=[], actions=[], redis_conn=redis_mock)
    event_id = "12345"
    redis_mock.xadd.return_value = event_id

    await service.send_event(
        action="test_action",
        data={"key": "value"},
        maxlen=1000,
    )
    redis_mock.xadd.assert_called_once()
    assert redis_mock.xadd.call_args[0][0] == "test_service"
    assert "data" in redis_mock.xadd.call_args[0][1]
    assert "test_action" in redis_mock.xadd.call_args[0][1]["data"]


@pytest.mark.asyncio
async def test_process_event():
    redis_mock = AsyncMock()
    service = Service(name="test_service", streams=[], actions=["test_action"], redis_conn=redis_mock)
    message = Message(
        stream="test_stream",
        action="test_action",
        rpc="test_rpc",
        message_id="msg123",
        who="test_user",
        data={"key": "value"},
    )
    message.event_id = "12345"

    with patch.object(service, "process_event", new=AsyncMock(return_value=None)) as mock_process_event:
        await service.process_and_ack_event(message)
        mock_process_event.assert_called_once_with(message)
        redis_mock.xack.assert_called_once_with("test_stream", "test_service", "12345")
