import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from lib.service import Service
from lib.event import Event


@pytest.mark.asyncio
async def test_service_send_event():
    redis_mock = AsyncMock()
    service = Service(name="test_service", streams=[], actions=[], redis_conn=redis_mock)
    event_id = "12345"
    redis_mock.xadd.return_value = event_id

    await service.send_event(action="test_action", data={"key": "value"})
    redis_mock.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_service_create_consumer_group():
    redis_mock = AsyncMock()
    redis_mock.xinfo_groups.return_value = []
    service = Service(name="test_service", streams=["test_stream"], actions=[], redis_conn=redis_mock)

    await service.create_consumer_group()
    redis_mock.xgroup_create.assert_called_once_with("test_stream", "test_service", id="$", mkstream=True)


@pytest.mark.asyncio
async def test_service_process_and_ack_event():
    redis_mock = AsyncMock()
    service = Service(name="test_service", streams=[], actions=["test_action"], redis_conn=redis_mock)
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    event.event_id = "12345"

    with patch.object(service, "process_event", new=AsyncMock(return_value=None)) as mock_process_event:
        await service.process_and_ack_event(event)
        mock_process_event.assert_called_once_with(event)
        redis_mock.xack.assert_called_once_with("test_stream", "test_service", "12345")


@pytest.mark.asyncio
async def test_service_handle_dead_letter():
    redis_mock = AsyncMock()
    service = Service(name="test_service", streams=[], actions=[], redis_conn=redis_mock)
    event = Event(stream="test_stream", action="test_action", data={"key": "value"})
    event.event_id = "12345"

    await service.handle_dead_letter(event)
    redis_mock.xadd.assert_called_once_with(
        "test_stream-dead-letter", {"data": event.serialize()}, maxlen=1000
    )


@pytest.mark.asyncio
async def test_service_claim_and_handle_pending_events():
    redis_mock = AsyncMock()
    redis_mock.xpending_range.return_value = [{"message_id": "12345"}]
    redis_mock.xread.return_value = [("test_stream", [("12345", {b"data": b'{"stream": "test_stream", "action": "test_action", "data": {"key": "value"}}'})])]
    service = Service(name="test_service", streams=["test_stream"], actions=["test_action"], redis_conn=redis_mock)

    with patch.object(service, "process_and_ack_event", new=AsyncMock(return_value=None)) as mock_process_and_ack_event:
        await service.claim_and_handle_pending_events()
        mock_process_and_ack_event.assert_called_once()


@pytest.mark.asyncio
async def test_service_clear_idle_workers():
    redis_mock = AsyncMock()
    redis_mock.xinfo_consumers.return_value = [{"name": "worker1", "idle": 40000}]
    service = Service(name="test_service", streams=["test_stream"], actions=[], redis_conn=redis_mock)

    await service.clear_idle_workers()
    redis_mock.xgroup_delconsumer.assert_called_once_with("test_stream", "test_service", "worker1")
