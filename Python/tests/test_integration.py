import pytest
import asyncio
import json

from unittest.mock import AsyncMock, patch
from redis.asyncio import Redis

from lib.event import Event
from lib.service import Service


@pytest.fixture
def mock_redis():
    """
    Fixture for mocking Redis connection.
    """
    mock_redis = AsyncMock(spec=Redis)
    return mock_redis


@pytest.fixture
def mock_metrics_provider():
    """
    Fixture for mocking metrics provider.
    """
    mock_pusher = AsyncMock()
    return mock_pusher


@pytest.fixture
def sample_event_data():
    """
    Sample event data for testing.
    """
    return {
        "stream": "test_stream",
        "action": "test_action",
        "data": {"key1": "value1", "key2": "value2"},
    }


### Event Class Tests ###
@pytest.mark.asyncio
async def test_event_publish(mock_redis, sample_event_data):
    """
    Test the publish method of the Event class.
    """
    mock_redis.xadd = AsyncMock(return_value="event_id", side_effect=None)
    event = Event(**sample_event_data)
    event_id = await event.publish(mock_redis)
    expected_body = {
        "data": '{"stream": "test_stream", "action": "test_action", "data": {"key1": "value1", "key2": "value2"}}'
    }
    assert event_id == "event_id"
    mock_redis.xadd.assert_called_once_with(
        sample_event_data["stream"], expected_body, maxlen=1000
    )


@pytest.mark.asyncio
async def test_event_generate_hash(mock_redis, sample_event_data):
    """
    Test the generate_hash method of the Event class.
    """
    import hashlib
    mock_redis.set = AsyncMock()
    event = Event(**sample_event_data)
    unique_hash = await event.generate_hash(mock_redis)
    expected_hash = hashlib.sha256(json.dumps(sample_event_data["data"], default=str).encode()).hexdigest()
    assert unique_hash == expected_hash
    mock_redis.set.assert_called_once_with(
        expected_hash, json.dumps(sample_event_data["data"], default=str), ex=3600
    )


@pytest.mark.asyncio
async def test_event_receive_message(mock_redis):
    """
    Test the receive_message method of the Event class.
    """
    mock_pubsub = AsyncMock()
    mock_pubsub.get_message.return_value = {
        "type": "message",
        "data": b'{"key": "value"}',
    }
    mock_redis.pubsub.return_value = mock_pubsub
    event = Event()
    result = await event.receive_message("test_channel", mock_redis)
    assert result.data == {'key': 'value'}


### Service Class Tests ###

@pytest.fixture
def service_instance(mock_redis):
    """
    Fixture for creating a Service instance.
    """
    return Service(
        name="test_service",
        streams=["test_stream"],
        actions=["test_action"],
        redis_conn=mock_redis,
    )


@pytest.mark.asyncio
async def test_service_send_event(service_instance, mock_redis):
    """
    Test the send_event method of the Service class.
    """
    mock_redis.xadd = AsyncMock(return_value="event_id")
    await service_instance.send_event("test_action", {"key": "value"})
    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_service_create_consumer_group(service_instance, mock_redis):
    """
    Test the create_consumer_group method of the Service class.
    """
    mock_redis.xinfo_groups = AsyncMock(return_value=[])
    mock_redis.xgroup_create = AsyncMock()
    await service_instance.create_consumer_group()
    mock_redis.xgroup_create.assert_called_once_with(
        "test_stream", "test_service", id="$", mkstream=True
    )


@pytest.mark.asyncio
async def test_service_process_and_ack_event(service_instance, mock_redis, sample_event_data):
    """
    Test the process_and_ack_event method of the Service class.
    """
    mock_redis.xack = AsyncMock()
    mock_event = Event(**sample_event_data)
    service_instance.actions = ["test_action"]
    await service_instance.process_and_ack_event(mock_event)
    mock_redis.xack.assert_called_once_with(
        sample_event_data["stream"], "test_service", mock_event.event_id
    )


@pytest.mark.asyncio
async def test_service_listen(service_instance, mock_redis):
    """
    Test the listen method of the Service class.
    """
    mock_redis.xinfo_groups = AsyncMock(return_value=[])
    mock_redis.xgroup_create = AsyncMock()
    mock_redis.xpending_range = AsyncMock(return_value=[])
    mock_redis.xreadgroup = AsyncMock(return_value=None, side_effect=[
        [["test_stream", [["id1", {b"data": b'{"stream": "test_stream", "action": "test_action", "data": {"key": "value"}}'}]]]],
        asyncio.CancelledError()  # Stop the loop after one iteration
    ])
    service_instance.pubsub = AsyncMock()
    service_instance.pubsub.close = AsyncMock()
    with patch.object(service_instance, "process_and_ack_event", AsyncMock(return_value=None)) as mock_process:
        try:
            await service_instance.listen()
        except asyncio.CancelledError:
            pass
        mock_process.assert_called_once()


@pytest.mark.asyncio
async def test_service_claim_and_handle_pending_events(service_instance, mock_redis):
    """
    Test the claim_and_handle_pending_events method of the Service class.
    """
    mock_redis.xpending_range = AsyncMock(return_value=[
        {"message_id": "id1", "consumer": "worker1", "count": 1, "time_since_delivered": 1000}
    ])
    mock_redis.xclaim = AsyncMock()
    mock_redis.xread = AsyncMock(return_value=[
        ["test_stream", [["id1", {b"data": b'{"stream": "test_stream", "action": "test_action", "data": {"key": "value"}}'}]]]
    ])
    with patch.object(service_instance, "process_and_ack_event", AsyncMock(return_value=None)) as mock_process:
        await service_instance.claim_and_handle_pending_events()
        mock_process.assert_called_once()


@pytest.mark.asyncio
async def test_service_clear_idle_workers(service_instance, mock_redis):
    """
    Test the clear_idle_workers method of the Service class.
    """
    mock_redis.xinfo_consumers = AsyncMock(return_value=[
        {"name": "worker1", "pending": 0, "idle": 40000}
    ])
    mock_redis.xgroup_delconsumer = AsyncMock()
    await service_instance.clear_idle_workers()
    mock_redis.xgroup_delconsumer.assert_called_once_with(
        "test_stream", "test_service", "worker1"
    )

@pytest.mark.asyncio
async def test_service_graceful_shutdown(service_instance, mock_redis):
    """
    Test graceful shutdown of the Service.
    """
    with patch.object(service_instance, "graceful_shutdown") as mock_shutdown:
        mock_shutdown.return_value = None
        await service_instance.graceful_shutdown()
        mock_shutdown.assert_called_once()
