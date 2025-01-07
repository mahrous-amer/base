import sys, os
import asyncio
from redis.asyncio import Redis
import pytest

from unittest.mock import AsyncMock, Mock
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from lib.service import Service

@pytest.fixture
def service():
    """
    Fixture for creating a Service instance.
    """
    redis_conn = AsyncMock(spec=Redis)
    service_instance = Service(
        name="test_service",
        streams=["test_stream"],
        actions=["action1", "action2"],
        redis_conn=redis_conn,
    )
    return service_instance

# Test generating worker ID
@pytest.mark.asyncio
async def test_generate_worker_id(service):
    worker_id = service.generate_worker_id()
    assert worker_id is not None

# Test RPC decorator
@pytest.mark.asyncio
async def test_rpc_decorator(service):
    """
    Test the rpc decorator of the Service class.
    """
    @service.rpc
    async def test_rpc_method(self, args):
        return args

    mock_redis = AsyncMock()
    service.redis = mock_redis

    # Call the wrapped function
    await test_rpc_method(service, {"auth": "test_channel", "key": "value"})

    # Assert that the result was published to Redis
    mock_redis.publish.assert_called_once_with("test_channel", "{'auth': 'test_channel', 'key': 'value'}")

# Test sending an event
@pytest.mark.asyncio
async def test_send_event(service):
    """
    Test the send_event method of the Service class.
    """
    service.redis.xadd = AsyncMock(return_value="event_id")
    await service.send_event("action1", {"key": "value"})
    service.redis.xadd.assert_called_once()

# Test creating a consumer group
@pytest.mark.asyncio
async def test_create_consumer_group(service):
    """
    Test the create_consumer_group method of the Service class.
    """
    service.redis.xinfo_groups = AsyncMock(return_value=[])
    service.redis.xgroup_create = AsyncMock()
    await service.create_consumer_group()
    service.redis.xgroup_create.assert_called_once_with(
        "test_stream", "test_service", id="$", mkstream=True
    )
