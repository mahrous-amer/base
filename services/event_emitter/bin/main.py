import asyncio
import logging
from lib.message import Message
from lib.service import Service
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO)

async def main():
    # Example Redis connection (replace with actual connection)
    redis_conn = redis.from_url("redis://redis:6379", decode_responses=True)

    # Initialize the Service
    service = Service(
        name="event_emitter",
        streams=["example_stream"],
        actions=[],
        redis_conn=redis_conn,
    )

    # Emit an event
    async def emit_event():
        try:
            event = Message(stream="example_stream", action="example_action", args={"key": "value", "who": "me"}, who=service.name)
            await event.publish(redis_conn)
            logging.info("Event emitted.")
        except Exception as e:
            logging.error(f"Error emitting event: {e}")

    # Make an RPC call
    async def make_rpc_call():
        try:
            rpc = Message(stream="rpc_server", action="example_action", args={"param": "test", "who": "me"}, who=service.name, rpc=1)
            await rpc.publish(redis_conn)
            logging.info("RPC call made.")
        except Exception as e:
            logging.error(f"Error making RPC call: {e}")

    # Run both tasks
    try:
        await asyncio.gather(emit_event(), make_rpc_call())
    except Exception as e:
        logging.error(f"Unhandled exception in message emitter: {e}")
        await redis_conn.close()
        raise

if __name__ == "__main__":
    asyncio.run(main())
