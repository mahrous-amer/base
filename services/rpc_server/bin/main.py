import asyncio
import logging
from lib.service import Service
import redis.asyncio as redis

logging.basicConfig(level=logging.INFO)

async def main():
    # Example Redis connection (replace with actual connection)
    redis_conn = redis.from_url("redis://redis:6379", decode_responses=True)

    # Initialize the Service
    service = Service(
        name="rpc_server",
        streams=["example_stream", "rpc_server"],
        actions=["example_action"],
        redis_conn=redis_conn,
    )

    # Example RPC method
    @service.rpc
    async def example_action(self, args):
        logging.info(f"Processing RPC call with args: {args}")
        return {"status": "success", "data": args}

    # Start listening for events
    try:
        await service.listen()
    except Exception as e:
        logging.error(f"Unhandled exception in service: {e}")
        await redis_conn.close()
        raise

if __name__ == "__main__":
    asyncio.run(main())
