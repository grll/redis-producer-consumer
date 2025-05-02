import asyncio
import json

from redis import asyncio as aioredis

from common import HIGH_PRIORITY_QUEUE, LOW_PRIORITY_QUEUE, REDIS_HOST, REDIS_PORT


async def consume():
    """Single consumer processing tasks at exactly 1 per second"""
    # Create one persistent Redis connection
    redis = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
    print("Rate-limited consumer started")

    try:
        while True:
            # Block until a task is available from either queue (high priority first)
            task_data = await redis.blpop(
                [HIGH_PRIORITY_QUEUE, LOW_PRIORITY_QUEUE], timeout=0
            )
            task = json.loads(task_data[1])

            print(f"Processing: {task['content']}")

            # Process at exactly 1 task per second
            await asyncio.sleep(1)

            # Put result directly in task's result queue with auto-expiry
            result = f"Result for {task['content']}"
            result_key = f"result:{task['id']}"
            await redis.rpush(result_key, result)

            # Set expiry on result key (30 seconds) in case producer crashes
            await redis.expire(result_key, 30)
    except asyncio.CancelledError:
        print("Consumer shutting down")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(consume())
