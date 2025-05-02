import json
import uuid

from redis import asyncio as aioredis

from common import HIGH_PRIORITY_QUEUE, LOW_PRIORITY_QUEUE, REDIS_HOST, REDIS_PORT

# Create a single Redis connection pool for the module
_redis_pool = None


async def get_redis():
    """Get or create Redis connection pool"""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_pool


async def submit_task(content, priority="low"):
    """Submit a task to the specified priority queue and await its result using BLPOP"""
    # Get Redis from pool
    redis = await get_redis()

    # Generate unique ID
    task_id = str(uuid.uuid4())
    result_key = f"result:{task_id}"

    # Create task
    task = {"id": task_id, "content": content}

    # Choose queue based on priority
    if priority == "high":
        queue = HIGH_PRIORITY_QUEUE
    else:
        queue = LOW_PRIORITY_QUEUE
    await redis.rpush(queue, json.dumps(task))
    print(f"Submitted to {queue}: {content}")

    # Wait for result with efficient blocking operation
    result_data = await redis.blpop(result_key, timeout=0)
    return result_data[1]  # Already decoded due to decode_responses=True
