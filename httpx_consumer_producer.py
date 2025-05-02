"""
This module defines a consumer / producer pattern for httpx requests with Redis queues.

You can run this module to spawn a consumer process that will call httpx every
`tick` seconds like:

```bash
python httpx_consumer_producer.py --tick 1
```

You can then submit tasks (httpx requests) to the consumer from any other process with:

```python
await submit_task(
    httpx_request_args=["GET", "https://www.google.com"],
    httpx_request_kwargs={"timeout": 10},
)
```
You can configure the consumer and producer with environment variables like:

```bash
# redis url defaults to redis://localhost:6379
export HTTPX_CP_REDIS_URL=redis://localhost:6379

# ordered by priority, high first (default: high,low)
export HTTPX_CP_QUEUES=high,low 
```
"""

import asyncio
import json
import logging
import os
import uuid
from typing import Any

import httpx
from redis import asyncio as aioredis

log = logging.getLogger(__name__)


if os.getenv("HTTPX_CP_REDIS_URL"):
    queues = os.getenv("HTTPX_CP_QUEUES", "high,low").split(",")
else:
    queues = ["high", "low"]


class Settings:
    redis_url: str = os.getenv("HTTPX_CP_REDIS_URL", "redis://localhost:6379")
    queues: list[str] = queues


settings = Settings()

# Create a single Redis connection pool for the module
_redis_pool = None


async def get_redis():
    """Get or create Redis connection pool"""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = await aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_pool


async def submit_request(
    *args: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Submit a task to the specified priority queue and await its result using BLPOP.

    Args:
        *args: The args to pass to `httpx.request`
        **kwargs: The kwargs to pass to `httpx.request`
        queue: The queue to submit the task to (default: low)

    Returns:
        The result of the task as a dictionary with the following keys:
        - id: str a unique identifier for the task
        - status_code: optional int the status code of the response
        - content: optional str the content of the response
        - headers: optional dict[str, str] the headers of the response
        - error: optional str the error that occurred during the task
    """
    queue = kwargs.pop("queue", "low")
    if queue not in settings.queues:
        raise ValueError(f"Queue {queue} not in {settings.queues}")

    # Get Redis from pool
    redis = await get_redis()

    # Generate unique ID
    task_id = str(uuid.uuid4())
    result_key = f"result:{task_id}"

    # Create task
    task = {
        "id": task_id,
        "httpx_request_args": args,
        "httpx_request_kwargs": kwargs,
    }

    # Choose queue based on priority
    await redis.rpush(queue, json.dumps(task))
    log.info(f"Task {task_id} submitted to queue {queue}")

    # Wait for result with efficient blocking operation
    result_data = await redis.blpop(result_key, timeout=0)
    return json.loads(result_data[1])  # already decoded due to decode_responses=True


# create a consumer process that will consume the task from the queue at a given rate
async def consume(tick: int):
    """Consume tasks by calling `httpx.request` from the queues every `tick` seconds.

    !!! Important:
        Tasks added to the queues must have the following keys:
        - id: str a unique identifier for the task
        - httpx_request_args: list[str] the args to pass to `httpx.request`
        - httpx_request_kwargs: dict[str, Any] the kwargs to pass to `httpx.request`

    Args:
        tick: The number of seconds to wait between processing tasks

    """
    redis = await aioredis.from_url(settings.redis_url)
    log.info(
        f"Consumer started with tick every {tick} seconds and queues {settings.queues}"
    )

    try:
        async with httpx.AsyncClient() as client:
            while True:
                # Block until a task is available from the queues (high priority first)
                task_data = await redis.blpop(settings.queues, timeout=0)
                task = json.loads(task_data[1])

                # if a task has no id we can't create a result key so we just skip it.
                id = task.get("id")
                if not id:
                    log.error("Task has no id, skipping.")
                    continue

                # create a result object and a result key
                result_key = f"result:{id}"
                result = {
                    "id": id,
                    "status_code": None,
                    "content": None,
                    "headers": None,
                    "error": None,
                }

                # if a task has neither httpx_request_args nor httpx_request_kwargs we
                # can't make the httpx request so we skip it but still return error.
                httpx_request_args = task.get("httpx_request_args", [])
                httpx_request_kwargs = task.get("httpx_request_kwargs", {})
                if not httpx_request_args and not httpx_request_kwargs:
                    err = f"Task {id} has neither `httpx_request_args` nor `httpx_request_kwargs`."
                    log.error(err)
                    result["error"] = err
                    await redis.rpush(result_key, json.dumps(result))
                    continue

                try:
                    log.info(
                        f"Making httpx request: {httpx_request_args} {httpx_request_kwargs}"
                    )
                    response = await client.request(
                        *httpx_request_args, **httpx_request_kwargs
                    )
                    response.raise_for_status()
                    result["status_code"] = response.status_code
                    result["content"] = response.text
                    result["headers"] = dict(response.headers)
                except Exception as e:
                    err = f"Error making httpx request: {e}"
                    log.error(err)
                    result["error"] = err
                    await redis.rpush(result_key, json.dumps(result))
                    # since the call was made we need to wait for the next tick:
                    await asyncio.sleep(tick)
                    continue

                # Put result directly in task's result queue with auto-expiry
                await redis.rpush(result_key, json.dumps(result))

                # Set expiry on result key (30 seconds) in case producer crashes
                await redis.expire(result_key, 30)

                # wait for the next tick
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info("Consumer was cancelled via asyncio.CancelledError")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tick", type=int, default=1, help="Tick rate of the consumer in seconds"
    )
    args = parser.parse_args()

    asyncio.run(consume(args.tick))
