# HTTPX Consumer Producer with Redis

This module implements a robust, cross-process producer-consumer pattern for making HTTP requests using [httpx](https://www.python-httpx.org/) and Redis as a task queue. It allows you to submit HTTP requests from any process (producer) and have them executed by a dedicated consumer process at a controlled rate, with results returned via Redis.

## Features

- **Priority Queues:** Supports multiple queues (e.g., high, low) for prioritizing tasks.
- **Async & Cross-Process:** Producers and consumers can run in separate processes or even on different machines.
- **Result Handling:** Each task gets a unique result key; producers block until the result is available.
- **Configurable:** Redis URL and queue priorities are configurable via environment variables.
- **Rate Limiting:** The consumer processes tasks at a configurable tick interval.

## Installation

1. Clone the repository and install dependencies:
    ```bash
    git clone git@github.com:grll/redis-producer-consumer.git
    cd redis-producer-consumer
    uv sync
    ```

2. Install and start Redis (see [Redis documentation](https://redis.io/docs/getting-started/)).

## Configuration

You can configure the consumer and producer via environment variables:

- `HTTPX_CP_REDIS_URL`: Redis connection URL (default: `redis://localhost:6379`)
- `HTTPX_CP_QUEUES`: Comma-separated list of queue names, ordered by priority (default: `high,low`)

Example:
```bash
export HTTPX_CP_REDIS_URL=redis://localhost:6379
export HTTPX_CP_QUEUES=high,low
```

## How It Works

- **Producer:** Submits HTTP request tasks to a Redis queue and waits for the result.
- **Consumer:** Continuously polls the queues (by priority), executes HTTP requests using httpx, and pushes results back to Redis.

## Usage

### 1. Start the Consumer

The consumer will process tasks from the queues at a fixed interval (`--tick` seconds):

```bash
python httpx_consumer_producer.py --tick 1
```

- `--tick`: Number of seconds to wait between processing tasks (default: 1).

### 2. Submit Tasks from a Producer

You can submit tasks from any Python process:

```python
import asyncio
from httpx_consumer_producer import submit_request

async def main():
    result = await submit_request(
        "GET", "https://www.google.com",
        queue="high",  # optional, defaults to "low"
        timeout=10     # passed to httpx.request
    )
    print(result)

asyncio.run(main())
```

- `*args`: Arguments for `httpx.request` (e.g., method, URL).
- `**kwargs`: Keyword arguments for `httpx.request` (e.g., `timeout`, `headers`). You can also specify `queue` to select the priority.

The result is a dictionary:
```python
{
    "id": "<task-uuid>",
    "status_code": 200,
    "content": "...",
    "headers": {...},
    "error": None
}
```

### 3. Task Structure

Each task is a dictionary with:
- `id`: Unique identifier (auto-generated)
- `httpx_request_args`: List of positional args for `httpx.request`
- `httpx_request_kwargs`: Dict of keyword args for `httpx.request`

### 4. Error Handling

If the HTTP request fails, the result will include an `"error"` field with the error message.

## Example Workflow

1. Start the consumer:
    ```bash
    python httpx_consumer_producer.py --tick 2
    ```
2. In another process, submit a request:
    ```python
    import asyncio
    from httpx_consumer_producer import submit_request

    async def main():
        result = await submit_request("GET", "https://httpbin.org/get", timeout=5)
        print(result)

    asyncio.run(main())
    ```

## Notes

- The consumer and producers can run on different machines as long as they share the same Redis instance.
- Results are stored in Redis with a 30-second expiry to avoid stale data.
- The consumer logs all actions for easy debugging.
