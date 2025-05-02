import asyncio
from producer import submit_task, get_redis


async def main():
    try:
        # Create and await multiple tasks concurrently
        tasks = [
            submit_task(f"Process1 HIGH task {i}", priority="high")
            if i % 2 == 0
            else submit_task(f"Process1 LOW task {i}", priority="low")
            for i in range(5)
        ]

        results = await asyncio.gather(*tasks)

        # Print results
        for i, result in enumerate(results):
            print(f"Process1 got result {i}: {result}")
    finally:
        # Get the Redis connection pool and close it
        redis = await get_redis()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
