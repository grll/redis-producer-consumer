import asyncio
from httpx_consumer_producer import submit_request, get_redis


async def main():
    try:
        # Create and await multiple tasks concurrently
        tasks = [
            submit_request(
                "GET",
                "https://httpbin.org/get",
                timeout=10,
                queue="high",
            )
            if i % 2 == 0
            else submit_request(
                "GET",
                "https://httpbin.org/get",
                timeout=10,
                queue="low",
            )
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
