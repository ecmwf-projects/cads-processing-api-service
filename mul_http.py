#!/usr/bin/python

import asyncio
import sys
import time

import httpx

endpoint = sys.argv[1]
replicas = int(sys.argv[2]) or 10


async def get_async(url):
    async with httpx.AsyncClient(timeout=60) as client:
        return await client.get(url)


urls = [
    f"http://localhost:8080/api/retrieve/v1/testing/{endpoint}" for _ in range(replicas)
]


async def launch():
    resps = await asyncio.gather(*map(get_async, urls))
    data = [resp.status_code for resp in resps]

    for status_code in data:
        print(status_code)


tm1 = time.perf_counter()

asyncio.run(launch())

tm2 = time.perf_counter()
print(f"Total time elapsed: {tm2-tm1:0.2f} seconds")
