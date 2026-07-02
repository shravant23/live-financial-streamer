import asyncio
import json

import aiohttp

from . import config


class Fetcher:
    # small wrapper around aiohttp that adds rate limiting and retries

    def __init__(self, session, limiter):
        self.session = session
        self.limiter = limiter

    async def get(self, url):
        # returns raw bytes, retries with exponential backoff on failures
        delay = 1
        for attempt in range(config.MAX_RETRIES):
            await self.limiter.acquire()  # wait for a token before every request
            try:
                headers = {"User-Agent": config.USER_AGENT}
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    if resp.status == 429 or resp.status >= 500:
                        # server is overloaded or rate limiting us, back off
                        print(f"got {resp.status} for {url}, retrying in {delay}s")
                    else:
                        # 404 and friends, retrying wont help
                        print(f"error {resp.status} for {url}, giving up")
                        return None
            except aiohttp.ClientError as e:
                # network hiccup, worth a retry
                print(f"network error {e}, retrying in {delay}s")
            await asyncio.sleep(delay)
            delay *= 2  # backoff doubles each attempt

        print(f"gave up on {url} after {config.MAX_RETRIES} trys")
        return None

    async def get_json(self, url):
        # same as get but parses the json for you
        data = await self.get(url)
        if data is None:
            return None
        return json.loads(data)
