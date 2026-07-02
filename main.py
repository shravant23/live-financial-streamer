import argparse
import asyncio
import ssl

import aiohttp
import certifi

from src import config, dedup, worker
from src.fetcher import Fetcher
from src.rate_limiter import TokenBucket


async def run(args):
    seen = dedup.load_seen()  # what we already downloaded before
    limiter = TokenBucket(config.RATE_LIMIT, config.BUCKET_SIZE)

    # mac python sometimes misses root certs, certifi fixes that
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    async with aiohttp.ClientSession(connector=connector) as session:
        fetcher = Fetcher(session, limiter)
        while True:
            new = await worker.run_once(fetcher, args.tickers, seen)
            print(f"done, {len(new)} new filings this round")
            if args.once:
                break
            # watch mode, sleep then poll again
            print(f"sleeping {args.interval}s...")
            await asyncio.sleep(args.interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="live financial filing streamer")
    parser.add_argument("--tickers", nargs="+", default=list(config.TICKERS.keys()),
                        help="which tickers to watch")
    parser.add_argument("--once", action="store_true", help="run one poll then exit")
    parser.add_argument("--interval", type=int, default=config.POLL_INTERVAL,
                        help="seconds between polls")
    args = parser.parse_args()

    # make sure we actualy know these tickers
    for t in args.tickers:
        if t not in config.TICKERS:
            parser.error(f"unknown ticker {t}, known ones: {list(config.TICKERS)}")

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("stopped by user")
