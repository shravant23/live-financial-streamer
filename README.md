# Live Financial Streamer

CLI app that watches public tech stocks, polls SEC Edgar for new filings (10-K, 10-Q, 8-K), downloads the raw documents concurrently and flattens them into a clean csv table.

## Architecture

```mermaid
flowchart LR
    CLI["main.py<br/><i>CLI entrypoint</i>"] --> W["worker.py<br/><i>polls all tickers<br/>concurrently</i>"]

    subgraph FETCH [" fetch layer "]
        direction TB
        F["fetcher.py<br/><i>retries + backoff</i>"] -.- RL["rate_limiter.py<br/><i>token bucket</i>"]
    end

    W --> F
    F <--> SEC[("SEC EDGAR")]

    W --> E["extractor.py<br/><i>json → Filing rows</i>"]
    E --> D["dedup.py<br/><i>skips seen filings</i>"]
    D --> WR["writer.py<br/><i>csv + raw docs</i>"]

    subgraph OUT [" data/ "]
        direction TB
        CSV[("filings.csv")]
        RAW[("raw/")]
        SEEN[("seen.json")]
    end

    WR --> CSV
    WR --> RAW
    D --> SEEN

    style SEC fill:#e8f0fe,stroke:#4285f4,color:#1a3c7a
    style CSV fill:#e6f4ea,stroke:#34a853,color:#1e4620
    style RAW fill:#e6f4ea,stroke:#34a853,color:#1e4620
    style SEEN fill:#e6f4ea,stroke:#34a853,color:#1e4620
```

## How it works

- `asyncio` + `aiohttp` for concurrent downloads, all tickers polled at the same time
- `pydantic` schema (`src/schema.py`) so every row in the table has exact predictable types
- token bucket rate limiter (`src/rate_limiter.py`) to stay under the SEC 10 req/sec limit
- exponential backoff retries in the fetcher for 429s / 5xx / network errors
- dedup file so restarts dont re-download filings we already have

## Setup

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```
# poll every ticker once and exit
python main.py --once

# just some tickers
python main.py --once --tickers AAPL NVDA

# watch mode, polls every 60s untill you ctrl-c
python main.py --interval 60
```

Output lands in `data/`:
- `data/filings.csv` — the structured table
- `data/raw/` — the raw downloaded documents
- `data/seen.json` — accession numbers already proccessed

Note: each run grabs at most 5 new filings per ticker so the first run doesnt hammer the SEC, it catches up on older ones over the next runs.

## Tests

```
python -m pytest tests/ -v
```

End-to-end tests run the whole pipeline against a fake fetcher (no network), covering the csv output, raw downloads, dedup across restarts, the per-ticker cap and the rate limiter.
