import asyncio
import csv
import json
import os
import time

import pytest

from src import config, dedup, worker
from src.rate_limiter import TokenBucket
from src.schema import Filing


class FakeFetcher:
    # stands in for the real Fetcher so tests never touch the network,
    # same get / get_json interface the worker expects
    def __init__(self, submissions, documents):
        self.submissions = submissions  # url -> parsed edgar json
        self.documents = documents  # url -> raw document bytes
        self.requests = []

    async def get(self, url):
        self.requests.append(url)
        return self.documents.get(url)

    async def get_json(self, url):
        self.requests.append(url)
        return self.submissions.get(url)


def edgar_json(rows):
    # minimal shape of the real edgar submissions endpoint,
    # each row is (form, accession, date, primary_doc)
    return {
        "filings": {
            "recent": {
                "form": [r[0] for r in rows],
                "accessionNumber": [r[1] for r in rows],
                "filingDate": [r[2] for r in rows],
                "primaryDocument": [r[3] for r in rows],
            }
        }
    }


def doc_url(cik, accession, doc):
    # same url the extractor builds
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{doc}"


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    # point all output paths at a temp dir so tests dont touch real data/
    monkeypatch.setattr(config, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(config, "RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setattr(config, "SEEN_FILE", str(tmp_path / "seen.json"))
    monkeypatch.setattr(config, "CSV_FILE", str(tmp_path / "filings.csv"))
    return tmp_path


def make_fetcher():
    # two tickers with a mix of forms we want and forms we dont
    aapl_cik = config.TICKERS["AAPL"]
    msft_cik = config.TICKERS["MSFT"]
    submissions = {
        config.SUBMISSIONS_URL.format(cik=aapl_cik): edgar_json([
            ("10-K", "0000320193-24-000001", "2024-11-01", "aapl-10k.htm"),
            ("8-K", "0000320193-24-000002", "2024-11-15", "aapl-8k.htm"),
            ("S-8", "0000320193-24-000003", "2024-11-20", "aapl-s8.htm"),  # form we skip
        ]),
        config.SUBMISSIONS_URL.format(cik=msft_cik): edgar_json([
            ("10-Q", "0000789019-24-000001", "2024-10-30", "msft-10q.htm"),
        ]),
    }
    documents = {
        doc_url(aapl_cik, "0000320193-24-000001", "aapl-10k.htm"): b"<html>aapl 10-K</html>",
        doc_url(aapl_cik, "0000320193-24-000002", "aapl-8k.htm"): b"<html>aapl 8-K</html>",
        doc_url(msft_cik, "0000789019-24-000001", "msft-10q.htm"): b"<html>msft 10-Q</html>",
    }
    return FakeFetcher(submissions, documents)


def test_full_pipeline_once(sandbox):
    # one full pass: poll -> extract -> download -> csv + raw files + seen.json
    fetcher = make_fetcher()
    new = asyncio.run(worker.run_once(fetcher, ["AAPL", "MSFT"], set()))

    # 3 filings match our form types, the S-8 gets filtered out
    assert len(new) == 3
    assert all(isinstance(f, Filing) for f in new)
    assert {f.form_type for f in new} == {"10-K", "8-K", "10-Q"}

    # raw documents landed on disk with the right contents
    for f in new:
        assert os.path.exists(f.local_path)
        with open(f.local_path, "rb") as fh:
            assert f.ticker.lower().encode() in fh.read()

    # csv has a header from the schema plus one row per filing
    with open(config.CSV_FILE) as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == list(Filing.model_fields.keys())
    assert len(rows) == 4
    tickers = {r[0] for r in rows[1:]}
    assert tickers == {"AAPL", "MSFT"}

    # seen.json tracks every accession we processed
    with open(config.SEEN_FILE) as fh:
        seen_on_disk = set(json.load(fh))
    assert seen_on_disk == {f.accession for f in new}


def test_second_run_downloads_nothing(sandbox):
    # restart the pipeline with the seen file from run one, nothing should redownload
    fetcher = make_fetcher()
    first = asyncio.run(worker.run_once(fetcher, ["AAPL", "MSFT"], set()))
    assert len(first) == 3

    fetcher2 = make_fetcher()
    seen = dedup.load_seen()  # reload from disk like a real restart
    second = asyncio.run(worker.run_once(fetcher2, ["AAPL", "MSFT"], seen))

    assert second == []
    # only the two submissions polls, zero document downloads
    assert len(fetcher2.requests) == 2

    # csv didnt grow
    with open(config.CSV_FILE) as fh:
        assert len(list(csv.reader(fh))) == 4


def test_max_per_ticker_cap(sandbox, monkeypatch):
    # first run against a big backlog only grabs MAX_PER_TICKER filings
    monkeypatch.setattr(config, "MAX_PER_TICKER", 2)
    cik = config.TICKERS["AAPL"]

    rows = [("8-K", f"0000320193-24-{i:06d}", "2024-11-01", f"doc{i}.htm") for i in range(10)]
    submissions = {config.SUBMISSIONS_URL.format(cik=cik): edgar_json(rows)}
    documents = {doc_url(cik, acc, doc): b"data" for _, acc, _, doc in rows}
    fetcher = FakeFetcher(submissions, documents)

    new = asyncio.run(worker.run_once(fetcher, ["AAPL"], set()))
    assert len(new) == 2

    # next run picks up the next chunk of the backlog
    new2 = asyncio.run(worker.run_once(fetcher, ["AAPL"], dedup.load_seen()))
    assert len(new2) == 2
    assert {f.accession for f in new} & {f.accession for f in new2} == set()


def test_skips_filings_without_primary_doc(sandbox):
    # some old filings have no primary document, they should just be ignored
    cik = config.TICKERS["AAPL"]
    submissions = {config.SUBMISSIONS_URL.format(cik=cik): edgar_json([
        ("10-K", "0000320193-24-000001", "2024-11-01", ""),
        ("10-Q", "0000320193-24-000002", "2024-11-02", "real.htm"),
    ])}
    documents = {doc_url(cik, "0000320193-24-000002", "real.htm"): b"data"}
    fetcher = FakeFetcher(submissions, documents)

    new = asyncio.run(worker.run_once(fetcher, ["AAPL"], set()))
    assert len(new) == 1
    assert new[0].primary_doc == "real.htm"


def test_rate_limiter_spaces_out_requests():
    # empty bucket refilling at 50/sec, 5 acquires should take at least ~0.08s
    async def drain():
        bucket = TokenBucket(rate=50, capacity=1)
        start = time.monotonic()
        for _ in range(5):
            await bucket.acquire()
        return time.monotonic() - start

    elapsed = asyncio.run(drain())
    assert elapsed >= 0.07  # 4 refills at 1/50s each, minus timer slop
