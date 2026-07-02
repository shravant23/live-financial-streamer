import csv
import os

from . import config
from .schema import Filing


def save_raw(filing, data):
    # dump the raw document bytes to disk
    os.makedirs(config.RAW_DIR, exist_ok=True)
    name = f"{filing.ticker}_{filing.accession}_{filing.primary_doc}"
    path = os.path.join(config.RAW_DIR, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


def append_rows(filings):
    # append clean rows to the csv table, write the header only once
    os.makedirs(config.DATA_DIR, exist_ok=True)
    is_new = not os.path.exists(config.CSV_FILE)
    with open(config.CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(Filing.model_fields.keys())  # header from the schema
        for filing in filings:
            writer.writerow(filing.model_dump().values())
