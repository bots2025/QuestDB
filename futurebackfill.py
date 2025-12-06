import time
import datetime
import json
import os
from utils import future_fetch_klines, get_futures_symbols
from questdb.ingress import Sender, TimestampNanos
import requests
import pytz

QUEST_HOST = "82.29.166.107"
QUEST_PORT = 9000
conf = f"http::addr={QUEST_HOST}:{QUEST_PORT};"

INTERVAL = "1m"

timezone = pytz.timezone("Etc/UTC")
# create 'datetime' objects in UTC time zone to avoid the implementation of a local time zone offset
utc_from = datetime.datetime(2023, 1, 1, tzinfo=timezone)
utc_to = datetime.datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone)

# Date boundaries
DATE_FROM = int(utc_from.timestamp() * 1000)
DATE_TO   = int(utc_to.timestamp() * 1000)

PROGRESS_FILE = "future_progress.json"
BATCH_LIMIT = 500       # smaller batch to reduce risk of 429
REQUEST_DELAY = 0.5     # seconds between requests

def ingest_batch(sender, rows, symbol):
    for r in rows:
        open_time = r[0]
        if open_time < DATE_FROM or open_time > DATE_TO:
            continue

        sender.row(
            "futures_klines_v1",
            symbols={
                "symbol": symbol,
                "interval": INTERVAL
            },
            columns={
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": TimestampNanos(int(r[6] * 1_000_000)),
                "quote_volume": float(r[7]),
                "trades": int(r[8]),
                "taker_base_volume": float(r[9]),
                "taker_quote_volume": float(r[10]),
            },
            at=TimestampNanos(int(open_time * 1_000_000)),
        )

def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return None, None
    with open(PROGRESS_FILE, "r") as f:
        data = json.load(f)
        return data.get("symbol"), data.get("timestamp")

def save_progress(symbol, timestamp):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"symbol": symbol, "timestamp": timestamp}, f)

def fetch_with_retry(symbol, interval, start_time, end_time, limit=BATCH_LIMIT, max_retries=5):
    for attempt in range(max_retries):
        try:
            rows = future_fetch_klines(symbol, interval, start_time, end_time, limit)
            return rows
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait = 2 ** attempt
                print(f"[{symbol}] Rate limited (429). Waiting {wait}s before retry...")
                time.sleep(wait)
            elif e.response.status_code == 400:
                print(f"[{symbol}] Bad Request (400). Likely startTime > endTime. Skipping batch.")
                return []
            else:
                raise
    raise Exception(f"[{symbol}] Failed after {max_retries} retries due to rate limiting")

def futures_backfill_all():
    symbols = get_futures_symbols()
    print(f"Found {len(symbols)} futures markets")

    last_symbol, last_timestamp = load_progress()
    start_resuming = False if last_symbol else True
    batch_counter = 0

    with Sender.from_conf(conf) as sender:
        for symbol in symbols:

            if not start_resuming:
                if symbol == last_symbol:
                    start_resuming = True
                    start_time = last_timestamp
                else:
                    print(f"[{symbol}] skipped (already completed)")
                    continue
            else:
                start_time = DATE_FROM

            print(f"[{symbol}] RESUME start_time={start_time}")

            while start_time <= DATE_TO:

                rows = fetch_with_retry(
                    symbol,
                    INTERVAL,
                    start_time=start_time,
                    end_time=DATE_TO,
                    limit=BATCH_LIMIT
                )

                if not rows:
                    break

                ingest_batch(sender, rows, symbol)
                batch_counter += 1

                # Save progress
                save_progress(symbol, rows[-1][0])

                if batch_counter % 20 == 0:
                    sender.flush()

                if len(rows) < BATCH_LIMIT:
                    break

                # Update start_time for next batch
                next_start = rows[-1][6] + 1
                if next_start > DATE_TO:
                    break
                start_time = next_start

                time.sleep(REQUEST_DELAY)

        sender.flush()

    print("Historical backfill complete.")

if __name__ == "__main__":
    print("=== Starting Historical Backfill ===")
    try:
        futures_backfill_all()
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")
