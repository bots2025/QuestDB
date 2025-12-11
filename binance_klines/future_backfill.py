import time
import datetime
import json
import os
import socket
import pytz
import requests
from utils import future_fetch_klines, get_futures_symbols
from questdb.ingress import Sender, TimestampNanos

QUEST_HOST = "82.29.166.107"
QUEST_PORT = 9000
conf = f"http::addr={QUEST_HOST}:{QUEST_PORT};"

INTERVAL = "1m"

timezone = pytz.timezone("Etc/UTC")
utc_from = datetime.datetime(2023, 1, 1, tzinfo=timezone)
utc_to = datetime.datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone)

DATE_FROM = int(utc_from.timestamp() * 1000)
DATE_TO   = int(utc_to.timestamp() * 1000)

PROGRESS_FILE = "future_progress.json"
BATCH_LIMIT = 500
REQUEST_DELAY = 0.5  # seconds between requests
VOLUME_THRESHOLD = 50000  # base volume > 50k

# -------------------------------
# Helper Functions
# -------------------------------

def ingest_batch(sender, rows, symbol):
    ingested_count = 0
    skipped_count = 0
    for r in rows:
        open_time = r[0]
        if open_time < DATE_FROM or open_time > DATE_TO:
            continue

        volume = float(r[5])
        if volume <= VOLUME_THRESHOLD:  # filter: only store volume > 50,000
            skipped_count += 1
            continue

        sender.row(
            "binance_futures_klines",
            symbols={
                "symbol": symbol,
                "interval": INTERVAL
            },
            columns={
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": volume,
                "close_time": TimestampNanos(int(r[6] * 1_000_000)),
                "quote_volume": float(r[7]),
                "trades": int(r[8]),
                "taker_base_volume": float(r[9]),
                "taker_quote_volume": float(r[10]),
            },
            at=TimestampNanos(int(open_time * 1_000_000)),
        )
        ingested_count += 1
    print(f"[{symbol}] Batch processed: {ingested_count} ingested, {skipped_count} skipped")

def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return None, None
    with open(PROGRESS_FILE, "r") as f:
        data = json.load(f)
        return data.get("symbol"), data.get("timestamp")

def save_progress(symbol, timestamp):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"symbol": symbol, "timestamp": timestamp}, f)

# -------------------------------
# Fetch with retry (safe)
# -------------------------------

def fetch_with_retry(symbol, interval, start_time, end_time, limit=BATCH_LIMIT, max_retries=5):
    BINANCE_FAPI = "https://fapi.binance.com"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }

    for attempt in range(max_retries):
        try:
            # Force IPv4 to avoid Windows DNS issues
            socket.getaddrinfo = lambda host, port, *args, **kwargs: [
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', (host, port))
            ]

            r = requests.get(BINANCE_FAPI + "/fapi/v1/klines", params=params, timeout=10)
            r.raise_for_status()
            return r.json()

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

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, socket.gaierror) as e:
            wait = 2 ** attempt
            print(f"[{symbol}] Network/DNS error: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    raise Exception(f"[{symbol}] Failed after {max_retries} retries due to network/DNS errors")

# -------------------------------
# Main backfill loop
# -------------------------------

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

                save_progress(symbol, rows[-1][0])

                if batch_counter % 20 == 0:
                    sender.flush()

                if len(rows) < BATCH_LIMIT:
                    break

                next_start = rows[-1][6] + 1
                if next_start > DATE_TO:
                    break
                start_time = next_start

                time.sleep(REQUEST_DELAY)

        sender.flush()

    print("Historical backfill complete.")

# -------------------------------
# Entry point
# -------------------------------

if __name__ == "__main__":
    print("=== Starting Historical Backfill ===")
    try:
        futures_backfill_all()
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")
