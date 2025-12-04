import time
import datetime
from utils import future_fetch_klines, get_futures_symbols
from questdb.ingress import Sender, TimestampNanos

QUEST_HOST = "82.29.166.107"
QUEST_PORT = 9000
conf = f"http::addr={QUEST_HOST}:{QUEST_PORT};"

INTERVAL = "1m"

# Date boundaries
DATE_FROM = int(datetime.datetime(2023, 1, 1).timestamp() * 1000)
DATE_TO   = int(datetime.datetime(2024, 12, 31, 23, 59, 59).timestamp() * 1000)


def ingest_batch(sender, rows, symbol):
    for r in rows:
        open_time = r[0]

        # Safety filter
        if open_time < DATE_FROM or open_time > DATE_TO:
            continue

        sender.row(
            "futures_klines",
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

import json
import os

PROGRESS_FILE = "progress.json"


def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return None, None
    with open(PROGRESS_FILE, "r") as f:
        data = json.load(f)
        return data.get("symbol"), data.get("timestamp")


def save_progress(symbol, timestamp):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"symbol": symbol, "timestamp": timestamp}, f)


def futures_backfill_all():
    symbols = get_futures_symbols()
    print(f"Found {len(symbols)} futures markets")

    last_symbol, last_timestamp = load_progress()

    start_resuming = False if last_symbol else True

    batch_counter = 0

    with Sender.from_conf(conf) as sender:

        for symbol in symbols:

            # If resuming, skip symbols until we reach the last one
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

            while True:
                rows = future_fetch_klines(
                    symbol,
                    INTERVAL,
                    start_time=start_time,
                    end_time=DATE_TO,
                    limit=1500
                )

                if not rows:
                    break

                ingest_batch(sender, rows, symbol)
                batch_counter += 1

                # SAVE PROGRESS HERE ⬅️⬅️⬅️
                save_progress(symbol, rows[-1][0])

                if batch_counter % 20 == 0:
                    sender.flush()

                if len(rows) < 1500:
                    break

                start_time = rows[-1][6] + 1

                if start_time > DATE_TO:
                    break

                time.sleep(0.05)

        sender.flush()

    print("Historical backfill complete.")



if __name__ == "__main__":
    print("=== Starting Historical Backfill ===")
    try:
        futures_backfill_all()
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")
