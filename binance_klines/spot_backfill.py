import datetime
import time
import json
import os
from questdb.ingress import Sender, TimestampNanos
from utils import spot_fetch_klines, get_spot_symbols

QUEST_HOST = "82.29.166.107"
QUEST_PORT = 9000
conf = f"http::addr={QUEST_HOST}:{QUEST_PORT};"

INTERVAL = "1m"

# Date boundaries
DATE_FROM = int(datetime.datetime(2022, 1, 1).timestamp() * 1000)
DATE_TO   = int(datetime.datetime(2024, 12, 31, 23, 59, 59).timestamp() * 1000)

PROGRESS_FILE = "spot_progress.json"


# ---------------- Progress helpers ----------------
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return None, None
    with open(PROGRESS_FILE, "r") as f:
        data = json.load(f)
        ts = data.get("timestamp")
        # Fix for accidentally saved seconds
        if ts is not None and ts < 10_000_000_000:
            ts *= 1000
        return data.get("symbol"), ts


def save_progress(symbol, timestamp):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"symbol": symbol, "timestamp": timestamp}, f)


# ---------------- Ingest batch ----------------
def ingest_batch(sender, rows, symbol, interval):
    for r in rows:
        open_time = r[0]

        # Safety filter
        if open_time < DATE_FROM or open_time > DATE_TO:
            continue

        sender.row(
            "spot_klines",
            symbols={"symbol": symbol, "interval": interval},
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


# ---------------- Spot backfill ----------------
def spot_backfill_all():
    symbols = get_spot_symbols()
    print(f"Found {len(symbols)} spot markets")

    last_symbol, last_timestamp = load_progress()
    resuming = last_symbol is not None
    batch_counter = 0

    print("Resume mode:", resuming)
    if resuming:
        print("Last symbol:", last_symbol)
        print("Last timestamp:", last_timestamp)

    with Sender.from_conf(conf) as sender:
        for symbol in symbols:

            # Skip symbols until we reach the last one (if resuming)
            if resuming:
                if symbol != last_symbol:
                    print(f"[{symbol}] SKIPPED (already completed before)")
                    continue
                else:
                    print(f"[{symbol}] RESUMING...")
                    # Ensure resume timestamp is within DATE_FROM
                    start_time = max(last_timestamp, DATE_FROM)
                    resuming = False
            else:
                start_time = DATE_FROM

            print(f"[{symbol}] start_time={datetime.datetime.fromtimestamp(start_time/1000)}")

            while True:
                rows = spot_fetch_klines(
                    symbol,
                    INTERVAL,
                    start_time=start_time,
                    end_time=DATE_TO,
                    limit=1500
                )

                if not rows:
                    print(f"[{symbol}] no rows returned (start_time={start_time})")
                    break

                ingest_batch(sender, rows, symbol, INTERVAL)
                batch_counter += 1

                # Save resume point based on close_time + 1 ms
                resume_point = rows[-1][6] + 1
                save_progress(symbol, resume_point)

                if batch_counter % 20 == 0:
                    sender.flush()

                # If fewer than 1500 rows → done with this symbol
                if len(rows) < 1500:
                    break

                start_time = resume_point

                # Stop if we reached DATE_TO
                if start_time > DATE_TO:
                    break

                time.sleep(0.05)

        sender.flush()

    print("Historical spot backfill complete.")


# ---------------- Main ----------------
if __name__ == "__main__":
    print("=== Starting Historical Backfill (Spot) ===")
    try:
        spot_backfill_all()
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")
