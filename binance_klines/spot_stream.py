# spotstream.py

import asyncio
import json
import traceback
import aiohttp
from questdb.ingress import Sender, TimestampNanos
from utils import get_spot_symbols

# ============================================================
# QuestDB Sender configuration
# ============================================================
CONF = "http::addr=82.29.166.107:9000;"

STREAM_INTERVAL = "1m"
SYMBOLS_PER_WS = 5  # symbols per websocket

# ============================================================
# Symbol Filtering — CRITICAL for Spot WebSocket
# ============================================================
def filter_spot_symbols(symbols):
    """
    Binance Spot WebSocket silently rejects connections if ANY
    symbol in the multi-stream URL is invalid.

    So we keep only symbols that are REAL spot tradable pairs.
    """
    allowed_suffixes = ("USDT", "USDC", "BTC", "ETH", "BNB")

    filtered = [
        s for s in symbols
        if s.isalpha()
        and any(s.endswith(suf) for suf in allowed_suffixes)
        and len(s) <= 12
    ]

    return filtered


# ============================================================
# WebSocket worker
# ============================================================
async def ws_worker(session, symbols):
    """Async WS connection handling a group of Spot symbols."""
    streams = [f"{s.lower()}@kline_{STREAM_INTERVAL}" for s in symbols]
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    with Sender.from_conf(CONF) as sender:
        while True:
            try:
                print(f"[WS] Connecting ({len(symbols)} symbols)...")

                async with session.ws_connect(
                    url,
                    heartbeat=20,
                    timeout=10,
                    headers={"User-Agent": "Mozilla/5.0"}
                ) as ws:

                    print("[WS] Connected:", symbols)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if "data" in data and "k" in data["data"]:
                                    k = data["data"]["k"]

                                    sender.row(
                                        "spot_klines",
                                        symbols={"symbol": k["s"], "interval": k["i"]},
                                        columns={
                                            "open": float(k["o"]),
                                            "high": float(k["h"]),
                                            "low": float(k["l"]),
                                            "close": float(k["c"]),
                                            "volume": float(k["v"]),
                                            "close_time": TimestampNanos(int(k["T"] * 1_000_000)),
                                            "quote_volume": float(k["q"]),
                                            "trades": int(k["n"]),
                                            "taker_base_volume": float(k["V"]),
                                            "taker_quote_volume": float(k["Q"]),
                                        },
                                        at=TimestampNanos(int(k["t"] * 1_000_000))
                                    )
                            except Exception:
                                traceback.print_exc()

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("[WS] Error:", msg)
                            break

                sender.flush()

            except Exception as e:
                print("[WS] Exception:", e)

            print("[WS] Reconnecting in 5s...", symbols)
            await asyncio.sleep(5)


# ============================================================
# Start all streams
# ============================================================
async def start_all_streams():
    raw_symbols = get_spot_symbols()
    print(f"[SYS] Loaded {len(raw_symbols)} raw spot symbols")

    symbols = filter_spot_symbols(raw_symbols)
    total = len(symbols)
    print(f"[SYS] Filtered to {total} Spot WebSocket-supported symbols")
    print("[SYS] Sample:", symbols[:25])

    # shard into groups
    groups = [symbols[i:i + SYMBOLS_PER_WS] for i in range(0, total, SYMBOLS_PER_WS)]
    print(f"[SYS] Creating {len(groups)} async WebSocket tasks")

    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(ws_worker(session, grp)) for grp in groups]
        await asyncio.gather(*tasks)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=== AsyncIO Binance Spot → QuestDB Streamer ===")
    try:
        asyncio.run(start_all_streams())
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")
