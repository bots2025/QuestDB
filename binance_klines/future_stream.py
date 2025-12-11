import asyncio
import json
import time
import traceback
import aiohttp
from questdb.ingress import Sender, TimestampNanos
from utils import get_futures_symbols

# ============================================================
# QuestDB Sender configuration
# ============================================================
conf = "http::addr=82.29.166.107:9000;"

STREAM_INTERVAL = "1m"
SYMBOLS_PER_WS = 5  # 5 streams per WS connection

# ============================================================
# WebSocket worker
# ============================================================
async def ws_worker(session, symbols):
    """Async WS connection handling a group of symbols."""
    streams = [f"{s.lower()}@kline_{STREAM_INTERVAL}" for s in symbols]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    with Sender.from_conf(conf) as sender:
        while True:
            try:
                print(f"[WS] Connecting ({len(symbols)} symbols)...")
                async with session.ws_connect(url, heartbeat=20) as ws:
                    print("[WS] Connected:", symbols)

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if "data" in data and "k" in data["data"]:
                                    k = data["data"]["k"]
                                    sender.row(
                                            "futures_klines_v1",
                                            symbols={"symbol": k["s"], "interval": k["i"]},
                                            columns={
                                                "open": float(k["o"]),
                                                "high": float(k["h"]),
                                                "low": float(k["l"]),
                                                "close": float(k["c"]),
                                                "volume": float(k["v"]),
                                                "close_time": TimestampNanos(int(k["t"] * 1_000_000)),
                                                "quote_volume": float(k["q"]),
                                                "trades": int(k["n"]),
                                                "taker_base_volume": float(k["V"]),
                                                "taker_quote_volume": float(k["Q"]),
                                            },
                                            at=TimestampNanos(int(k["T"] * 1_000_000))
                                        )
                            except Exception:
                                traceback.print_exc()
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("[WS] Error:", msg)
                            break
                # Flush after each reconnect iteration
                sender.flush()
            except Exception as e:
                print("[WS] Exception:", e)

            print("[WS] Reconnecting in 5s...", symbols)
            await asyncio.sleep(5)


# ============================================================
# Start all streams
# ============================================================
async def start_all_streams():
    symbols = get_futures_symbols()
    total = len(symbols)
    print(f"[SYS] Found {total} futures symbols")

    # shard symbols into groups
    groups = [symbols[i:i + SYMBOLS_PER_WS] for i in range(0, total, SYMBOLS_PER_WS)]
    print(f"[SYS] Creating {len(groups)} async WebSocket tasks")

    async with aiohttp.ClientSession() as session:

        # start all WS workers
        tasks = [asyncio.create_task(ws_worker(session, grp)) for grp in groups]
        await asyncio.gather(*tasks)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=== AsyncIO Binance Futures → QuestDB Streamer ===")
    try:
        asyncio.run(start_all_streams())
    except KeyboardInterrupt:
        print("\n[SYS] Shutting down...")