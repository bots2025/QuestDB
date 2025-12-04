import requests

BINANCE_SPOT = "https://api.binance.com"

BINANCE_FAPI = "https://fapi.binance.com"

def get_spot_symbols():
    """Return all spot trading symbols."""
    r = requests.get(BINANCE_SPOT + "/api/v3/exchangeInfo")
    r.raise_for_status()
    data = r.json()["symbols"]

    # filter to active trading markets only
    return [s["symbol"] for s in data if s["status"] == "TRADING"]

def spot_fetch_klines(symbol, interval, start_time=None, end_time=None, limit=1500):
    """Fetch one batch of klines."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    r = requests.get(BINANCE_SPOT + "/api/v3/klines", params=params)
    r.raise_for_status()
    return r.json()

def get_futures_symbols():
    """Return all USDT-M Futures symbols."""
    r = requests.get(BINANCE_FAPI + "/fapi/v1/exchangeInfo")
    r.raise_for_status()
    symbols = r.json()["symbols"]
    return [s["symbol"] for s in symbols if s["contractType"] == "PERPETUAL"]


def future_fetch_klines(symbol, interval, start_time=None, end_time=None, limit=1500):
    """Fetch one batch of klines."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    r = requests.get(BINANCE_FAPI + "/fapi/v1/klines", params=params)
    r.raise_for_status()
    return r.json()