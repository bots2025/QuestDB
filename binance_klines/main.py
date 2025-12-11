from spotbackfill import spot_backfill_all
from futuresbackfill import futures_backfill_all
from stream import start_stream

if __name__ == "__main__":
    print("=== Starting Historical Backfill ===")
    future_backfill_all()

    print("=== Starting Historical Backfill (Spot) ===")
    spot_backfill_all()

    print("=== Starting Real-Time Streaming ===")
    start_stream()
