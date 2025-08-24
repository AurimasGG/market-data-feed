import json, yaml
from datetime import datetime, timezone
import os

# ---- keliai: visada rašom į <repo>/data ----
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)  # saugiklis

def load_universe(path=os.path.join(ROOT, "symbols.yaml")):
    with open(path, "r") as f:
        y = yaml.safe_load(f)
    return y["universe"]

def fetch_yfinance_one(ticker: str):
    import yfinance as yf
    t = yf.Ticker(ticker)

    df = None
    # bandome 1m intraday, tada 5m, tada 1d
    try:
        df = t.history(period="1d", interval="1m")
    except Exception:
        df = None
    if df is None or df.empty:
        try:
            df = t.history(period="5d", interval="5m")
        except Exception:
            df = None
    if df is None or df.empty:
        try:
            df = t.history(period="1mo", interval="1d")
        except Exception:
            df = None
    if df is None or df.empty:
        return None

    last = df.tail(1)
    try:
        ts = last.index[0].tz_convert("UTC").isoformat()
    except Exception:
        ts = datetime.now(timezone.utc).isoformat()

    return {"price": float(last["Close"].iloc[0]), "ts": ts}

def main():
    uni = load_universe()
    quotes = []
    for item in uni:
        res = fetch_yfinance_one(item["query"])
        if not res:
            continue
        quotes.append({
            "id": item["id"],
            "asset_class": item["asset_class"],
            "source": item["source"],
            "query": item["query"],
            **res
        })

    out = {"generated_at": datetime.now(timezone.utc).isoformat(), "quotes": quotes}

    out_path = os.path.join(DATA_DIR, "quotes.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
