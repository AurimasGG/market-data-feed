import json, yaml
from datetime import datetime, timezone
import os

def load_universe(path="symbols.yaml"):
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

    return {
        "price": float(last["Close"].iloc[0]),
        "ts": ts,
    }

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

    # SAUGIKLIS #2: jei netyčia nėra katalogo – sukurti
    os.makedirs("data", exist_ok=True)

    with open("data/quotes.json", "w") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
