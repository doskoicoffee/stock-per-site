import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

CSV_FILE = "tickers.csv"
DATA_FILE = "data.json"
OUTPUT_DIR = Path("sector_data")
SERIES_DIR = OUTPUT_DIR / "series"

DAYS = 200
W_DAYS = 5
M_DAYS = 21
H_DAYS = 126

MAX_RETRIES = 2
RETRY_SLEEP_SECONDS = 8
SLEEP_BETWEEN_TICKERS = 0.2

MARKET_ALIASES = {
    "プライム": "プライム",
    "スタンダード": "スタンダード",
    "グロース": "グロース",
    "PRO Market": "PRO Market"
}

PERIODS = {
    "1w": W_DAYS,
    "1m": M_DAYS,
    "6m": H_DAYS
}


def normalize_market(value):
    if not value:
        return "不明"
    for key, name in MARKET_ALIASES.items():
        if key in value:
            return name
    return value


def load_market_caps():
    if not Path(DATA_FILE).exists():
        return {}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    caps = {}
    for stock in data.get("stocks", []):
        code = str(stock.get("code"))
        cap = stock.get("financial", {}).get("market_cap_oku")
        caps[code] = cap
    return caps


def fetch_history(code):
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            hist = yf.Ticker(code + ".T").history(period=f"{DAYS}d", interval="1d", auto_adjust=False)
            if hist is not None and not hist.empty:
                return hist
        except Exception as e:
            last_error = e
            msg = str(e)
            if "Too Many Requests" in msg or "Rate limited" in msg:
                wait = RETRY_SLEEP_SECONDS * (attempt + 1)
                time.sleep(wait)
                continue
        time.sleep(2)
    if last_error:
        print("取得失敗:", code, last_error)
    return None


def normalize_history(hist):
    df = hist.reset_index()
    if "Date" in df.columns:
        date_col = "Date"
    elif "Datetime" in df.columns:
        date_col = "Datetime"
    else:
        date_col = df.columns[0]
    if "Close" not in df.columns:
        return pd.DataFrame(columns=["date", "close"])
    df = df.rename(columns={date_col: "date", "Close": "close"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    return df.sort_values("date")


def load_cached_history(code):
    path = SERIES_DIR / f"{code}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if "date" not in df.columns or "close" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    return df.sort_values("date")


def save_history(code, df):
    path = SERIES_DIR / f"{code}.csv"
    df.to_csv(path, index=False)


def pick_close(df, offset):
    if df is None or df.empty:
        return None
    if len(df) <= offset:
        return None
    return float(df.iloc[-(offset + 1)]["close"])


def calc_pct(current, base):
    if current is None or base is None or base == 0:
        return None
    return round((current / base - 1) * 100, 2)


def series_pct_to_current(df, days):
    if df is None or df.empty:
        return []
    span = days + 1
    if len(df) > span:
        slice_df = df.iloc[-span:]
    else:
        slice_df = df
    current = float(slice_df.iloc[-1]["close"])
    out = []
    for _, row in slice_df.iterrows():
        close = row["close"]
        if close is None or close == 0:
            pct = None
        else:
            pct = round((current / close - 1) * 100, 2)
        out.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "pct": pct
        })
    return out


def compute_market_return(stocks, period):
    values = []
    weights = []
    for stock in stocks:
        ret = stock["returns"].get(period)
        if ret is None:
            continue
        cap = stock.get("market_cap_oku")
        values.append(ret)
        if cap is not None and cap > 0:
            weights.append((ret, cap))
    if weights:
        total = sum(w for _, w in weights)
        if total > 0:
            return round(sum(v * w for v, w in weights) / total, 2)
    if values:
        return round(sum(values) / len(values), 2)
    return None


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SERIES_DIR.mkdir(parents=True, exist_ok=True)

    caps = load_market_caps()
    df = pd.read_csv(CSV_FILE)

    industries = {}

    for _, row in df.iterrows():
        code_raw = str(row["code"]).strip()
        code = code_raw.zfill(4) if code_raw.isdigit() else code_raw
        name = str(row["name"]).strip()
        market = normalize_market(str(row["market"]))
        industry = str(row["industry33"]).strip()

        hist = fetch_history(code)
        if hist is None or hist.empty:
            cached = load_cached_history(code)
            if cached is None or cached.empty:
                continue
            df_hist = cached
        else:
            df_hist = normalize_history(hist)
            if not df_hist.empty:
                save_history(code, df_hist)

        if df_hist is None or df_hist.empty:
            continue

        current_close = float(df_hist.iloc[-1]["close"])

        returns = {
            "1w": calc_pct(current_close, pick_close(df_hist, W_DAYS)),
            "1m": calc_pct(current_close, pick_close(df_hist, M_DAYS)),
            "6m": calc_pct(current_close, pick_close(df_hist, H_DAYS))
        }

        series = {
            "1w": series_pct_to_current(df_hist, W_DAYS),
            "1m": series_pct_to_current(df_hist, M_DAYS),
            "6m": series_pct_to_current(df_hist, H_DAYS)
        }

        stock = {
            "code": code,
            "name": name,
            "market": market,
            "industry": industry,
            "market_cap_oku": caps.get(code),
            "current_close": round(current_close, 2),
            "returns": returns,
            "series": series
        }

        industries.setdefault(industry, {"name": industry, "markets": {}})

        for key in (market, "all"):
            bucket = industries[industry]["markets"].setdefault(key, {
                "market_cap": 0,
                "returns": {"1w": None, "1m": None, "6m": None},
                "stocks": []
            })
            cap = stock.get("market_cap_oku")
            if cap is not None:
                bucket["market_cap"] += cap
            bucket["stocks"].append(stock)

        time.sleep(SLEEP_BETWEEN_TICKERS)

    # 集計
    for industry in industries.values():
        for market_key, bucket in industry["markets"].items():
            bucket["stocks"].sort(key=lambda s: (s.get("market_cap_oku") or 0), reverse=True)
            bucket["returns"]["1w"] = compute_market_return(bucket["stocks"], "1w")
            bucket["returns"]["1m"] = compute_market_return(bucket["stocks"], "1m")
            bucket["returns"]["6m"] = compute_market_return(bucket["stocks"], "6m")
            bucket["market_cap"] = round(bucket["market_cap"], 2)

    result = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "markets": ["all", "プライム", "スタンダード", "グロース", "PRO Market"],
        "periods": {"1w": "週次", "1m": "月次", "6m": "半年"},
        "industries": list(industries.values())
    }

    with open(OUTPUT_DIR / "sector.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
