import json
import os
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from pandas_datareader import data as web

OUTPUT_DIR = Path("market_data")
SERIES_DIR = OUTPUT_DIR / "series"
DAYS = 183

FRED_SERIES = [
    {"id": "NIKKEI225", "label": "日経225", "unit": "Index"},
    {"id": "SP500", "label": "S&P500", "unit": "Index"},
    {"id": "DJIA", "label": "NYダウ", "unit": "Index"},
    {"id": "NASDAQCOM", "label": "NASDAQ", "unit": "Index"},
    {"id": "VIXCLS", "label": "VIX", "unit": "Index"}
]

YF_SERIES = [
    {"id": "USDJPY", "label": "米ドル/円", "unit": "JPY per USD", "ticker": "JPY=X"},
    {"id": "EURJPY", "label": "ユーロ/円", "unit": "JPY per EUR", "ticker": "EURJPY=X"},
    {"id": "EURUSD", "label": "ユーロ/米ドル", "unit": "USD per EUR", "ticker": "EURUSD=X"},
    {"id": "WTI", "label": "WTI原油", "unit": "USD/barrel", "ticker": "CL=F"},
    {"id": "GOLD", "label": "金", "unit": "USD/oz", "ticker": "GC=F", "fallback": "GLD"},
    {"id": "SILVER", "label": "銀", "unit": "USD/oz", "ticker": "SI=F", "fallback": "SLV"}
]

UNAVAILABLE = []


def fetch_fred_series(series_id):
    cutoff = datetime.utcnow().date() - timedelta(days=DAYS)
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={cutoff.isoformat()}"
    res = requests.get(url, timeout=30)
    res.raise_for_status()
    df = pd.read_csv(StringIO(res.text))
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])
    if "DATE" in df.columns:
        date_col = "DATE"
    elif "observation_date" in df.columns:
        date_col = "observation_date"
    else:
        return pd.DataFrame(columns=["date", "value"])
    value_col = [c for c in df.columns if c != date_col]
    if not value_col:
        return pd.DataFrame(columns=["date", "value"])
    value_col = value_col[0]
    df = df.rename(columns={date_col: "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    return df.sort_values("date")


def fetch_topix_stooq():
    start = datetime.utcnow().date() - timedelta(days=DAYS)
    end = datetime.utcnow().date()
    try:
        df = web.DataReader("^TPX", "stooq", start, end)
    except Exception as e:
        return pd.DataFrame(columns=["date", "value"]), str(e)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "value"]), "stooq empty"

    df = df.reset_index()
    date_col = "Date" if "Date" in df.columns else "date"
    close_col = "Close" if "Close" in df.columns else "close"
    if date_col not in df.columns or close_col not in df.columns:
        return pd.DataFrame(columns=["date", "value"]), "stooq columns missing"

    df = df.rename(columns={date_col: "date", close_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    cutoff = datetime.utcnow().date() - timedelta(days=DAYS)
    df = df[df["date"].dt.date >= cutoff]
    if df.empty:
        return pd.DataFrame(columns=["date", "value"]), "stooq no valid rows"
    return df.sort_values("date"), None


def fetch_yfinance_series(ticker):
    try:
        hist = yf.Ticker(ticker).history(period=f"{DAYS}d", interval="1d", auto_adjust=False)
    except Exception as e:
        return pd.DataFrame(columns=["date", "value"]), str(e)
    if hist is None or hist.empty:
        return pd.DataFrame(columns=["date", "value"]), "yfinance empty"

    df = hist.reset_index()
    if "Date" in df.columns:
        date_col = "Date"
    elif "Datetime" in df.columns:
        date_col = "Datetime"
    else:
        date_col = df.columns[0]

    if "Close" not in df.columns:
        return pd.DataFrame(columns=["date", "value"]), "yfinance columns missing"

    df = df.rename(columns={date_col: "date", "Close": "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    cutoff = datetime.utcnow().date() - timedelta(days=DAYS)
    df = df[df["date"].dt.date >= cutoff]
    if df.empty:
        return pd.DataFrame(columns=["date", "value"]), "yfinance no valid rows"
    return df.sort_values("date"), None


def load_cached_series(series_id):
    path = SERIES_DIR / f"{series_id}.csv"
    if not path.exists():
        return pd.DataFrame(columns=["date", "value"])
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    return df.sort_values("date")


def summarize(df):
    if df.empty:
        return {"latest": None, "change": None, "change_pct": None}
    latest = df.iloc[-1]["value"]
    prev = df.iloc[-2]["value"] if len(df) >= 2 else None
    change = (latest - prev) if prev is not None else None
    change_pct = (change / prev * 100) if prev not in (None, 0) else None
    return {
        "latest": round(float(latest), 3),
        "change": round(float(change), 3) if change is not None else None,
        "change_pct": round(float(change_pct), 3) if change_pct is not None else None
    }


def series_payload(series_id, label, unit, df):
    return {
        "id": series_id,
        "label": label,
        "unit": unit,
        "summary": summarize(df),
        "data": [
            {"date": d.strftime("%Y-%m-%d"), "value": round(float(v), 3)}
            for d, v in zip(df["date"], df["value"])
        ]
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SERIES_DIR.mkdir(parents=True, exist_ok=True)

    series_output = []
    unavailable = list(UNAVAILABLE)

    # TOPIX (stooq) with cache fallback
    topix_df, topix_err = fetch_topix_stooq()
    if topix_df.empty:
        cached = load_cached_series("TOPIX")
        if not cached.empty:
            series_output.append(series_payload("TOPIX", "TOPIX", "Index", cached))
            unavailable.append({
                "id": "TOPIX",
                "label": "TOPIX",
                "reason": f"stooq unavailable; using cached data ({topix_err})"
            })
        else:
            unavailable.append({
                "id": "TOPIX",
                "label": "TOPIX",
                "reason": topix_err or "stooq empty"
            })
    else:
        out_csv = SERIES_DIR / "TOPIX.csv"
        topix_df.to_csv(out_csv, index=False)
        series_output.append(series_payload("TOPIX", "TOPIX", "Index", topix_df))

    # FRED series (indices/VIX)
    for item in FRED_SERIES:
        df = fetch_fred_series(item["id"])
        if not df.empty:
            out_csv = SERIES_DIR / f"{item['id']}.csv"
            df.to_csv(out_csv, index=False)
        series_output.append(series_payload(item["id"], item["label"], item["unit"], df))
        time.sleep(0.2)

    # yfinance series (FX/commodities)
    for item in YF_SERIES:
        df, err = fetch_yfinance_series(item["ticker"])
        if df.empty and item.get("fallback"):
            df, err = fetch_yfinance_series(item["fallback"])
        if df.empty:
            cached = load_cached_series(item["id"])
            if not cached.empty:
                series_output.append(series_payload(item["id"], item["label"], item["unit"], cached))
                unavailable.append({
                    "id": item["id"],
                    "label": item["label"],
                    "reason": f"yfinance unavailable; using cached data ({err})"
                })
            else:
                unavailable.append({
                    "id": item["id"],
                    "label": item["label"],
                    "reason": err or "yfinance empty"
                })
        else:
            out_csv = SERIES_DIR / f"{item['id']}.csv"
            df.to_csv(out_csv, index=False)
            series_output.append(series_payload(item["id"], item["label"], item["unit"], df))
            time.sleep(0.2)

    result = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "days": DAYS,
        "series": series_output,
        "unavailable": unavailable,
        "notes": {
            "topix_source": "stooq (^TPX) via pandas_datareader",
            "yfinance_source": "Yahoo Finance via yfinance",
            "yfinance_tickers": {item["id"]: item["ticker"] for item in YF_SERIES}
        }
    }

    with open(OUTPUT_DIR / "market.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()