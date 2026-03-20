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


def fetch_gdelt_news(query, maxrecords=30, timespan="1day"):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": maxrecords,
        "timespan": timespan,
        "sort": "datedesc"
    }
    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()
    payload = res.json()
    articles = payload.get("articles") or payload.get("data") or []
    news = []
    seen = set()
    for a in articles:
        title = a.get("title") or a.get("seendate")
        url = a.get("url") or a.get("link")
        if not title or not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        published_raw = a.get("seendate") or a.get("date")
        published_at = None
        if published_raw:
            try:
                published_at = datetime.strptime(published_raw[:14], "%Y%m%d%H%M%S").isoformat()
            except Exception:
                published_at = published_raw
        news.append({
            "title": title,
            "url": url,
            "source": a.get("sourcecountry") or a.get("domain"),
            "domain": a.get("domain"),
            "language": a.get("language"),
            "published_at": published_at
        })
    return news


def generate_ai_summary(series_output, news):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None, "missing OPENAI_API_KEY"

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    snapshot = [
        {
            "id": s["id"],
            "label": s["label"],
            "latest": s["summary"].get("latest"),
            "change_pct": s["summary"].get("change_pct")
        }
        for s in series_output if s.get("summary")
    ]

    prompt = {
        "role": "system",
        "content": "あなたは金融ニュース編集者です。与えられた市場データとニュース見出しだけを根拠に、日本語で簡潔にまとめてください。投資助言はしないでください。出力は次の形式で、各項目は短く。\n【本日の市況】3-5行\n【主なニュース】3件（タイトルを引用）\n【注目点】1-2点"
    }
    user = {
        "role": "user",
        "content": "市場データ: " + json.dumps(snapshot, ensure_ascii=False) + "\nニュース: " + json.dumps(news[:10], ensure_ascii=False)
    }

    payload = {
        "model": model,
        "input": [prompt, user],
        "max_output_tokens": 600
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    res = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=60)
    res.raise_for_status()
    data = res.json()

    text = data.get("output_text")
    if not text:
        chunks = []
        for item in data.get("output", []):
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    chunks.append(c.get("text"))
        text = "\n".join(chunks).strip() if chunks else None

    return text, None


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

    # GDELT news
    base_query = os.getenv(
        "GDELT_QUERY",
        '(日経 OR TOPIX OR 東証 OR 日本株 OR 株式 OR 円 OR 為替 OR 原油 OR 金 OR 銀 OR 米国株 OR ダウ OR ナスダック OR S&P500)'
    )
    news = []
    try:
        query = base_query + " sourcelang:japanese"
        news = fetch_gdelt_news(query)
        if not news:
            news = fetch_gdelt_news(base_query)
    except Exception as e:
        unavailable.append({"id": "NEWS", "label": "ニュース", "reason": str(e)})

    # AI summary
    summary_text = None
    summary_err = None
    if news:
        try:
            summary_text, summary_err = generate_ai_summary(series_output, news)
        except Exception as e:
            summary_err = str(e)

    result = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "days": DAYS,
        "series": series_output,
        "unavailable": unavailable,
        "news": news[:10],
        "summary": {
            "text": summary_text,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "error": summary_err
        },
        "notes": {
            "topix_source": "stooq (^TPX) via pandas_datareader",
            "yfinance_source": "Yahoo Finance via yfinance",
            "gdelt_source": "GDELT DOC 2.0 API",
            "openai_model": os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        }
    }

    with open(OUTPUT_DIR / "market.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()