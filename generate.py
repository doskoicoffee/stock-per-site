import yfinance as yf
import pandas as pd
import json
import time
import math
import os
from datetime import datetime


START_TIME = time.time()

# ===== 設定 =====
LIMIT = None   # テスト用（全銘柄にするときはNone）
CSV_FILE = "tickers.csv"
OUTPUT_FILE = "data.json"
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 10


# ===== ユーティリティ =====
def safe(info, key):
    value = info.get(key)
    return None if value is None else value


def finite_number(value):
    if value is None:
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num):
        return None
    return num


def percent(value):
    num = finite_number(value)
    if num is None:
        return None
    if num < 1:
        return round(num * 100, 1)
    return round(num, 1)


def percent_dividend_yield(value):
    num = finite_number(value)
    if num is None:
        return None
    # yfinance の dividendYield は銘柄により単位が混在するため、
    # 0.2未満のみ比率(0.03=3%)として扱う。
    if num < 0.2:
        return round(num * 100, 1)
    return round(num, 1)


def oku(value):
    num = finite_number(value)
    if num is None:
        return None
    return round(num / 100_000_000, 1)


def r1(value):
    num = finite_number(value)
    if num is None:
        return None
    return round(num, 1)


def safe_ratio(numerator, denominator):
    n = finite_number(numerator)
    d = finite_number(denominator)
    if n is None or d is None or d == 0:
        return None
    return n / d


def to_date(value):
    if value is None:
        return None
    if isinstance(value, list) and len(value) > 0:
        return value[0].strftime("%Y-%m-%d")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return None


def get_ticker_info_with_retry(code):
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(code + ".T")
            return ticker, (ticker.info or {})
        except Exception as e:
            last_error = e
            msg = str(e)
            if "Too Many Requests" in msg or "Rate limited" in msg:
                wait = RETRY_SLEEP_SECONDS * (attempt + 1)
                print("Rate limited:", code, "sleep", wait, "seconds")
                time.sleep(wait)
                continue
            break
    raise last_error


def _pick_row_value(df, row_keys, col):
    if df is None or df.empty:
        return None

    key_map = {}
    for idx in df.index:
        key_map[str(idx).strip().lower()] = idx

    for key in row_keys:
        idx = key_map.get(str(key).strip().lower())
        if idx is None:
            continue
        try:
            val = df.at[idx, col]
        except Exception:
            continue
        num = finite_number(val)
        if num is not None:
            return num
    return None


def extract_financial_history(ticker, max_years=6):
    frames = []
    for attr in ("income_stmt", "financials"):
        try:
            obj = getattr(ticker, attr)
            df = obj() if callable(obj) else obj
        except Exception:
            df = None
        if isinstance(df, pd.DataFrame) and not df.empty:
            frames.append(df)

    if not frames:
        return []

    df = frames[0]
    col_year_pairs = []
    for col in df.columns:
        dt = pd.to_datetime(col, errors="coerce")
        if pd.isna(dt):
            continue
        col_year_pairs.append((col, int(dt.year), dt))

    if not col_year_pairs:
        return []

    col_year_pairs.sort(key=lambda x: x[2])

    revenue_keys = ["Total Revenue", "Revenue", "Operating Revenue", "TotalRevenue"]
    operating_keys = ["Operating Income", "Operating Income Loss", "OperatingIncome"]
    net_keys = ["Net Income", "Net Income Common Stockholders", "NetIncome"]

    by_year = {}
    for col, year, _ in col_year_pairs:
        revenue = _pick_row_value(df, revenue_keys, col)
        operating_income = _pick_row_value(df, operating_keys, col)
        net_income = _pick_row_value(df, net_keys, col)

        if revenue is None and operating_income is None and net_income is None:
            continue

        by_year[year] = {
            "year": str(year),
            "revenue_oku": oku(revenue),
            "operating_income_oku": oku(operating_income),
            "net_income_oku": oku(net_income)
        }

    years = sorted(by_year.keys())
    rows = [by_year[y] for y in years]
    if max_years and len(rows) > max_years:
        rows = rows[-max_years:]
    return rows


# ===== CSV読み込み =====
df = pd.read_csv(CSV_FILE)

if LIMIT:
    df = df.head(LIMIT)

print("処理銘柄数:", len(df))

stocks = []

# ===== データ取得 =====
for index, row in df.iterrows():
    code = str(row["code"]).zfill(4)
    print("取得中:", code)

    stock = {
        "code": code,
        "name": row["name"],
        "market": row["market"],
        "industry": row["industry33"],
        "valuation": {
            "per": None,
            "pbr": None
        },
        "profitability": {
            "roe": None,
            "roa": None,
            "operating_margin": None,
            "net_margin": None
        },
        "dividend": {
            "yield": None,
            "per_share": None
        },
        "financial": {
            "market_cap_oku": None,
            "equity_ratio": None,
            "current_ratio": None,
            "revenue_oku": None,
            "operating_income_oku": None,
            "net_income_oku": None
        },
        "financial_history": [],
        "price": {
            "current": None,
            "target": None
        },
        "schedule": {
            "next_earnings": None
        }
    }

    try:
        ticker, info = get_ticker_info_with_retry(code)
        revenue = info.get("totalRevenue")
        operating_margin = info.get("operatingMargins")
        net_income = info.get("netIncomeToCommon")

        if revenue is not None and operating_margin is not None:
            revenue_val = finite_number(revenue)
            operating_margin_val = finite_number(operating_margin)
            if revenue_val is not None and operating_margin_val is not None:
                operating_income = finite_number(revenue_val * operating_margin_val)
            else:
                operating_income = None
        else:
            operating_income = None

        # 純利益率（自動計算）
        net_margin_calc = safe_ratio(net_income, revenue)

        # 自己資本比率（自動計算）
        total_equity = info.get("totalStockholderEquity")
        total_assets = info.get("totalAssets")

        equity_ratio = safe_ratio(total_equity, total_assets)

        stock["valuation"]["per"] = r1(safe(info, "trailingPE"))
        stock["valuation"]["pbr"] = r1(safe(info, "priceToBook"))

        stock["profitability"]["roe"] = percent(info.get("returnOnEquity"))
        stock["profitability"]["roa"] = percent(info.get("returnOnAssets"))
        stock["profitability"]["operating_margin"] = percent(info.get("operatingMargins"))
        stock["profitability"]["net_margin"] = percent(info.get("profitMargins"))

        stock["dividend"]["yield"] = percent_dividend_yield(info.get("dividendYield"))
        stock["dividend"]["per_share"] = r1(safe(info, "dividendRate"))

        stock["financial"]["market_cap_oku"] = oku(info.get("marketCap"))
        stock["financial"]["equity_ratio"] = percent(equity_ratio)
        stock["financial"]["current_ratio"] = r1(safe(info, "currentRatio"))
        stock["financial"]["revenue_oku"] = oku(info.get("totalRevenue"))
        stock["financial"]["operating_income_oku"] = oku(operating_income)
        stock["financial"]["net_income_oku"] = oku(net_income)
        stock["financial_history"] = extract_financial_history(ticker)

        stock["price"]["current"] = r1(safe(info, "previousClose"))
        stock["price"]["target"] = r1(safe(info, "targetMeanPrice"))

        stock["schedule"]["next_earnings"] = to_date(info.get("earningsDate"))

    except Exception as e:
        print("エラー:", code, e)
        stock["error"] = str(e)

    stocks.append(stock)

# ===== JSON出力（更新日時付き） =====
result = {
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "count": len(stocks),
    "stocks": stocks
}

# ===== JSON出力（UTF-8固定） =====
def write_json_utf8(path, data):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

write_json_utf8(OUTPUT_FILE, result)

END_TIME = time.time()
print("処理時間:", round(END_TIME - START_TIME, 2), "秒")
