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
MAX_RETRIES = 1
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


def first_non_null(*values):
    for value in values:
        if value is not None:
            return value
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


def _latest_frame_value(df, row_keys):
    if df is None or df.empty:
        return None

    key_map = {}
    for idx in df.index:
        key_map[str(idx).strip().lower()] = idx

    ordered_cols = []
    for col in df.columns:
        dt = pd.to_datetime(col, errors="coerce")
        if pd.isna(dt):
            ordered_cols.append((col, None))
        else:
            ordered_cols.append((col, dt))

    ordered_cols.sort(key=lambda item: (item[1] is not None, item[1]), reverse=True)

    for key in row_keys:
        idx = key_map.get(str(key).strip().lower())
        if idx is None:
            continue
        for col, _ in ordered_cols:
            try:
                val = df.at[idx, col]
            except Exception:
                continue
            num = finite_number(val)
            if num is not None:
                return num
    return None


def extract_balance_sheet_snapshot(ticker, info):
    equity = finite_number(info.get("totalStockholderEquity"))
    assets = finite_number(info.get("totalAssets"))
    if equity is not None and assets is not None:
        return equity, assets

    frames = []
    for attr in ("balance_sheet", "balancesheet", "quarterly_balance_sheet"):
        try:
            obj = getattr(ticker, attr)
            df = obj() if callable(obj) else obj
        except Exception:
            df = None
        if isinstance(df, pd.DataFrame) and not df.empty:
            frames.append(df)

    equity_keys = [
        "Total Stockholder Equity",
        "Stockholders Equity",
        "Total Equity Gross Minority Interest",
        "Common Stock Equity",
        "StockholdersEquity"
    ]
    asset_keys = ["Total Assets", "TotalAssets"]

    for df in frames:
        if equity is None:
            equity = _latest_frame_value(df, equity_keys)
        if assets is None:
            assets = _latest_frame_value(df, asset_keys)
        if equity is not None and assets is not None:
            break

    return equity, assets


def _get_first_frame(ticker, attrs):
    for attr in attrs:
        try:
            obj = getattr(ticker, attr)
            df = obj() if callable(obj) else obj
        except Exception:
            df = None
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    return None


def _build_year_value_map(df, row_keys):
    if df is None or df.empty:
        return {}

    pairs = []
    for col in df.columns:
        dt = pd.to_datetime(col, errors="coerce")
        if pd.isna(dt):
            continue
        pairs.append((col, int(dt.year), dt))

    pairs.sort(key=lambda item: item[2])

    out = {}
    for col, year, _ in pairs:
        value = _pick_row_value(df, row_keys, col)
        if value is not None:
            out[year] = value
    return out


def extract_financial_history(ticker, info, max_years=6):
    income_df = _get_first_frame(ticker, ("income_stmt", "financials"))
    balance_df = _get_first_frame(ticker, ("balance_sheet", "balancesheet"))

    if income_df is None and balance_df is None:
        return []

    revenue_map = _build_year_value_map(
        income_df,
        ["Total Revenue", "Revenue", "Operating Revenue", "TotalRevenue"]
    )
    operating_income_map = _build_year_value_map(
        income_df,
        ["Operating Income", "Operating Income Loss", "OperatingIncome"]
    )
    net_income_map = _build_year_value_map(
        income_df,
        ["Net Income", "Net Income Common Stockholders", "NetIncome"]
    )
    eps_map = _build_year_value_map(
        income_df,
        ["Diluted EPS", "Basic EPS", "Normalized Diluted EPS", "Normalized Basic EPS"]
    )
    equity_map = _build_year_value_map(
        balance_df,
        [
            "Total Stockholder Equity",
            "Stockholders Equity",
            "Total Equity Gross Minority Interest",
            "Common Stock Equity",
            "StockholdersEquity"
        ]
    )
    assets_map = _build_year_value_map(balance_df, ["Total Assets", "TotalAssets"])
    shares_map = _build_year_value_map(
        balance_df,
        [
            "Ordinary Shares Number",
            "Share Issued",
            "Common Stock Shares Outstanding",
            "Shares Outstanding"
        ]
    )

    shares_fallback = finite_number(info.get("sharesOutstanding"))
    years = sorted(set(
        list(revenue_map.keys()) +
        list(operating_income_map.keys()) +
        list(net_income_map.keys()) +
        list(equity_map.keys()) +
        list(assets_map.keys()) +
        list(eps_map.keys()) +
        list(shares_map.keys())
    ))

    rows = []
    for year in years:
        revenue = revenue_map.get(year)
        operating_income = operating_income_map.get(year)
        net_income = net_income_map.get(year)
        equity = equity_map.get(year)
        assets = assets_map.get(year)
        shares = shares_map.get(year) or shares_fallback
        eps = eps_map.get(year)
        if eps is None:
            eps = safe_ratio(net_income, shares)
        bps = safe_ratio(equity, shares)

        row = {
            "year": str(year),
            "revenue_oku": oku(revenue),
            "operating_income_oku": oku(operating_income),
            "net_income_oku": oku(net_income),
            "roe": percent(safe_ratio(net_income, equity)),
            "roa": percent(safe_ratio(net_income, assets)),
            "eps": r1(eps),
            "bps": r1(bps)
        }

        if any(value is not None for key, value in row.items() if key != "year"):
            rows.append(row)

    if max_years and len(rows) > max_years:
        rows = rows[-max_years:]
    return rows


def extract_dividend_history(ticker, current_yield, max_years=6):
    try:
        series = ticker.dividends
    except Exception:
        series = None

    yearly_dividend = {}
    if series is not None and len(series) > 0:
        for idx, value in series.items():
            dt = pd.to_datetime(idx, errors="coerce")
            amount = finite_number(value)
            if pd.isna(dt) or amount is None:
                continue
            year = int(dt.year)
            yearly_dividend[year] = yearly_dividend.get(year, 0.0) + amount

    close_map = {}
    if yearly_dividend:
        try:
            price_df = ticker.history(period=f"{max_years + 2}y", interval="1mo", auto_adjust=False)
        except Exception:
            price_df = None
        if isinstance(price_df, pd.DataFrame) and not price_df.empty and "Close" in price_df.columns:
            close_df = price_df[["Close"]].copy()
            close_df["Year"] = close_df.index.year
            for year, group in close_df.groupby("Year"):
                close = finite_number(group["Close"].dropna().iloc[-1] if not group["Close"].dropna().empty else None)
                if close is not None:
                    close_map[int(year)] = close

    years = sorted(yearly_dividend.keys())
    rows = []
    for year in years:
        dividend_per_share = r1(yearly_dividend.get(year))
        close = close_map.get(year)
        dividend_yield = percent(safe_ratio(dividend_per_share, close))
        rows.append({
            "year": str(year),
            "dividend_per_share": dividend_per_share,
            "dividend_yield": dividend_yield
        })

    if rows and current_yield is not None and rows[-1]["dividend_yield"] is None:
        rows[-1]["dividend_yield"] = current_yield

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
        "dividend_history": [],
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
        total_equity, total_assets = extract_balance_sheet_snapshot(ticker, info)

        equity_ratio = safe_ratio(total_equity, total_assets)
        roe_calc = percent(safe_ratio(net_income, total_equity))
        roa_calc = percent(safe_ratio(net_income, total_assets))

        stock["valuation"]["per"] = r1(safe(info, "trailingPE"))
        stock["valuation"]["pbr"] = r1(safe(info, "priceToBook"))
        stock["profitability"]["operating_margin"] = percent(info.get("operatingMargins"))
        stock["profitability"]["net_margin"] = percent(info.get("profitMargins"))

        stock["dividend"]["yield"] = percent_dividend_yield(info.get("dividendYield"))
        stock["dividend"]["per_share"] = r1(safe(info, "dividendRate"))
        stock["dividend_history"] = extract_dividend_history(ticker, stock["dividend"]["yield"])

        stock["financial"]["market_cap_oku"] = oku(info.get("marketCap"))
        stock["financial"]["equity_ratio"] = percent(equity_ratio)
        stock["financial"]["current_ratio"] = r1(safe(info, "currentRatio"))
        stock["financial"]["revenue_oku"] = oku(info.get("totalRevenue"))
        stock["financial"]["operating_income_oku"] = oku(operating_income)
        stock["financial"]["net_income_oku"] = oku(net_income)
        stock["financial_history"] = extract_financial_history(ticker, info)

        latest_history = stock["financial_history"][-1] if stock["financial_history"] else {}
        stock["profitability"]["roe"] = first_non_null(
            percent(info.get("returnOnEquity")),
            roe_calc,
            latest_history.get("roe")
        )
        stock["profitability"]["roa"] = first_non_null(
            percent(info.get("returnOnAssets")),
            roa_calc,
            latest_history.get("roa")
        )

        stock["price"]["current"] = r1(safe(info, "previousClose"))
        stock["price"]["target"] = r1(safe(info, "targetMeanPrice"))

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
