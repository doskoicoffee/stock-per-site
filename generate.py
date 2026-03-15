import yfinance as yf
import pandas as pd
import json
import time
from datetime import datetime


START_TIME = time.time()

# ===== 設定 =====
LIMIT = 20   # テスト用（全銘柄にするときはNone）
CSV_FILE = "tickers.csv"
OUTPUT_FILE = "data.json"


# ===== ユーティリティ =====
def safe(info, key):
    value = info.get(key)
    return None if value is None else value


def percent(value):
    if value is None:
        return None
    if value < 1 : 
        return round(value * 100, 1)
    return round(value,1)

def oku(value):
    if value is None:
        return None
    return round(value / 100_000_000, 1)


def r1(value):
    if value is None:
        return None
    return round(float(value), 1)

def to_date(value):
    if value is None:
        return None
    if isinstance(value, list) and len(value) > 0:
        return value[0].strftime("%Y-%m-%d")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return None

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

    try:
        ticker = yf.Ticker(code + ".T")
        info = ticker.info
        revenue = info.get("totalRevenue")
        operating_margin = info.get("operatingMargins")
        net_income = info.get("netIncomeToCommon")
        
        if revenue and operating_margin:
            operating_income = revenue * operating_margin
        else:
            operating_income = None

        # 純利益率（自動計算）
        if revenue and net_income:
            net_margin_calc = net_income / revenue
        else:
            net_margin_calc = None


        # 自己資本比率（自動計算）
        total_equity = info.get("totalStockholderEquity")
        total_assets = info.get("totalAssets")
            
        if total_equity and total_assets:
            equity_ratio = total_equity / total_assets
        else:
            equity_ratio = None
                
        
        stock = {
            "code": code,
            "name": row["name"],
            "market": row["market"],
            "industry": row["industry33"],
            
            "valuation": {
                "per": r1(safe(info, "trailingPE")),
                "pbr": r1(safe(info, "priceToBook"))
            },

            "profitability": {
                "roe": percent(info.get("returnOnEquity")),
                "roa": percent(info.get("returnOnAssets")),
                "operating_margin": percent(info.get("operatingMargins")),
                "net_margin": percent(info.get("profitMargins"))
            },
            
            "dividend": {
                "yield": percent(info.get("dividendYield")),
                "per_share": r1(safe(info, "dividendRate"))
            },

            
            "financial": {
                "market_cap_oku": oku(info.get("marketCap")),
                "equity_ratio": percent(equity_ratio),
                "current_ratio": r1(safe(info, "currentRatio")),
                "revenue_oku": oku(info.get("totalRevenue")),
                "operating_income_oku": oku(operating_income),
                "net_income_oku": oku(net_income)
            },
            
            "price": {
                "current": r1(safe(info, "previousClose")),
                "target": r1(safe(info, "targetMeanPrice"))
            },
            
            "schedule": {
                "next_earnings": to_date(info.get("earningsDate"))
            }
        }
        # === 
        

        stocks.append(stock)

    except Exception as e:
        print("エラー:", code, e)

# ===== JSON出力（更新日時付き） =====
result = {
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "count": len(stocks),
    "stocks": stocks
}
# ===== JSON出力 =====
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

END_TIME = time.time()
print("処理時間:", round(END_TIME - START_TIME, 2), "秒")
