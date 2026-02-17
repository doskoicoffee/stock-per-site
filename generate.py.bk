import yfinance as yf

TOP_N = 10

tickers = [
    "7203.T",  # トヨタ
    "6758.T",  # ソニー
    "8306.T",  # 三菱UFJ
    "9432.T",  # NTT
    "9984.T",  # ソフトバンク
]

html = "<h1>簡易PER一覧</h1>"

for t in tickers[:TOP_N]:
    stock = yf.Ticker(t)
    price = stock.history(period="1d")["Close"].iloc[-1]
    eps = stock.info.get("trailingEps")

    if eps:
        per = price / eps
        html += f"<p>{t} 株価:{price:.2f} EPS:{eps:.2f} PER:{per:.2f}</p>"

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)
