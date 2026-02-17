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
        #html += f"<p>{t} 株価:{price:.2f} EPS:{eps:.2f} PER:{per:.2f}</p>"
        html = """
        <html>
        <head>
        <meta charset="UTF-8">
        <title>日本株PER一覧</title>
        <style>
        table {
        border-collapse: collapse;
        width: 100%;
        }
        th, td {
        border: 1px solid #ccc;
        padding: 8px;
        text-align: center;
        }
        th {
        background-color: #f2f2f2;
        }
        </style>
        </head>
        <body>
        <h1>日本株 PER一覧</h1>
        <table>
        <thead>
        <tr>
        <th>銘柄コード</th>
        <th>銘柄名</th>
        <th>PER</th>
        <th>EPS</th>
        </tr>
        </thead>
        <tbody>
        """

        for stock in stocks:
            html += f"""
            <tr>
            <td>{stock['code']}</td>
            <td>{stock['name']}</td>
            <td>{stock['per']}</td>
        <td>{stock['eps']}</td>
            </tr>
            """

        html += """
        </tbody>
        </table>
        </body>
        </html>
        """

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)
