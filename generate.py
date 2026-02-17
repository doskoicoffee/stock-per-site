import yfinance as yf

TOP_N = 10

tickers = [
    "7203.T",  # トヨタ
    "6758.T",  # ソニー
    "8306.T",  # 三菱UFJ
    "9432.T",  # NTT
    "9984.T",  # ソフトバンク
]

stocks = []

# ---------------------------
# ① データ取得
# ---------------------------
for t in tickers[:TOP_N]:
    try:
        stock = yf.Ticker(t)
        info = stock.info
        price = stock.history(period="1d")["Close"].iloc[-1]
        eps = info.get("trailingEps")
        name = info.get("shortName", t)

        if eps and eps != 0:
            per = price / eps
        else:
            per = None

        stocks.append({
            "code": t,
            "name": name,
            "price": round(price, 2),
            "eps": round(eps, 2) if eps else "-",
            "per": round(per, 2) if per else "-"
        })

    except Exception as e:
        print(f"{t} 取得失敗: {e}")


# ---------------------------
# ② HTML生成（表形式）
# ---------------------------
html = """
<html>
<head>
<meta charset="UTF-8">
<title>日本株PER一覧</title>
<style>
body { font-family: Arial, sans-serif; }
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
<th>株価</th>
<th>EPS</th>
<th>PER</th>
</tr>
</thead>
<tbody>
"""

# テーブル行を追加
for s in stocks:
    html += f"""
<tr>
<td>{s['code']}</td>
<td>{s['name']}</td>
<td>{s['price']}</td>
<td>{s['eps']}</td>
<td>{s['per']}</td>
</tr>
"""

# 閉じタグ
html += """
</tbody>
</table>
</body>
</html>
"""

# ---------------------------
# ③ index.htmlに書き出し
# ---------------------------
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("index.html を生成しました")
