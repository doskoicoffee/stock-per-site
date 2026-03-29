from pathlib import Path

path = Path('generate_market.py')
text = path.read_text(encoding='utf-8')

old = 'def fetch_series(series_id):\n    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={cutoff.isoformat()}"\n    res = requests.get(url, timeout=30)\n    res.raise_for_status()\n    df = pd.read_csv(StringIO(res.text))\n    if df.empty or "DATE" not in df.columns:\n        return pd.DataFrame(columns=["date", "value"])\n    value_col = [c for c in df.columns if c != "DATE"]\n    if not value_col:\n        return pd.DataFrame(columns=["date", "value"])\n    value_col = value_col[0]\n    df = df.rename(columns={"DATE": "date", value_col: "value"})\n    df["date"] = pd.to_datetime(df["date"], errors="coerce")\n    df["value"] = pd.to_numeric(df["value"], errors="coerce")\n    df = df.dropna(subset=["date", "value"])\n    cutoff = datetime.utcnow().date() - timedelta(days=DAYS)\n    df = df[df["date"].dt.date >= cutoff]\n    return df.sort_values("date")\n'

new = 'def fetch_series(series_id):\n    cutoff = datetime.utcnow().date() - timedelta(days=DAYS)\n    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={cutoff.isoformat()}"\n    res = requests.get(url, timeout=30)\n    res.raise_for_status()\n    df = pd.read_csv(StringIO(res.text))\n    if df.empty or "DATE" not in df.columns:\n        return pd.DataFrame(columns=["date", "value"])\n    value_col = [c for c in df.columns if c != "DATE"]\n    if not value_col:\n        return pd.DataFrame(columns=["date", "value"])\n    value_col = value_col[0]\n    df = df.rename(columns={"DATE": "date", value_col: "value"})\n    df["date"] = pd.to_datetime(df["date"], errors="coerce")\n    df["value"] = pd.to_numeric(df["value"], errors="coerce")\n    df = df.dropna(subset=["date", "value"])\n    return df.sort_values("date")\n'

if old not in text:
    raise SystemExit('fetch_series block not found')

text = text.replace(old, new)
path.write_text(text, encoding='utf-8')
