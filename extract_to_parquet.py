import argparse
import pandas as pd
import yfinance as yf

def main(tickers, start=None, end=None, period="30d"):
    all_rows = []

    for t in tickers:
        if start and end:
            df = yf.download(t, start=start, end=end, interval="1d", group_by="column", auto_adjust=False)
        else:
            df = yf.download(t, period=period, interval="1d", group_by="column", auto_adjust=False)

        if df.empty:
            print(f"[WARN] Sem dados para {t}")
            continue

        # Se vier MultiIndex por algum motivo, achata
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]  # pega só o primeiro nível (Open/High/Low/Close/Volume)

        df.index.name = "Date"
        df = df.reset_index()

        df["ticker"] = t

        # Date como string YYYY-MM-DD (evita TIMESTAMP nanos)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date.astype(str)

        # (não criar coluna 'date' no arquivo; 'date' vem da partição no S3)
        all_rows.append(df[["Date","ticker","Open","High","Low","Close","Volume"]])

    out = pd.concat(all_rows, ignore_index=True)

    print("COLUMNS:", out.columns.tolist())
    print(out.dtypes)

    out.to_parquet("b3_raw.parquet", index=False, engine="pyarrow")
    print("Gerado: b3_raw.parquet", out.shape)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+", default=["PETR4.SA", "VALE3.SA"])
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--period", default="30d")
    args = parser.parse_args()
    main(args.tickers, args.start, args.end, args.period)