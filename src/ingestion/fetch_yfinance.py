import yfinance as yf
import pandas as pd

def fetch_data(tickers, start_date, end_date, interval):
    data = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True
    )

    all_data = []

    for ticker in tickers:
        df = data[ticker].copy()
        df["ticker"] = ticker
        df.reset_index(inplace=True)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)
    return final_df