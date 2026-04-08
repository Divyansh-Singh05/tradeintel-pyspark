from pyspark.sql.functions import corr

def compute_pairwise_correlation(df):
    
    tickers = [row['ticker'] for row in df.select("ticker").distinct().collect()]
    
    results = []

    for i in range(len(tickers)):
        for j in range(i+1, len(tickers)):
            t1 = tickers[i]
            t2 = tickers[j]

            df1 = df.filter(df.ticker == t1).select("date", df.daily_return.alias("r1"))
            df2 = df.filter(df.ticker == t2).select("date", df.daily_return.alias("r2"))

            joined = df1.join(df2, on="date", how="inner")

            corr_value = joined.select(corr("r1", "r2")).collect()[0][0]

            results.append((t1, t2, corr_value))

    return results