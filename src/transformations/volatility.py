from pyspark.sql.window import Window
from pyspark.sql.functions import stddev, col

def add_volatility(df, period=20):
    window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-period, 0)

    df = df.withColumn(
        "volatility_20",
        stddev(col("daily_return")).over(window)
    )

    return df