from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when, avg

def add_rsi(df, period=14):
    window = Window.partitionBy("ticker").orderBy("date")

    df = df.withColumn("price_change", col("close") - lag("close").over(window))

    df = df.withColumn(
        "gain",
        when(col("price_change") > 0, col("price_change")).otherwise(0)
    )

    df = df.withColumn(
        "loss",
        when(col("price_change") < 0, -col("price_change")).otherwise(0)
    )

    rolling_window = window.rowsBetween(-period, 0)

    df = df.withColumn("avg_gain", avg("gain").over(rolling_window))
    df = df.withColumn("avg_loss", avg("loss").over(rolling_window))

    df = df.withColumn("rs", col("avg_gain") / col("avg_loss"))

    df = df.withColumn(
        "rsi",
        100 - (100 / (1 + col("rs")))
    )

    return df