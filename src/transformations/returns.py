from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

def add_returns(df):
    window = Window.partitionBy("ticker").orderBy("date")

    df = df.withColumn(
        "prev_close",
        lag("close").over(window)
    )

    df = df.withColumn(
        "daily_return",
        (col("close") - col("prev_close")) / col("prev_close")
    )

    return df