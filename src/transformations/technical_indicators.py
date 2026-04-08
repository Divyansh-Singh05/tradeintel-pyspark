from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

def add_sma(df, period=20):
    window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-period, 0)

    df = df.withColumn(
        f"sma_{period}",
        avg(col("close")).over(window)
    )

    return df