from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev, when

def add_zscore_anomaly(df, period=20):
    
    window = Window.partitionBy("ticker").orderBy("date").rowsBetween(-period, 0)

    df = df.withColumn("mean_return", avg("daily_return").over(window))
    df = df.withColumn("std_return", stddev("daily_return").over(window))

    df = df.withColumn(
        "z_score",
        (col("daily_return") - col("mean_return")) / col("std_return")
    )

    df = df.withColumn(
        "anomaly_flag",
        when(col("z_score") > 2, "SPIKE")
        .when(col("z_score") < -2, "CRASH")
        .otherwise("NORMAL")
    )

    return df