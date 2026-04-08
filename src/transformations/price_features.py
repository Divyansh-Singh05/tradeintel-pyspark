from pyspark.sql.functions import col

def add_price_features(df):
    
    df = df.withColumn(
        "daily_range",
        (col("high") - col("low")) / col("close")
    )

    df = df.withColumn(
        "price_change",
        (col("close") - col("open")) / col("open")
    )

    return df