from pyspark.sql.functions import col
from pyspark.sql import Row
from src.utils.spark_session import get_spark
from src.utils.helpers import load_config
from src.metadata.sector_map import SECTOR_MAP
from src.processing.sector_join import get_sector_df

def run_silver():
    config = load_config()
    spark = get_spark()

    print("Reading Bronze data...")

    df = spark.read.parquet(config["storage"]["bronze_path"])

    print("Cleaning data...")

    df = df.withColumnRenamed("Date", "date")

    df = df.select(
        col("date"),
        col("ticker"),
        col("Open").alias("open"),
        col("High").alias("high"),
        col("Low").alias("low"),
        col("Close").alias("close"),
        col("Adj Close").alias("adj_close"),
        col("Volume").alias("volume")
    )

    df = df.dropna()

    sector_df = get_sector_df(spark, SECTOR_MAP)
    df = df.join(sector_df, on="ticker", how="left")
    print("Writing Silver layer...")

    df.write.mode("overwrite").parquet(config["storage"]["silver_path"])

    print("Silver layer complete")


def run_silver_rdd():
    config = load_config()
    spark = get_spark()

    print("Reading Bronze data (RDD path)...")

    bronze_df = spark.read.parquet(config["storage"]["bronze_path"])

    print("Cleaning + joining sector using RDD transformations...")

    # (ticker, (date, open, high, low, close, adj_close, volume))
    price_rdd = (
        bronze_df.rdd.map(
            lambda r: (
                r["ticker"],
                (
                    r["Date"],
                    r["Open"],
                    r["High"],
                    r["Low"],
                    r["Close"],
                    r["Adj Close"],
                    r["Volume"],
                ),
            )
        )
        # similar to dropna() on these fields
        .filter(lambda kv: kv[0] is not None and all(v is not None for v in kv[1]))
    )

    # RDD join (left outer) to demonstrate the key-based shuffle join path.
    sector_rdd = spark.sparkContext.parallelize(list(SECTOR_MAP.items()))
    joined_rdd = price_rdd.leftOuterJoin(sector_rdd)

    silver_rows_rdd = joined_rdd.map(
        lambda kv: Row(
            date=kv[1][0][0],
            ticker=kv[0],
            open=kv[1][0][1],
            high=kv[1][0][2],
            low=kv[1][0][3],
            close=kv[1][0][4],
            adj_close=kv[1][0][5],
            volume=kv[1][0][6],
            sector=kv[1][1],
        )
    )

    silver_df = spark.createDataFrame(silver_rows_rdd)

    print("Writing Silver layer (RDD path)...")

    silver_df.write.mode("overwrite").parquet(config["storage"]["silver_path"])

    print("Silver layer complete (RDD path)")