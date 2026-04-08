from pyspark.sql.functions import col
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