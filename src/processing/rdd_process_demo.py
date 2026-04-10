from pyspark.sql import Row

from src.utils.helpers import load_config
from src.utils.spark_session import get_spark
from src.metadata.sector_map import SECTOR_MAP


def run_rdd_process_demo():
    """
    Demonstrates the classic Spark RDD "process":
    - create RDDs
    - apply lazy transformations (map/filter/join/reduceByKey)
    - trigger execution with actions (count/take/write)
    """
    config = load_config()
    spark = get_spark()

    print("RDD demo: reading Bronze parquet -> DataFrame -> RDD")
    bronze_df = spark.read.parquet(config["storage"]["bronze_path"])

    # Create an RDD from a DataFrame (RDD API starts here)
    price_rdd = bronze_df.rdd

    # Transformation: map to (ticker, close)
    # (uses Close if present; falls back to close if already cleaned)
    ticker_close_rdd = price_rdd.map(
        lambda r: (
            r["ticker"],
            r["Close"] if "Close" in r.asDict(recursive=False) else r["close"],
        )
    )

    # Transformation: filter out nulls
    ticker_close_rdd = ticker_close_rdd.filter(lambda kv: kv[0] is not None and kv[1] is not None)

    # Transformation: reduceByKey (aggregation) -> (ticker, sum_close)
    sum_close_by_ticker = ticker_close_rdd.reduceByKey(lambda a, b: a + b)

    # Create another RDD from in-memory Python data (sector mapping)
    sector_rdd = spark.sparkContext.parallelize(list(SECTOR_MAP.items()))

    # Transformation: leftOuterJoin -> (ticker, (sum_close, sector?))
    enriched_rdd = sum_close_by_ticker.leftOuterJoin(sector_rdd)

    # Action: count triggers the whole lineage/DAG execution
    n = enriched_rdd.count()
    print(f"RDD demo: enriched rows = {n}")

    # Action: take triggers execution (again) and returns sample to driver
    sample = enriched_rdd.take(5)
    print("RDD demo: sample (ticker, (sum_close, sector)):")
    for row in sample:
        print(row)

    # Convert back to DataFrame to write parquet (common pattern in real pipelines)
    out_df = spark.createDataFrame(
        enriched_rdd.map(lambda kv: Row(ticker=kv[0], sum_close=float(kv[1][0]), sector=kv[1][1]))
    )

    print("RDD demo: writing output parquet -> data/rdd_demo/")
    out_df.write.mode("overwrite").parquet("data/rdd_demo/")
    print("RDD demo complete")

