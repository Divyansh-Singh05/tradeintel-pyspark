from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession.builder \
        .appName("TradeIntel") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()

    return spark