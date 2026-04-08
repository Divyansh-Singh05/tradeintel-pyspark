from pyspark.sql import Row

def get_sector_df(spark, sector_map):
    data = [Row(ticker=k, sector=v) for k, v in sector_map.items()]
    return spark.createDataFrame(data)