from src.utils.spark_session import get_spark
from src.utils.helpers import load_config
from src.metadata.tickers import NIFTY50
from src.ingestion.fetch_yfinance import fetch_data

def run_bronze():
    config = load_config()
    spark = get_spark()

    print("Fetching data from yfinance...")

    pdf = fetch_data(
        tickers=NIFTY50,
        start_date=config["data"]["start_date"],
        end_date=config["data"]["end_date"],
        interval=config["data"]["interval"]
    )

    df = spark.createDataFrame(pdf)

    print("Writing Bronze layer...")

    df.write.mode("overwrite").parquet(config["storage"]["bronze_path"])

    print("Bronze layer complete")