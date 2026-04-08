from src.utils.spark_session import get_spark
from src.utils.helpers import load_config
from src.transformations.returns import add_returns
from src.transformations.technical_indicators import add_sma
from src.transformations.rsi import add_rsi
from src.transformations.price_features import add_price_features
from src.transformations.volatility import add_volatility
from src.transformations.anomalies import add_zscore_anomaly

def run_gold():
    config = load_config()
    spark = get_spark()

    print("Reading Silver data...")

    df = spark.read.parquet(config["storage"]["silver_path"])

    print("Applying transformations...")

    df = add_returns(df)
    df = add_sma(df, 20)
    df = add_rsi(df, 14)
    df = add_volatility(df, 20)
    df = add_price_features(df)

    df = add_zscore_anomaly(df, 20)

    print("Writing Gold layer...")

    df.write.mode("overwrite").parquet(config["storage"]["gold_path"])

    print("Gold layer complete")