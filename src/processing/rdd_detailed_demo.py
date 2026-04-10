from __future__ import annotations

from pyspark import RDD

from src.utils.helpers import load_config
from src.utils.spark_session import get_spark


def _print_header(title: str) -> None:
    line = "=" * len(title)
    print(f"\n{title}\n{line}")


def _describe_rdd(name: str, rdd: RDD) -> None:
    print(f"\nRDD: {name}")
    try:
        print(f"- partitions: {rdd.getNumPartitions()}")
    except Exception as exc:
        print(f"- partitions: (unable to read) {exc}")

    try:
        print("- lineage (toDebugString):")
        print(rdd.toDebugString())
    except Exception as exc:
        print(f"- lineage: (unable to print) {exc}")

def _show_df(title: str, df, n: int = 20) -> None:
    _print_header(title)
    try:
        df.show(n=n, truncate=False)
    except Exception as exc:
        print(f"(unable to show DataFrame) {exc}")


def run_rdd_detailed_demo(sample_rows: int = 8) -> None:
    """
    A terminal-first, detailed RDD demo designed for explanations/presentations.

    Shows:
    - DataFrame -> RDD conversion
    - Transformations vs actions
    - Partitions and per-partition samples
    - Lineage/DAG via toDebugString()
    - A shuffle-style aggregation via reduceByKey()
    """
    config = load_config()
    spark = get_spark()

    _print_header("RDD Detailed Demo")

    gold_path = config["storage"]["gold_path"]
    print(f"Reading Gold parquet from: {gold_path}")
    df = spark.read.parquet(gold_path).select(
        "date",
        "ticker",
        "sector",
        "daily_return",
        "volatility_20",
        "rsi",
        "anomaly_flag",
        "z_score",
    )

    _show_df("Gold sample (DataFrame)", df.limit(10), n=10)

    _print_header("Step 1: DataFrame -> RDD")
    base_rdd = df.rdd
    _describe_rdd("base_rdd (Row)", base_rdd)

    _print_header("Optional: increase partitions (demo)")
    try:
        desired_partitions = int(config.get("spark", {}).get("demo_partitions", 4))
    except Exception:
        desired_partitions = 4
    if base_rdd.getNumPartitions() < desired_partitions:
        base_rdd = base_rdd.repartition(desired_partitions)
        _describe_rdd(f"base_rdd repartitioned -> {desired_partitions}", base_rdd)
    else:
        print(f"Keeping partitions as-is: {base_rdd.getNumPartitions()}")

    _print_header("Step 2: Per-partition sample (transformation + action)")
    # Transformation: mapPartitionsWithIndex (lazy)
    per_part_sample_rdd = base_rdd.mapPartitionsWithIndex(
        lambda idx, it: [(idx, list(_take_n(it, sample_rows)))]
    )
    _describe_rdd("per_part_sample_rdd", per_part_sample_rdd)

    # Action: collect (triggers execution)
    per_part_samples = per_part_sample_rdd.collect()
    for idx, rows in per_part_samples:
        print(f"\nPartition {idx} sample_rows={len(rows)}")
        for r in rows[: min(3, len(rows))]:
            # Print a small subset so terminal output stays readable
            print(f"  {r}")

    _print_header("Step 3: Classic RDD transformation chain (map/filter)")
    # (sector, daily_return)
    sector_return_rdd = base_rdd.map(lambda r: (r["sector"], r["daily_return"]))
    sector_return_rdd = sector_return_rdd.filter(lambda kv: kv[0] is not None and kv[1] is not None)
    _describe_rdd("sector_return_rdd (sector, daily_return)", sector_return_rdd)

    _print_header("Step 4: Shuffle-style aggregation (reduceByKey)")
    # Map to (sector, (sum, count)) then reduceByKey => often causes a shuffle
    sum_count_rdd = sector_return_rdd.map(lambda kv: (kv[0], (float(kv[1]), 1)))
    agg_rdd = sum_count_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    _describe_rdd("agg_rdd (sector, (sum_return, count))", agg_rdd)

    # Action: takeOrdered to show top/bottom quickly
    top5 = agg_rdd.takeOrdered(5, key=lambda kv: -kv[1][0] / kv[1][1])
    bottom5 = agg_rdd.takeOrdered(5, key=lambda kv: kv[1][0] / kv[1][1])

    print("\nTop 5 sectors by avg daily return:")
    for sector, (s, c) in top5:
        print(f"- {sector:20s} avg={(s / c) * 100:8.4f}%  records={c}")

    print("\nBottom 5 sectors by avg daily return:")
    for sector, (s, c) in bottom5:
        print(f"- {sector:20s} avg={(s / c) * 100:8.4f}%  records={c}")

    # Convert RDD -> DataFrame so you can say "RDD output as DataFrame"
    sector_avg_df = spark.createDataFrame(
        agg_rdd.map(lambda kv: (kv[0], float(kv[1][0] / kv[1][1]), int(kv[1][1]))),
        ["sector", "avg_daily_return", "records"],
    ).orderBy("sector")
    _show_df("RDD -> DataFrame: sector average daily return", sector_avg_df, n=50)

    _print_header("Step 4B: RDD -> DataFrame: ticker summary")
    ticker_return_rdd = (
        base_rdd.map(lambda r: (r["ticker"], r["daily_return"]))
        .filter(lambda kv: kv[0] is not None and kv[1] is not None)
        .map(lambda kv: (kv[0], (float(kv[1]), 1)))
    )
    ticker_agg_rdd = ticker_return_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    ticker_avg_df = spark.createDataFrame(
        ticker_agg_rdd.map(lambda kv: (kv[0], float(kv[1][0] / kv[1][1]), int(kv[1][1]))),
        ["ticker", "avg_daily_return", "records"],
    ).orderBy("avg_daily_return", ascending=False)
    _show_df("RDD -> DataFrame: ticker avg daily return (top 20)", ticker_avg_df.limit(20), n=20)

    _print_header("Step 4C: RDD -> DataFrame: anomaly counts")
    anomaly_rdd = (
        base_rdd.map(lambda r: (r["anomaly_flag"], 1))
        .filter(lambda kv: kv[0] is not None)
        .reduceByKey(lambda a, b: a + b)
    )
    anomaly_df = spark.createDataFrame(anomaly_rdd.map(lambda kv: (kv[0], int(kv[1]))), ["anomaly_flag", "records"])
    _show_df("RDD -> DataFrame: anomaly_flag counts", anomaly_df.orderBy("records", ascending=False), n=50)

    _print_header("Step 4D: RDD -> DataFrame: extreme z-score rows (top 20)")
    # Keep a smaller set of columns for display
    z_rdd = (
        base_rdd.map(lambda r: (r["z_score"], (r["date"], r["ticker"], r["sector"], r["daily_return"], r["anomaly_flag"])))
        .filter(lambda kv: kv[0] is not None)
    )
    top_z = z_rdd.takeOrdered(20, key=lambda kv: -float(kv[0]))
    z_df = spark.createDataFrame(
        [(float(z), d, t, s, (float(dr) if dr is not None else None), af) for z, (d, t, s, dr, af) in top_z],
        ["z_score", "date", "ticker", "sector", "daily_return", "anomaly_flag"],
    )
    _show_df("Extreme anomalies by z_score (RDD takeOrdered -> DataFrame)", z_df, n=50)

    _print_header("Step 5: Action demo (count)")
    # Action: count
    n = sector_return_rdd.count()
    print(f"RDD count (non-null sector + daily_return rows): {n}")

    print("\nRDD Detailed Demo complete.")


def _take_n(it, n: int):
    i = 0
    for x in it:
        yield x
        i += 1
        if i >= n:
            return

