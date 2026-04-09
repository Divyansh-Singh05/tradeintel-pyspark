import streamlit as st
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, desc, max as spark_max, min as spark_min, round as spark_round, stddev, when
from src.utils.spark_session import get_spark


st.set_page_config(
    page_title="TradeIntel Analytics Dashboard",
    page_icon="📈",
    layout="wide",
)

st.markdown(
    """
    <style>
        .stApp {
            background: linear-gradient(135deg, #f8fbff 0%, #eef9f4 50%, #f4f2ff 100%);
            color: #0f172a;
        }
        .stApp, .stApp p, .stApp span, .stApp label, .stApp li, .stApp div {
            color: #0f172a;
        }
        .stTabs [data-baseweb="tab"] {
            color: #1e293b;
        }
        .stTextInput input, .stTextArea textarea, .stSelectbox div[data-baseweb="select"] > div {
            color: #0f172a !important;
        }
        div[data-testid="stTextArea"] textarea {
            color: #ffffff !important;
            caret-color: #ffffff !important;
        }
        [data-testid="stSidebar"], [data-testid="stSidebar"] * {
            color: #ffffff !important;
        }
        .main-title {
            font-size: 2.2rem;
            font-weight: 800;
            color: #0f172a;
            margin-bottom: 0.15rem;
        }
        .subtitle {
            color: #334155;
            margin-bottom: 1.2rem;
        }
        .stMetric {
            background-color: rgba(255, 255, 255, 0.75);
            border: 1px solid rgba(148, 163, 184, 0.25);
            border-radius: 0.8rem;
            padding: 0.4rem 0.8rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource(show_spinner=False)
def load_gold_data() -> DataFrame:
    spark = get_spark()
    return spark.read.parquet("data/gold/")


def to_pandas_safe(spark_df: DataFrame, max_rows: int = 5000):
    return spark_df.limit(max_rows).toPandas()


def base_filtered_df(df: DataFrame, ticker: str, sector: str) -> DataFrame:
    filtered = df
    if ticker != "All":
        filtered = filtered.filter(col("ticker") == ticker)
    if sector != "All":
        filtered = filtered.filter(col("sector") == sector)
    return filtered


st.markdown('<div class="main-title">📊 TradeIntel Analytics Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Process large stock market datasets to identify trends, price fluctuations, and investment insights.</div>',
    unsafe_allow_html=True,
)

try:
    df = load_gold_data()
except Exception as exc:
    st.error("Unable to load Gold layer data from `data/gold/`.")
    st.exception(exc)
    st.info("Run `python main.py` first to build Bronze/Silver/Gold datasets, then rerun this app.")
    st.stop()

ticker_list = [row["ticker"] for row in df.select("ticker").distinct().orderBy("ticker").collect() if row["ticker"]]
sector_list = [row["sector"] for row in df.select("sector").distinct().orderBy("sector").collect() if row["sector"]]

with st.sidebar:
    st.header("Filters")
    ticker_filter = st.selectbox("Ticker", ["All"] + ticker_list, index=0)
    sector_filter = st.selectbox("Sector", ["All"] + sector_list, index=0)
    top_n = st.slider("Top rows to show", min_value=5, max_value=50, value=15, step=5)
    st.caption("Filters apply across all analysis sections.")

filtered_df = base_filtered_df(df, ticker_filter, sector_filter)

overview = filtered_df.agg(
    count("*").alias("rows"),
    count("ticker").alias("non_null_ticker_rows"),
    spark_round(avg("daily_return") * 100, 2).alias("avg_daily_return_pct"),
    spark_round(stddev("daily_return") * 100, 2).alias("daily_return_std_pct"),
    spark_round(avg("volatility_20") * 100, 2).alias("avg_volatility_pct"),
).collect()[0]

min_max_date = filtered_df.agg(
    spark_min("date").alias("min_date"),
    spark_max("date").alias("max_date"),
).collect()[0]

if overview["rows"] == 0:
    st.warning("No records found for the selected filters. Try broader filters.")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Rows", f'{overview["rows"]:,}')
col2.metric("Avg Daily Return", f'{overview["avg_daily_return_pct"] or 0:.2f}%')
col3.metric("Return Std Dev", f'{overview["daily_return_std_pct"] or 0:.2f}%')
col4.metric("Avg 20D Volatility", f'{overview["avg_volatility_pct"] or 0:.2f}%')

st.caption(f'Data window: {min_max_date["min_date"]} to {min_max_date["max_date"]}')

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "Top Volatile Stocks",
        "Sector Performance",
        "Extreme Price Moves",
        "Investment Insights",
        "Custom Query (Advanced)",
    ]
)

with tab1:
    st.subheader("Top Volatile Stocks")
    volatile_df = (
        filtered_df.groupBy("ticker")
        .agg(
            spark_round(avg("volatility_20") * 100, 2).alias("avg_volatility_pct"),
            spark_round(avg("daily_return") * 100, 2).alias("avg_return_pct"),
            count("*").alias("observations"),
        )
        .orderBy(desc("avg_volatility_pct"))
    )
    volatile_pd = to_pandas_safe(volatile_df, max_rows=top_n)
    st.dataframe(volatile_pd, use_container_width=True)

with tab2:
    st.subheader("Sector Performance")
    sector_perf_df = (
        filtered_df.groupBy("sector")
        .agg(
            spark_round(avg("daily_return") * 100, 2).alias("avg_return_pct"),
            spark_round(avg("volatility_20") * 100, 2).alias("avg_volatility_pct"),
            spark_round(avg("price_change") * 100, 2).alias("avg_price_change_pct"),
            count("*").alias("records"),
        )
        .orderBy(desc("avg_return_pct"))
    )
    sector_pd = to_pandas_safe(sector_perf_df, max_rows=100)
    st.dataframe(sector_pd, use_container_width=True)
    if not sector_pd.empty:
        st.bar_chart(sector_pd.set_index("sector")[["avg_return_pct", "avg_volatility_pct"]])

with tab3:
    st.subheader("Extreme Price Moves")
    extreme_df = (
        filtered_df.filter(col("anomaly_flag") != "NORMAL")
        .select(
            "date",
            "ticker",
            "sector",
            spark_round(col("daily_return") * 100, 2).alias("daily_return_pct"),
            spark_round("z_score", 2).alias("z_score"),
            "anomaly_flag",
        )
        .orderBy(desc("z_score"))
    )
    extreme_pd = to_pandas_safe(extreme_df, max_rows=top_n * 5)
    st.dataframe(extreme_pd, use_container_width=True)

with tab4:
    st.subheader("Investment Insights")
    insight_choice = st.selectbox(
        "Select an insight",
        [
            "Most Consistent Performers (Low Volatility + Positive Return)",
            "Potentially Overbought (RSI > 70)",
            "Potentially Oversold (RSI < 30)",
        ],
    )

    latest_date = filtered_df.agg(spark_max("date").alias("latest_date")).collect()[0]["latest_date"]
    latest_snapshot = filtered_df.filter(col("date") == latest_date)

    if insight_choice == "Most Consistent Performers (Low Volatility + Positive Return)":
        result_df = (
            latest_snapshot.groupBy("ticker", "sector")
            .agg(
                spark_round(avg("volatility_20") * 100, 2).alias("volatility_pct"),
                spark_round(avg("daily_return") * 100, 2).alias("daily_return_pct"),
            )
            .filter((col("volatility_pct") < 2.5) & (col("daily_return_pct") > 0))
            .orderBy(desc("daily_return_pct"), col("volatility_pct"))
        )
    elif insight_choice == "Potentially Overbought (RSI > 70)":
        result_df = (
            latest_snapshot.select(
                "ticker",
                "sector",
                spark_round("rsi", 2).alias("rsi"),
                spark_round(col("daily_return") * 100, 2).alias("daily_return_pct"),
            )
            .filter(col("rsi") > 70)
            .orderBy(desc("rsi"))
        )
    else:
        result_df = (
            latest_snapshot.select(
                "ticker",
                "sector",
                spark_round("rsi", 2).alias("rsi"),
                spark_round(col("daily_return") * 100, 2).alias("daily_return_pct"),
            )
            .filter(col("rsi") < 30)
            .orderBy(col("rsi"))
        )

    result_pd = to_pandas_safe(result_df, max_rows=top_n)
    st.dataframe(result_pd, use_container_width=True)

with tab5:
    st.subheader("Custom Query (Advanced)")
    st.caption("Use `df` as your DataFrame. Return a Spark DataFrame expression.")
    user_query = st.text_area(
        "Enter PySpark query",
        'df.groupBy("sector").agg(avg("volatility_20").alias("avg_volatility")).orderBy(col("avg_volatility").desc())',
        height=110,
    )

    if st.button("Run Query", type="primary"):
        try:
            safe_globals = {
                "__builtins__": {},
                "df": filtered_df,
                "col": col,
                "when": when,
                "avg": avg,
                "count": count,
                "desc": desc,
            }
            result = eval(user_query, safe_globals, {})
            if not isinstance(result, DataFrame):
                raise ValueError("Query must return a Spark DataFrame.")
            st.success("Query executed successfully.")
            st.dataframe(to_pandas_safe(result), use_container_width=True)
        except Exception as exc:
            st.error(f"Query failed: {exc}")