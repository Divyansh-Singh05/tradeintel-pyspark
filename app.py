import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/gold/")

st.title("TradeIntel Analytics Dashboard")




# --- Existing UI ---
option = st.selectbox(
    "Choose Analysis",
    [
        "Top Volatile Stocks",
        "Sector Performance",
        "Extreme Price Moves",
        "Custom Query (Advanced)"
    ]
)

# --- Normal UI ---
if option == "Top Volatile Stocks":
    result = df.groupBy("ticker") \
        .avg("volatility_20") \
        .orderBy("avg(volatility_20)", ascending=False)
    
    st.dataframe(result.toPandas())

# --- CUSTOM QUERY MODE ---
elif option == "Custom Query (Advanced)":

    st.subheader("⚡ Spark-like Query Console")

    st.write("Use `df` as your DataFrame")

    user_query = st.text_area(
        "Enter PySpark Query",
        'df.groupBy("sector").avg("volatility_20")'
    )

    if st.button("Run Query"):

        try:
            safe_globals = {
            "df": df,
            "col": col,
            "when": when
            }

            result = eval(user_query, safe_globals)

            st.success("Query executed successfully!")

            st.dataframe(result.toPandas())

        except Exception as e:
            st.error(f"Error: {str(e)}")