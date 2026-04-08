tradeintel-pyspark/
│
├── config/
│   ├── config.yaml
│   └── logging.yaml
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── notebooks/              # optional (experimentation)
│
├── src/
│   ├── ingestion/
│   │   ├── fetch_yfinance.py
│   │   └── ingest_bronze.py
│   │
│   ├── processing/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   │
│   ├── transformations/
│   │   ├── technical_indicators.py
│   │   ├── returns.py
│   │   └── volatility.py
│   │
│   ├── utils/
│   │   ├── spark_session.py
│   │   ├── logger.py
│   │   └── helpers.py
│   │
│   └── metadata/
│       └── tickers.py
│
├── tests/
│   └── test_indicators.py
│
├── requirements.txt
├── main.py
└── README.md