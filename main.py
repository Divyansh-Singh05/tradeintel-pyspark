from src.ingestion.ingest_bronze import run_bronze
import os
from src.processing.bronze_to_silver import run_silver, run_silver_rdd
from src.processing.silver_to_gold import run_gold
from src.processing.rdd_process_demo import run_rdd_process_demo
from src.processing.rdd_detailed_demo import run_rdd_detailed_demo

if __name__ == "__main__":
    print("Starting pipeline...")

    if os.environ.get("RUN_RDD_DEMO", "").strip() in {"1", "true", "True", "yes", "YES"}:
        run_rdd_process_demo()
        raise SystemExit(0)

    if os.environ.get("RUN_RDD_DETAILED_DEMO", "").strip() in {"1", "true", "True", "yes", "YES"}:
        run_rdd_detailed_demo()
        raise SystemExit(0)

    run_bronze()
    if os.environ.get("USE_RDD_SILVER", "").strip() in {"1", "true", "True", "yes", "YES"}:
        run_silver_rdd()
    else:
        run_silver()
    run_gold()

    print("Pipeline complete")