from src.ingestion.ingest_bronze import run_bronze
from src.processing.bronze_to_silver import run_silver
from src.processing.silver_to_gold import run_gold

if __name__ == "__main__":
    print("Starting pipeline...")

    run_bronze()
    run_silver()
    run_gold()

    print("Pipeline complete")