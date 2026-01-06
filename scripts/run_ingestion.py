# scripts/run_ingestion.py
import argparse
from app.ingestion.run_all import ingest_dataset

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_id", help="National Gas dataset id (e.g. PUBOB37)")
    parser.add_argument("--lookback-days", type=int, default=14)
    args = parser.parse_args()

    ingest_dataset(args.dataset_id, lookback_days=args.lookback_days)
