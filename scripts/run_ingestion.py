from app.ingestion.run_all import ingest_dataset

if __name__ == "__main__":
    ingest_dataset("PUBOB637", lookback_days=14)
