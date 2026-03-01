# scripts/run_ingestion.py
import argparse
from datetime import datetime, timedelta, timezone
from app.ingestion.run_all import ingest_dataset

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_id", help="Dataset id (e.g. GAS_QUALITY, ENTSOG, PUBOB28)")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--from-date", type=str, help="YYYY-MM-DD (overrides lookback)")
    parser.add_argument("--to-date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--publication-ids", type=str, nargs="*", help="For GAS_PUBLICATIONS")
    args = parser.parse_args()

    if args.from_date and args.to_date:
        from_date, to_date = args.from_date, args.to_date
    else:
        to_d = datetime.now(timezone.utc).date()
        from_d = to_d - timedelta(days=args.lookback_days)
        from_date, to_date = from_d.isoformat(), to_d.isoformat()

    kwargs = {"from_date": from_date, "to_date": to_date}
    if args.publication_ids and args.dataset_id == "GAS_PUBLICATIONS":
        kwargs["publication_ids"] = args.publication_ids
    ingest_dataset(args.dataset_id, **kwargs)
