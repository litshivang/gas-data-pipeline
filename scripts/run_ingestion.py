from app.ingestion.national_gas_client import NationalGasClient
from app.ingestion.transformer import transform_demand_csv
from app.ingestion.loader import upsert_observations

DATASET_ID = "PUBOB637"
SERIES_ID = "UK_NBP_DEMAND"
LAST_DAYS = 7


def main():
    client = NationalGasClient()

    df = client.fetch_last_days(DATASET_ID, LAST_DAYS)
    
    records = transform_demand_csv(df, SERIES_ID)

    upsert_observations(records)


if __name__ == "__main__":
    main()
