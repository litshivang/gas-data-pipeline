from app.ingestion.national_gas_client import NationalGasClient

client = NationalGasClient()
df = client.fetch_last_days("PUBOB611", 30)
df.to_csv("source.csv", index=False)


import pandas as pd
from app.db.connection import engine

df_db = pd.read_sql("""
    SELECT raw_payload 
    FROM raw_events 
    WHERE dataset_id = 'PUBOB611'
    ORDER BY ingested_at
""", engine)

pd.DataFrame(list(df_db["raw_payload"])).to_csv("db.csv", index=False)
