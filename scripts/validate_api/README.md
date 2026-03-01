# API migration validation scripts

One script per migrated API. Run after each migration to verify ingestion and DB state.

**Run from project root** (with `.env` and DB available):

```bash
# GAS_QUALITY
python -m scripts.validate_api.validate_gas_quality

# ENTSOG (needs FROM_DATE, TO_DATE, and indicators or point_keys+direction_keys)
python -m scripts.validate_api.validate_entsog

# GIE AGSI (optional: COUNTRY)
python -m scripts.validate_api.validate_gie_agsi

# GIE ALSI (optional: COUNTRY)
python -m scripts.validate_api.validate_gie_alsi

# INSTANTANEOUS_FLOW
python -m scripts.validate_api.validate_instantaneous_flow

# GAS_PUBLICATIONS (optional: FROM_DATE, TO_DATE, PUBLICATION_IDS)
python -m scripts.validate_api.validate_gas_publications
```

Optional env vars: `FROM_DATE`, `TO_DATE` (YYYY-MM-DD), `COUNTRY` (GIE), `PUBLICATION_IDS` (comma-separated for GAS_PUBLICATIONS).

Each script runs ingestion via orchestrator + adapter, compares DB counts before/after, and prints pass/fail.
