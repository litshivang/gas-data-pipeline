# All 16 APIs — Checklist

## Summary

- **6 POST ingestion APIs** — Migrated to orchestrator + adapters; entry points call `Orchestrator(registry).run(dataset_id, ...)` or `ingest_dataset(...)`.
- **10 other APIs** — GET (health, data, discovery, publication-catalogue, export, gie/data). Already implemented; unchanged by refactor and working.

---

## 1. GET `/health`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/health.py` |
| Router | Included in `main.py` (no prefix) |
| Notes  | Returns welcome message. No change in refactor. |

---

## 2. GET `/v2/data`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/routes.py` |
| Router | `v2_router` (prefix `/v2`) |
| Notes  | Query params: series_id, dataset_id, start, end, quality_flag, min_value, max_value, limit, offset, include_raw. Uses `DATA_QUERY` and `meta_series` + `data_observations`. No change in refactor. |

---

## 3. GET `/v2/discovery/datasets`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/discovery.py` |
| Router | `discovery_router` (prefix `/v2/discovery`) |
| Notes  | Returns distinct `dataset_id` from `raw_events`. No change in refactor. |

---

## 4. GET `/v2/discovery/fields`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/discovery.py` |
| Query  | `dataset_id` (required) |
| Notes  | Returns field_catalog for dataset. No change in refactor. |

---

## 5. GET `/v2/discovery/sample`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/discovery.py` |
| Query  | `dataset_id`, `limit` (default 5, max 50) |
| Notes  | Sample raw_payload from raw_events. No change in refactor. |

---

## 6. GET `/v2/discovery/raw`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/discovery.py` |
| Query  | `dataset_id`, `limit`, optional `site_id` |
| Notes  | Raw payload preview with optional filters. No change in refactor. |

---

## 7. POST `/v2/ingest/gas`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/ingestion.py` |
| Notes  | **Migrated.** Uses `_orchestrator.run("GAS_QUALITY", from_date, to_date, site_ids)`. |

---

## 8. POST `/v2/ingest/entsog`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/ingestion.py` |
| Notes  | **Migrated.** Uses `_orchestrator.run("ENTSOG", ...)`. |

---

## 9. POST `/v2/ingest/instantaneous`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/ingestion.py` |
| Notes  | **Migrated.** Calls `ingest_dataset("INSTANTANEOUS_FLOW")` → orchestrator. |

---

## 10. GET `/v2/ingest/publication-catalogue`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/ingestion.py` |
| Notes  | Uses `NationalGasClient().fetch_publication_catalogue()`. Returns simplified publication list. No change in refactor. |

---

## 11. POST `/v2/ingest/gas-publications`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/ingestion.py` |
| Notes  | **Migrated.** Calls `ingest_dataset("GAS_PUBLICATIONS", from_date, to_date, publication_ids)` → orchestrator. |

---

## 12. GET `/v2/export/raw/json`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/export.py` |
| Router | `export_router` (prefix `/v2/export`) |
| Query  | `dataset_id`, `limit` |
| Notes  | Returns raw_events raw_payload as JSON. No change in refactor. |

---

## 13. GET `/v2/export/raw/csv`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/export.py` |
| Query  | `dataset_id`, `limit` |
| Notes  | Returns raw_events as CSV download. No change in refactor. |

---

## 14. POST `/v2/gie/agsi`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/gie.py` |
| Notes  | **Migrated.** Uses `_orchestrator.run("AGSI", country=...)`. |

---

## 15. POST `/v2/gie/alsi`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/gie.py` |
| Notes  | **Migrated.** Uses `_orchestrator.run("ALSI", country=...)`. |

---

## 16. GET `/v2/gie/data`

| Item   | Status |
|--------|--------|
| File   | `app/api/v2/gie.py` |
| Query  | `source` (required), `country`, `variable`, `start_date`, `end_date`, `limit` |
| Notes  | Reads from `energy.daily` + `meta.series` + `meta.assets`. No change in refactor. |

---

## Verification

All 16 APIs are implemented and wired in `app/main.py`. The 10 non-ingestion APIs (GET health, data, discovery, publication-catalogue, export, gie/data) did not require code changes for the new implementation; they continue to work as before.

To confirm at runtime:

```bash
# Start server
uvicorn app.main:app --reload

# Then open Swagger UI
# http://localhost:8000/docs
```

All 16 endpoints appear under their tags (health, v2, Discovery, Ingestion, Export, GIE).
