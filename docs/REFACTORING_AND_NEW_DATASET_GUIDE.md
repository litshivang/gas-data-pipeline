# Gas Data Pipeline — Refactoring & New Dataset Guide

This document explains the refactored ingestion implementation, the end-to-end flow, and how to add a new API or dataset.

---

## 1. What Was Refactored

**Before:** One big `ingest_dataset()` with `if dataset_id == "X"` branches; fetch, transform, and load logic mixed with conditionals.

**After:**

- **Single entry point:** `ingest_dataset(dataset_id, **kwargs)` only looks up the adapter in the **registry** and runs the **orchestrator**. No dataset-specific `if` in the ingestion flow.
- **Adapters:** Each dataset has an adapter class that implements fetch, parse, normalize, define_series, and get_time_field. Adapters do **not** touch the DB, retries, or lifecycle.
- **Orchestrator:** Runs a fixed **12-step lifecycle** for every ingestion. It uses the adapter only for data shaping; it owns retries, raw store, validation, delete policy, series registration, and bulk insert.
- **Registry:** Maps `dataset_id` (string) → adapter **class**. The orchestrator gets the class, instantiates it once per run, and calls its methods in order.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  API Layer (FastAPI)                                                     │
│  POST /v2/ingest/gas, /entsog, /instantaneous, /gas-publications        │
│  POST /v2/gie/agsi, /alsi                                                │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Entry point: ingest_dataset(dataset_id, **kwargs)                       │
│  • Checks registry.get(dataset_id)                                       │
│  • If missing → ValueError with list of available datasets               │
│  • If present → Orchestrator(registry).run(dataset_id, **kwargs)         │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Registry (singleton)                                                    │
│  dataset_id → Adapter class (e.g. "GAS_QUALITY" → NationalGasAdapter)   │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Orchestrator.run(dataset_id, **kwargs)                                  │
│  1. Create ingestion_run (insert into ingestion_runs)                   │
│  2. Load dataset config (stub)                                          │
│  3. Fetch (adapter.fetch + centralized retry)                            │
│  4. Store raw (ingest_raw_df / ingest_raw_json)                          │
│  5. Parse (adapter.parse) → list of records                             │
│  6. Normalize (adapter.normalize per record, flatten)                    │
│  7. Validate (adapter.get_validation_config + core validation)           │
│  8. Apply delete policy (config or GIE-specific)                         │
│  9. Register series (adapter.define_series → MetaSeries or GIE meta)    │
│ 10. Bulk insert (upsert_observations or insert_gie_rows)                 │
│ 11. Finalize ingestion_run (status, counts, error_message)               │
│ 12. Emit metrics (stub)                                                  │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
          ┌──────────────────────────┼──────────────────────────┐
          ▼                          ▼                            ▼
┌──────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  Adapter         │    │  Shared components   │    │  DB / storage        │
│  fetch/parse/    │    │  raw_ingestor,       │    │  raw_events,         │
│  normalize/      │    │  loader,             │    │  meta_series,        │
│  define_series/  │    │  field_discovery,    │    │  data_observations,  │
│  get_time_field  │    │  delete_policy       │    │  ingestion_runs,     │
│  (no DB, no      │    │                      │    │  energy.daily (GIE)  │
│   retry)         │    │                      │    │                      │
└──────────────────┘    └──────────────────────┘    └──────────────────────┘
```

---

## 3. Directory Structure (Relevant Parts)

```
app/
├── main.py                           # FastAPI app; mounts all routers
├── api/v2/
│   ├── health.py                     # GET /health
│   ├── routes.py                     # GET /v2/data
│   ├── discovery.py                  # GET /v2/discovery/*
│   ├── ingestion.py                  # POST /v2/ingest/*, GET publication-catalogue
│   ├── export.py                     # GET /v2/export/raw/json, /raw/csv
│   ├── gie.py                        # POST /v2/gie/agsi, /alsi; GET /v2/gie/data
│   ├── schemas.py
│   └── queries.py
├── ingestion/
│   ├── run_all.py                    # ingest_dataset(), run_national_gas(); registry-driven only
│   ├── core/
│   │   ├── base_adapter.py           # Abstract BaseAdapter contract
│   │   ├── registry.py                # AdapterRegistry + singleton registry
│   │   ├── orchestrator.py            # 12-step lifecycle
│   │   ├── validation.py             # Validation rules from adapter; ValidationError
│   │   └── __init__.py
│   ├── common/
│   │   ├── delete_policy.py          # Config-driven delete (e.g. last_n_days)
│   │   └── ...
│   ├── adapters/
│   │   ├── __init__.py               # Register all adapters with registry
│   │   ├── national_gas.py           # GAS_QUALITY
│   │   ├── entsog.py                 # ENTSOG
│   │   ├── instantaneous_flow.py     # INSTANTANEOUS_FLOW
│   │   ├── gas_publications.py       # GAS_PUBLICATIONS
│   │   ├── gie_agsi.py               # AGSI
│   │   └── gie_alsi.py               # ALSI
│   ├── raw_ingestor.py               # ingest_raw_df, ingest_raw_json
│   ├── loader.py                     # upsert_observations
│   ├── field_discovery.py            # discover_fields
│   └── gie/                          # GIE-specific: delete_gie_by_source, insert_gie_rows, etc.
├── db/
│   ├── models.py                     # MetaSeries, DataObservation, RawEvent, IngestionRun, ...
│   └── connection.py
└── ...
```

---

## 4. The 12-Step Lifecycle (Orchestrator)

The order is fixed. Adapters cannot change it.

| Step | Name | Who does it | What happens |
|------|------|-------------|--------------|
| 1 | Create ingestion_run | Orchestrator | INSERT into `ingestion_runs` (run_id, dataset_id, started_at, status='RUNNING'). Returns run_id. |
| 2 | Load dataset config | Orchestrator | Load config (e.g. YAML) for delete_strategy, validation. Currently stub returns {}. |
| 3 | Fetch | Adapter + Orchestrator | Orchestrator calls `adapter.fetch(**kwargs)` with **retry** (exponential backoff). Adapter does not retry. |
| 4 | Store raw payload | Orchestrator | DataFrame → `ingest_raw_df(df, dataset_id, run_id)` + `discover_fields(dataset_id)`. For GIE (dict raw) → `ingest_raw_json(...)`. |
| 5 | Parse | Adapter | `adapter.parse(raw)` must return a **list** of records (e.g. list of row dicts). |
| 6 | Normalize | Adapter | For each record, `adapter.normalize(record)`. Result can be one dict or a list of dicts; orchestrator flattens. Output: list of observation-like dicts (e.g. series_id, observation_time, value, quality_flag, raw_payload). |
| 7 | Validate | Orchestrator | Uses `adapter.get_validation_config()` (optional). Checks min_row_count, required_fields, date_range. On failure → marks run FAILED, raises ValidationError. |
| 8 | Apply delete policy | Orchestrator | Config: delete_strategy (e.g. last_n_days), delete_window_days → delete old rows from data_observations. For GIE (AGSI/ALSI) → delete from energy.daily by source. |
| 9 | Register canonical series | Adapter + Orchestrator | `adapter.define_series(normalized)` returns list of series meta dicts. Orchestrator inserts into `meta_series` (or for GIE, series are created during step 10). |
| 10 | Bulk insert observations | Orchestrator | For non-GIE: `upsert_observations(normalized, run_id)`. For GIE: `insert_gie_rows(source, normalized)` (energy.daily + meta.series/meta.assets). |
| 11 | Finalize ingestion_run | Orchestrator | UPDATE ingestion_runs SET finished_at, status (SUCCESS/FAILED), rows_fetched, rows_inserted, rows_deleted, error_message. |
| 12 | Emit metrics | Orchestrator | Stub; can wire to your metrics system. |

---

## 5. Adapter Contract (BaseAdapter)

Adapters **must not**: write to DB, delete records, retry requests, log ingestion metrics, or control lifecycle.

Adapters **only**:

| Method | Return | Responsibility |
|--------|--------|----------------|
| `fetch(**kwargs)` | Raw (e.g. DataFrame or dict) | Call external API; return raw response. No retry. |
| `parse(raw)` | `List[Any]` | Turn raw into list of records (e.g. list of row dicts). |
| `normalize(record)` | One dict or list of dicts | Turn one record into internal observation shape (e.g. series_id, observation_time, value, quality_flag, raw_payload). One row may become multiple observations. |
| `define_series(normalized_records)` | `List[dict]` | From normalized list, derive unique series and return list of series meta dicts (e.g. series_id, source, dataset_id, description, unit, frequency, timezone_source, is_active). For GIE this can return [] (series created on insert). |
| `get_time_field()` | `str` | Name of the datetime field used for delete policy (e.g. `"observation_time"` or `"date"`). |

Optional:

- `get_validation_config()` → dict with `required_fields`, `min_row_count`, `date_range` (min_date, max_date). Default: `{}`.

---

## 6. End-to-End Flow (Example: POST /v2/ingest/gas)

1. Client calls `POST /v2/ingest/gas?from_date=2024-01-01&to_date=2024-01-02`.
2. **ingestion.py** validates dates, then `background_tasks.add_task(_orchestrator.run, "GAS_QUALITY", from_date=..., to_date=..., site_ids=...)`.
3. **Orchestrator.run("GAS_QUALITY", ...)**:
   - Gets `NationalGasAdapter` class from registry, instantiates it.
   - Creates row in `ingestion_runs` (status RUNNING).
   - Calls `adapter.fetch(from_date=..., to_date=..., site_ids=...)` (with retry) → DataFrame.
   - Calls `ingest_raw_df(df, "GAS_QUALITY", run_id=run_id)`, then `discover_fields("GAS_QUALITY")`.
   - `adapter.parse(raw)` → list of row dicts.
   - For each row, `adapter.normalize(row)` → list of observation dicts; orchestrator flattens.
   - Runs validation (if adapter provides `get_validation_config`).
   - Applies delete policy if config has delete_strategy/delete_window_days.
   - `adapter.define_series(normalized)` → series meta list; orchestrator inserts into `meta_series`.
   - `upsert_observations(normalized, run_id=run_id)`.
   - Updates `ingestion_runs` (status SUCCESS, counts, finished_at).
4. API had already returned 202 Accepted; ingestion continues in background.

Same pattern for other POST ingest endpoints: they call `_orchestrator.run(dataset_id, ...)` or `ingest_dataset(dataset_id, ...)` which in turn runs the orchestrator.

---

## 7. Current Datasets and Storage

| dataset_id | Adapter | Raw storage | Series | Observations |
|------------|---------|-------------|--------|--------------|
| GAS_QUALITY | NationalGasAdapter | raw_events | meta_series | data_observations |
| ENTSOG | EntsogAdapter | raw_events | meta_series | data_observations |
| INSTANTANEOUS_FLOW | InstantaneousFlowAdapter | raw_events | meta_series | data_observations |
| GAS_PUBLICATIONS | GasPublicationsAdapter | raw_events | meta_series | data_observations |
| AGSI | GieAgsiAdapter | raw_events | meta.series (GIE) | energy.daily |
| ALSI | GieAlsiAdapter | raw_events | meta.series (GIE) | energy.daily |

GIE datasets use a different schema (energy.daily, meta.series, meta.assets). The orchestrator has a small branch for `dataset_id in ("AGSI", "ALSI")` for steps 4, 8, 9, and 10 (store raw as JSON, delete by source, skip meta_series insert, call insert_gie_rows).

---

## 8. How to Add a New API or Dataset

Follow these steps to add a new dataset (e.g. `"NEW_SOURCE"`) and expose it via a new or existing API.

### Step 1: Create the adapter file

Create `app/ingestion/adapters/new_source.py` (or a name that matches your dataset).

- Implement **BaseAdapter** (subclass it and implement all abstract methods).
- **fetch(kwargs):** Call your external API, return raw (DataFrame or dict). No DB, no retry.
- **parse(raw):** Return a list of records (e.g. `raw.to_dict('records')` if DataFrame, or list of dicts from JSON).
- **normalize(record):** From one record, return one or more dicts with at least: `series_id`, `observation_time`, `value`, `quality_flag`, `raw_payload`. If your storage is different (e.g. like GIE), match the shape that your loader expects.
- **define_series(normalized_records):** From normalized list, compute unique series and return list of dicts with keys expected by `meta_series` (series_id, source, dataset_id, data_item, description, unit, frequency, timezone_source, is_active). If you use a different store (e.g. GIE), return [] and handle series in your loader.
- **get_time_field():** Return the name of the datetime field used for delete policy (e.g. `"observation_time"`).

Optional: override **get_validation_config()** to return `required_fields`, `min_row_count`, or `date_range`.

Keep the same logic you had before (same URLs, same formulas, same series_id format); only move it into the adapter.

### Step 2: Register the adapter

In `app/ingestion/adapters/__init__.py`:

- Import your adapter class.
- Call `registry.register("NEW_SOURCE", NewSourceAdapter)` (use the same `dataset_id` string you will use everywhere).

Ensure `app.ingestion.adapters` is imported somewhere at startup (e.g. in `run_all.py` or in the API module that calls `ingest_dataset`) so that registration runs.

### Step 3: Wire the API (if needed)

- If you want a dedicated endpoint: add a new route (e.g. in `app/api/v2/ingestion.py` or a new router) that calls `_orchestrator.run("NEW_SOURCE", ...)` or `ingest_dataset("NEW_SOURCE", ...)` with the right query/body parameters.
- If you only need programmatic use: call `ingest_dataset("NEW_SOURCE", **kwargs)` (e.g. from a script or scheduler). No new endpoint required.

### Step 4: Storage behavior

- **Same as existing National Gas / ENTSOG:** Your adapter returns observations with `series_id`, `observation_time`, `value`, `quality_flag`, `raw_payload`. The orchestrator will store raw via `ingest_raw_df` (if raw is a DataFrame) or you’ll need to extend step 4 for dict raw like GIE. Series go to `meta_series`, observations to `upsert_observations`. No extra orchestrator change.
- **Different storage (e.g. like GIE):** You’ll need to extend the orchestrator in steps 4, 8, 9, and 10 for `dataset_id == "NEW_SOURCE"` (e.g. store raw your way, delete your way, skip or custom series registration, call your loader). Prefer reusing existing patterns (e.g. a shared “loader” and “delete” helper) to keep the orchestrator simple.

### Step 5: Add a validation script (recommended)

Under `scripts/validate_api/`, add e.g. `validate_new_source.py` that:

- Imports the registry and orchestrator and ensures adapters are loaded.
- Calls `Orchestrator(registry).run("NEW_SOURCE", ...)` with a small set of parameters.
- Queries the DB (raw, series, observations) and checks counts or sample data.
- Prints pass/fail so you can verify the new dataset end-to-end.

### Step 6: Optional: dataset config

If you want delete policy or validation to be config-driven, add a config (e.g. YAML) for `NEW_SOURCE` and implement `_load_dataset_config(dataset_id)` in the orchestrator to return it. Then step 2 (config) and steps 7–8 (validation, delete) will use it.

---

## 9. Checklist for a New Dataset

- [ ] Adapter class in `app/ingestion/adapters/` implementing all BaseAdapter methods.
- [ ] Same business logic as before (same API, same transforms, same series_id shape).
- [ ] Registered in `app/ingestion/adapters/__init__.py` with `registry.register("DATASET_ID", AdapterClass)`.
- [ ] API or script calls `ingest_dataset("DATASET_ID", ...)` or `_orchestrator.run("DATASET_ID", ...)` with correct kwargs.
- [ ] If storage is different, orchestrator steps 4/8/9/10 extended for this dataset_id (or a shared loader used).
- [ ] Validation script added under `scripts/validate_api/` and run to confirm.

---

## 10. Summary

- **Single entry point:** `ingest_dataset(dataset_id, **kwargs)` → registry lookup → `Orchestrator(registry).run(dataset_id, **kwargs)`.
- **No dataset conditionals** in the ingestion flow; routing is by registry.
- **Adapters** only handle fetch, parse, normalize, define_series, get_time_field (and optional validation config).
- **Orchestrator** runs the fixed 12-step lifecycle, uses existing shared components (raw_ingestor, loader, field_discovery, delete_policy), and writes to ingestion_runs.
- **To add a new API/dataset:** implement an adapter, register it, call `ingest_dataset` or `orchestrator.run`, and add a validation script; extend orchestrator only if storage or delete behavior is different (e.g. like GIE).
