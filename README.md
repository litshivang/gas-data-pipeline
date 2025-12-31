# Gas Data Pipeline

## Project Overview
Automated data ingestion and access platform for National Gas time-series data.

## Architecture Overview
```
National Gas API
        ↓
Ingestion Engine
        ↓
PostgreSQL
        ↓
Scheduler (Hourly)
        ↓
FastAPI / Python Client
```

## Tech Stack
- Python 3.10+
- PostgreSQL
- SQLAlchemy
- APScheduler
- FastAPI
- Pandas

## Setup (Linux)

### 1. Clone repo
```bash
git clone <repo>
cd gas-data-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

### 4. Configure environment
Create `.env`:
```env
DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/gas_data
```

## Database Setup
```bash
python -m app.db.init_db
```

## Run Scheduler
```bash
python -m scripts.start_scheduler
```

## Run API
```bash
uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

## Python Usage
```python
import gas_client

df = gas_client.get_history("UK_NBP_DEMAND", last_days=7)
```

## API Docs
```
http://<server>:8000/docs
```
