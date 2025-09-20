# Assignment 1 — End-to-End Big Data Pipeline (Local VS Code)

This project contains a complete solution scaffold for **PROG8850 – Assignment 1: End-to-End Big Data Pipeline Automation**.
It's designed to run locally (VS Code) against a local or Docker MySQL instance.

## Structure
```
assignment1_pipeline/
├─ scripts/
│  ├─ data_ingestion.py
│  ├─ backup_script.py
│  └─ deploy_changes.py
├─ schema.sql
├─ .env.example
├─ requirements.txt
└─ report/Assignment1_Report.pdf
```

## Quick setup (Windows / macOS / Linux)
1. Open in VS Code.
2. Create and activate a Python virtual environment:
   - `python -m venv venv`
   - Windows PowerShell: `venv\Scripts\Activate.ps1`
   - Windows CMD: `venv\Scripts\activate.bat`
   - macOS/Linux: `source venv/bin/activate`
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in MySQL credentials.

5. Initialize the database schema:
   ```bash
   mysql -u $DB_USER -p -h $DB_HOST $DB_NAME < schema.sql
   ```
   (or use the `deploy_changes.py` script to create required tables)

## How to run scripts
- Ingest one month (example):
  ```bash
  python scripts/data_ingestion.py --url "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.csv" --chunksize 20000
  ```
- Backup the database:
  ```bash
  python scripts/backup_script.py
  ```
- Apply schema changes safely:
  ```bash
  python scripts/deploy_changes.py
  ```

## Notes
- Do NOT upload your `.env` or raw data to any public repo.
- The scripts include logging and basic error handling.
- The ingestion script logs rows/sec, memory usage, and verifies aggregates after load.

If you'd like, I can also run through the code with you or adapt it to a different TLC month or to use Parquet inputs.
