# SkyLake

A personal data engineering project: a bronze → silver → gold pipeline for the
[2018 Airline Delay & Cancellation dataset](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018?select=2018.csv),
orchestrated with Airflow, stored as Apache Iceberg on S3, and queried with Athena.

## Architecture

```
Kaggle CSV(s) (2018 flight delays)
    → AWS S3 (raw/*.csv)
    → PySpark (local[4]): reader → cleaner → writer
    → Parquet (processed/flights_cleaned)
    → Iceberg table (flights.flight_events, AWS Glue catalog on S3)
    → AWS Athena queries the Glue-cataloged Iceberg table directly
    → Jupyter notebooks for ad hoc EDA
```

Orchestration: Airflow (Dockerized) runs a daily DAG with two tasks —
`extract_clean_and_write_parquet` → `write_iceberg_table` — with retries, SLAs,
and a failure callback.

## Project structure

```
SkyLake/
├── airflow/
│   ├── dags/flights_pipeline_dag.py   # DAG: extract/clean/write parquet -> write iceberg
│   └── Dockerfile                      # Airflow image + Java + PySpark
├── docker/
│   └── docker-compose.yml              # Airflow webserver + scheduler
├── spark/
│   ├── config.py                       # Spark session, S3/Iceberg/Glue config
│   ├── reader.py                       # Read raw CSV(s) from S3
│   ├── cleaner.py                      # Type/clean transformations
│   ├── writer.py                       # Write cleaned data to Parquet
│   ├── iceberg_writer.py               # Write Parquet to the Iceberg table
│   └── task_failure.py                 # Airflow on_failure_callback
├── tests/
│   ├── unit/                           # pytest for reader/cleaner
│   └── fixtures/                       # small sample CSV for tests
├── notebooks/                          # exploratory EDA only
├── dbt/                                 # parked — see note below
├── main.py                             # pipeline entrypoints used by the DAG
└── pyproject.toml
```

## Setup

Requires `uv`, Docker, and an AWS account with S3 + Glue + Athena access.

```bash
uv sync
cp .env.example .env   # fill in AWS_REGION, S3_BUCKET
```

`.env` is loaded via `python-dotenv` and mounted into the Airflow container via
`docker-compose.yml`. AWS credentials come from `~/.aws` (mounted read-only into
the container) via `DefaultAWSCredentialsProviderChain`.

### Run the pipeline manually

```bash
uv run python main.py
```

### Run via Airflow

```bash
cd docker
docker compose up --build
```

Airflow UI at `http://localhost:8080`, login `admin` / `admin`.

### Run tests

```bash
uv run pytest
```

## Why these choices

- **Iceberg + Glue catalog**: lets Athena query the table directly with no
  crawler — Iceberg writes metadata to Glue on every commit.
- **dbt was tried and dropped**: `dbt-spark`'s session method turned out to be
  poorly documented and its Glue-backed catalog only supports Iceberg tables,
  not views, which blocks dbt's `CREATE VIEW` materialization. The project's
  one transformation is simple enough to run directly in Athena instead.
- **SequentialExecutor**: fine for a single-DAG local learning project; not a
  production choice.

## Status / known gaps

- Ingestion is incremental: a manifest tracked in S3 (`raw/_manifest.json`)
  records which raw files have already been processed, so each run only picks
  up new files. Iceberg writes are self-healing — the writer checks whether
  `flight_events` exists and creates it on first run, then `append()`s on
  every run after that, rather than rebuilding the table each time.
- No CI, no BI layer yet.
