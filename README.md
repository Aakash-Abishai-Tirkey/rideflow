# RideFlow 🚕

A ride-hailing analytics platform built with a **zero-install data engineering stack**:
SQLite (OLTP + warehouse), Parquet data lake (pyarrow), pydantic data contracts,
a custom DAG orchestrator, and a Streamlit dashboard — all pure Python.

## Architecture

```mermaid
graph LR
    A[Simulator<br>'the app'] -->|writes| B[(SQLite OLTP)]
    B -->|watermarked extract| C[bronze/<br>raw JSONL]
    C -->|validate + dedupe| D[silver/<br>Parquet]
    D -->|star schema| E[(SQLite warehouse)]
    E --> F[Streamlit dashboard]
    E --> G[DQ checks + alerts]
```

## Quickstart

```bash
python -m rideflow.simulator.engine --days 7 --reset   # generate a week of rides
python -m rideflow.orchestrator run                    # bronze → silver → gold → DQ gate
python -m rideflow.orchestrator run                   # again: incremental (near no-op)
python -m rideflow.orchestrator run --backfill         # full reload, identical results
python -m rideflow.orchestrator status                 # run + task history
python -m rideflow.pipeline.dq run                     # data quality checks (standalone)
python -m rideflow.pipeline.dq history                 # DQ results over time
streamlit run rideflow/dashboard.py                    # explore the data
```

## Pipeline layers

| Layer | Format | Guarantee |
|---|---|---|
| bronze | JSONL, append-only, partitioned by ingestion date | raw & immutable — exactly what the source returned |
| silver | Parquet, compacted per (table, event date) | contract-validated (pydantic), deduped by PK, quarantine for rejects |
| gold | SQLite star schema | PK-upsert facts, rebuilt small dims, joins zone/date/user/driver |

Incremental correctness comes from **watermarks** (high-water mark minus a lookback
window for late-landing rows), a **processed-file ledger** (crashed runs replay
their bronze batches), and **idempotent materialization** (backfills are
byte-identical). Every run, task, attempt, and watermark lives in `data/state.db`.

## Data quality

A 13-check suite gates every pipeline run (`dq_gate` task — a failing check
fails the run after retries; `--no-strict` downgrades to record-only):

| Category | Checks |
|---|---|
| source ↔ gold reconciliation | rides, payments (count + revenue to the cent), events |
| gold integrity | PK uniqueness, payments→rides FK, rides→dims FK, status domain, surge range |
| freshness & volume | newest-ride lag, last-full-day volume vs 7-day mean |
| distribution health | failed-payment share, cancellation share |
| silver health | quarantine rate of the latest run |

Results land in `state.dq_results` with full history —
`python -m rideflow.pipeline.dq history` shows the trend.

Status: 🚧 under construction — see `docs/ROADMAP.md`.
