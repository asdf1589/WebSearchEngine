# migrate.py - Full Technical Document

## 1. Scope and Role

`migrate.py` copies finalized metric/stat tables from a source PostgreSQL instance into a destination PostgreSQL instance (Neon in current script).

This is an ETL-style table replication utility, intended for BI/reporting consumption.

## 2. Source and Destination URLs

- Source (`SOURCE_URL`, hard-coded): `postgresql+psycopg2://metric:metric@172.16.191.1:5433/metricdb`
- Destination (`DEST_URL`): read from the `NEON_URL` environment variable (Neon PostgreSQL with `sslmode=require`). The script exits without copying when it is unset. The connection string contains the Neon password, so it must not be committed.

## 3. Table Scope

`TABLES_TO_COPY`:

- `crawler_stat_a`
- `crawler_stat_b`
- `crawler_stat_total`
- `metric_headset_a`
- `metric_headset_b`
- `metric_headset_total`
- `metric_randomset_a`
- `metric_randomset_b`
- `metric_randomset_total`

These are exactly the reporting output tables produced by measurement jobs.

Because destination tables are recreated with `if_exists='replace'`, schema changes on the source (for example the `batch_id` / `measured_at` / `batch_age_days` / `is_recheck` columns on the coverage tables) reach Neon on the next run without manual changes there.

## 4. Migration Mechanics

For each table:

1. Read all rows with `SELECT * FROM public.<table_name>` into pandas DataFrame.
2. If row count > 0, write using `DataFrame.to_sql(..., if_exists='replace', index=False, chunksize=1000)`.
3. Continue on exceptions (per-table fault isolation).

## 5. Behavior Details

- `if_exists='replace'` drops/recreates destination table per run.
- Schema is inferred from pandas/sqlalchemy dtype mapping.
- Empty tables are skipped (no destination overwrite).
- Runtime per table is measured and printed.

## 6. Data Flow

```mermaid
flowchart LR
    S[(Source metricdb)] -->|SELECT *| P[pandas DataFrame]
    P -->|to_sql replace| D[(Destination Neon DB)]
```

## 7. Risks and Engineering Considerations

- `replace` can drop constraints/indexes defined manually at destination.
- For very large tables, streaming/chunked read is safer than full DataFrame load.
- No transaction envelope across all tables; migration is eventually consistent per table.

