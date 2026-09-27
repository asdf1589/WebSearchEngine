# measure.py - Full Technical Document

## 1. Scope and Role

`measure.py` is the main orchestrator for:

- Creating metric tables.
- Generating golden datasets from trending keyword sources.
- Executing measurement jobs (crawler status and coverage metrics).

It coordinates three layers:

- Data source ingestion (`Metric.RawDataReader`).
- Golden set generation (`Metric.Query`).
- Metric computation and persistence (`Metric.Measure`).

## 2. CLI Contract

`measure.py` exposes the following arguments:

- `--strategy [random|head]...`: Which golden-set strategy(ies) to run.
- `--crawler_db_url`: Host:port for crawler PostgreSQL.
- `--metric_db_url`: Host:port for metric PostgreSQL.
- `--select_db_url`: Host:port for selectdb PostgreSQL (required for `crawler_all` index coverage).
- `--create`: Build/update golden sets.
- `--rawdatareader db`: Read trending raw data via DB-backed reader.
- `--update <days>`: Raw-data cache TTL in days (default: 14).
- `--keywordNums <N>`: Number of keywords for strategy (default: 100).
- `--test`: Execute measure phase.
- `--typesense_url`: Reserved/not actively used by current active measures.
- `--measure [status|rank|crawler_all|all]...`: Which measure(s) to execute.
- `--batch_id <id>`: `crawler_all` measures this batch instead of the latest one.
- `--batch_age_days <N>...`: `crawler_all` measures every batch created exactly N days ago (used by the daily re-measure cron with `7 14 27`).
- `--createtable`: Create metric tables and upgrade existing ones (see 3.1) before execution.

## 3. Runtime Modes

### 3.1 Table creation mode

- If `--createtable` is set:
  - Calls `createAllMetricModel()` to register dynamic ORM models:
    - `crawler_stat_total`, `crawler_stat_a`, `crawler_stat_b`
    - `metric_headset_total`, `metric_headset_a`, `metric_headset_b`
    - `metric_randomset_total`, `metric_randomset_a`, `metric_randomset_b`
  - Calls `createDB(..., createTable=True, base=MetricBase)` for metric DB.
  - Calls `Database/migrations.py:migrate_metric_db()`. `create_all` never alters an existing table, so this adds the new columns to existing tables: `batch_id` / `measured_at` / `batch_age_days` / `is_recheck` on the six coverage tables (primary key becomes `(batch_id, stat_date)`, old rows get the latest batch created on or before their `stat_date`), and the `url_canonical` + crawler-detail columns on `metric_url`. On the first upgrade it also turns never-measured zeros into NULL (`indexed_*` before selectdb was wired in, all `ranked_*`, `crawler_stat_*.indexed`). Safe to re-run; the container runs it on every start (`entrypoint.sh`) and exits without starting cron if it fails.

### 3.2 Dataset creation mode (`--create`)

Flow:

1. Build `DatabaseRawDataReader(metricDB, modelFactory, update_day)`.
2. `readData()` returns trending keywords:
   - Uses latest cached `metric_batches` entry if fresh.
   - Otherwise fetches from SerpApi and persists new batch/query rows.
3. Read latest batch id via `get_latest_batch_id()`.
4. For each strategy:
   - `random`: sample `keywordNums` keywords.
   - `head`: pick top `keywordNums` by frequency.
5. For each chosen keyword:
   - Query Google organic results via SerpApi.
   - Upsert `metric_queries.tags` with strategy tag.
   - Replace associated rows in `metric_url`.
6. Recompute batch metadata (`meta_total_queries`, `meta_total_urls`, `meta_tag_stats`).

### 3.3 Measure execution mode (`--test`)

- `status`: runs `CrawlerStatusMeasure`.
- `status`: `indexed` on `crawler_stat_total` is `count(*)` of `selectdb.selected_urls_current` (needs `--select_db_url`; NULL otherwise, and a failed count keeps the value measured earlier that day). Team A/B tables have no selectdb split, so their `indexed` is NULL.
- `crawler_all`: for each batch (latest, `--batch_id`, or `--batch_age_days`) and each selected strategy tag (`head`/`random`), runs `CrawlerAllMetricMeasure`.
  - Golden URLs are passed through w3lib `canonicalize_url` (the spider's key) before matching; the raw spelling is looked up too, for rows injected before `golden_inject` canonicalized.
  - Queries `selectdb.selected_urls_current` to determine `is_indexed` flag per golden URL.
  - Without `--select_db_url`, or if selectdb fails, `indexed_num` / `indexed_rate` are written as NULL (not measured) and the other columns are still written.
  - `is_recheck` is decided per batch from its age, not from `--create`: a batch created today (`batch_age_days = 0`) is the t0 measurement (`is_recheck = false`) and updates the per-URL labels in `metric_url`. Measuring any older batch writes `is_recheck = true` and puts its per-URL labels in `metric_url_recheck`, so the t0 labels are kept. A manual `--test` on the day a batch was built therefore also counts as t0 and overwrites that day's t0 row.
  - If any `url_state_current_*` shard cannot be scanned, the run aborts without writing coverage, rather than writing an undercount.
- `rank`: currently no-op in active implementation.

## 4. DB Initialization

`createDB(user, password, url, name)` composes:

- `postgresql+psycopg2://{user}:{password}@{url}/{name}`

Effective defaults from `measure.py`:

- Crawler DB credentials: `crawler:crawler`, DB name `crawlerdb`.
- Metric DB credentials: `metric:metric`, DB name `metricdb`.
- Select DB credentials: `select:select`, DB name `selectdb`.

## 5. Key Internal Functions

### 5.1 `get_latest_batch_id`

- SQLAlchemy `select(MetricBatch.id).order_by(id desc).limit(1)`.
- Returns latest batch id or `None`.
- Used by dataset generation and coverage measuring.

### 5.2 `createDataset`

Responsibilities:

- Read or refresh raw trending keyword data.
- Run one or more golden-set strategies.
- Persist/refresh query->URL mapping in metric DB.

### 5.3 `test`

Responsibilities:

- Run measurement classes against crawler + metric + select DBs.
- Persist computed KPI tables via UPSERT.

## 6. Control Flow Diagram

```mermaid
flowchart TD
    A[CLI args] --> B{--createtable?}
    B -->|yes| C[Create dynamic metric models + create tables]
    B -->|no| D[Connect crawlerDB + metricDB + selectDB]
    C --> D

    D --> E{--create?}
    E -->|yes| F[DatabaseRawDataReader.readData]
    F --> G{strategy contains random/head}
    G --> H[RandomQueryStrategy.getGoldenSet]
    G --> I[HeadQueryStrategy.getGoldenSet]

    D --> J{--test?}
    J -->|yes| K{measure contains status/crawler_all}
    K --> L[CrawlerStatusMeasure.test]
    K --> M[CrawlerAllMetricMeasure.test per strategy tag]
    M --> N[Query selectdb.selected_urls_current for is_indexed]
```

## 7. Operational Notes

- `rank` pathway is not active; `ranked_num` / `ranked_rate` are written as NULL.
- `indexed` is populated from `selectdb.selected_urls_current` when `--select_db_url` is provided.
- `indexed` means "selected by IndexSelection", which does not require the page to have been fetched, so IndexCov can exceed CrawlCov. `metric_url.is_indexed AND NOT is_crawled` counts those URLs.
- `--measure all` appears in choices but no explicit branch handles it in current code.

