# Dockerfile and Runtime Operations - Full Technical Document

## 1. Container Build Design

Base image:

- `ubuntu:24.04`

Installed packages:

- `git`, `python3`, `python3-pip`, `python3-venv`, `cron`, `vim`

Project layout in container:

- Code copied to `/root/WebSearchEngine`
- Virtualenv at `/root/system-venv`
- Dependencies installed from `/root/WebSearchEngine/Metric/requirments.txt`

## 2. Runtime Environment

- `SERPAPI_KEY` is expected as environment variable.
- Dockerfile currently includes placeholder default value and exports into `/etc/environment` for cron visibility.
- `NEON_URL` (Neon connection string for `migrate.py`) is passed at `docker run -e NEON_URL=...`; `entrypoint.sh` copies it into `/etc/environment` so cron jobs see it. It is never baked into the image or the repo.

## 3. Scheduled Jobs (Cron)

Configured in `/etc/cron.d/search-engine-cron`:

- Hourly at minute 0:
  - `measure.py --test --select_db_url ... --measure status`
  - `migrate.py`

- Monthly on day 1 and 16 at 12:00:
  - `measure.py --create --strategy random --keywordNums 50 --update 12 --test --measure crawler_all`

- Monthly on day 1 and 16 at 18:00:
  - `measure.py --create --strategy head --keywordNums 50 --update 12 --test --measure crawler_all`

- Daily at 06:00:
  - `measure.py --test --strategy random head --measure crawler_all --batch_age_days 7 14 27`
  - Re-measures each batch 7, 14 and 27 days after it was built (rows get `is_recheck = true`). Day 27 runs before `golden_inject` force-injects batches older than 4 weeks, so it is the last reading of natural discovery.

`--update 12`: the trending-keyword cache must be shorter than the shortest gap between runs (16 Feb -> 1 Mar is 13 days); with the old default of 14 the 1 Mar run reused the 16 Feb batch and overwrote its golden URLs.

All cron outputs append to `/var/log/cron.log`.

## 4. Runtime Process Model

Container command: `entrypoint.sh`

1. `measure.py --createtable --metric_db_url ...` upgrades the metricdb schema (idempotent).
2. Writes `NEON_URL` into `/etc/environment` when set.
3. `cron && tail -f /var/log/cron.log` keeps cron active and the container alive via log tailing.

## 5. Supporting Compose Service

`docker-compose.yml` defines `metric_postgres`:

- Image: `postgres:16`
- User/password/db: `metric/metric/metricdb`
- Port mapping: `5433:5432`
- Data volume: `/data/metric:/var/lib/postgresql/data`

## 6. Operations Diagram

```mermaid
flowchart TD
    A[Cron trigger] --> B[measure.py status]
    A --> C[migrate.py]
    A --> D[measure.py random crawler_all]
    A --> E[measure.py head crawler_all]
    A --> R[measure.py crawler_all --batch_age_days 7 14 27]

    B --> F[(metricdb crawler_stat_*)]
    D --> G[(metricdb metric_randomset_*)]
    E --> H[(metricdb metric_headset_*)]
    R --> G
    R --> H
    C --> I[(Neon reporting DB)]
```

