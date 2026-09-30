# Measurement & Metrics Documentation

This documentation set focuses on the measurement and metrics subsystem and intentionally excludes index-selection internals.

## Document Map

1. [01-measure-py.md](./01-measure-py.md): Full walkthrough of `measure.py` orchestration and runtime behavior.
2. [02-migrate-py.md](./02-migrate-py.md): Full walkthrough of `migrate.py` cross-database table migration.
3. [03-dockerfile-operations.md](./03-dockerfile-operations.md): Container runtime, dependencies, cron schedules, and operations model.
4. [04-database-schema.md](./04-database-schema.md): Detailed SQL table design across crawler and metric domains.
5. [05-metric-pipeline-and-queries.md](./05-metric-pipeline-and-queries.md): End-to-end data flow, architecture, metric formulas, SQL query patterns, and URL sources.
6. [06-rollout-checklist.md](./06-rollout-checklist.md): 上線前要做的事 — the first deployment's schema upgrade and the manual SQL fixes to run after it.
