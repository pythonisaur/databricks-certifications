# 01 — Developing Code for Data Processing using Python and SQL (22%)

The heaviest section of the exam — **runnable Databricks notebooks**, not just
notes. Everything targets `dev_catalog.de_pro`, a study schema cloned from the
real [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter)
deployment, so you can mutate tables freely and reset anytime.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Creates `dev_catalog.de_pro`: deep-clones the six medallion tables + a checkpoints volume. **Run this first, and re-run it to reset.** |
| [`01_dataframe_transformations.py`](01_dataframe_transformations.py) | Casting bad bronze data, `when/otherwise`, `selectExpr`, SCD2 join fan-out (bug demonstrated live), point-in-time joins, NULL rules in aggregates, narrow vs wide |
| [`02_window_functions.py`](02_window_functions.py) | `row_number` vs `rank` vs `dense_rank`, running totals and frames, `lag`/`lead`, latest-row-per-key CDC dedup |
| [`03_udfs_and_higher_order_functions.py`](03_udfs_and_higher_order_functions.py) | Built-in vs Python UDF vs pandas UDF (same logic 3 ways), SQL UDFs in Unity Catalog, `transform`/`filter`/`aggregate`/`exists` |
| [`04_delta_dml_and_merge.py`](04_delta_dml_and_merge.py) | CDC dedup-then-MERGE, `WHEN NOT MATCHED BY SOURCE`, UPDATE/DELETE, DESCRIBE HISTORY, time travel, RESTORE, MERGE schema evolution |
| [`05_structured_streaming.py`](05_structured_streaming.py) | Delta-to-Delta streams, checkpoint semantics (proved live), Auto Loader on the real raw volume, Change Data Feed, trigger cheat sheet |
| [`q_63a961c4d4_streaming_vs_mv.py`](q_63a961c4d4_streaming_vs_mv.py) | Exam question: Streaming Tables vs Materialized Views for Silver and Gold. Three pipeline updates run first (an Auto Loader stand-in with a checkpoint processes 40, 0, then 30 new rows; a Materialized View stand-in recomputes Gold and re-files history after a `dim_stores` change), then a stale streaming Gold, MV aggregates and window functions, both stored as Delta, and AUTO CDC updates/deletes on a streaming target. Self-contained in `dev_catalog.de_pro_qnotes` |

## Setup

Import this folder into your workspace (Repos → your fork, or
`databricks workspace import-dir`), open `00_setup.py`, run it. Prereq: the
lakehouse-starter bundle deployed to the same catalog
(`databricks bundle deploy -t dev && databricks bundle run lakehouse_job -t dev`).

Every notebook has `catalog` / `schema` widgets defaulting to
`dev_catalog` / `de_pro` — change them if your setup differs.

## How to study

1. **Predict each cell's output before running it.** The exam shows code and
   asks what happens — the notebooks are built around traps (fan-out joins,
   NULL sums, checkpoint replays) that you'll now have *seen*, not memorized.
2. Streaming notebook: run the seed job between passes so Auto Loader actually
   has new files to pick up.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 22% weight ≈ 13 of 60 questions.
