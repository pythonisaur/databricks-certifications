# 06 — Cost & Performance Optimisation (13%)

The second-heaviest section, and the most formulaic once you see the shape:
every question hands you a symptom and four levers, three of which are
plausible but wrong. The levers group cleanly — **layout** (files,
clustering), **storage** (VACUUM, retention), **plan** (joins, AQE, caching),
and **compute** (cluster type, Photon, serverless) — one notebook each.
**Runnable** on the section 01 clones: you manufacture a small-file problem
and compact it, trip the VACUUM safety check, and read join strategies off
real plans.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_file_layout_and_clustering.py`](01_file_layout_and_clustering.py) | Small-file problem created + measured + OPTIMIZE'd live, ZORDER, liquid clustering (`CLUSTER BY`, keys changed in place), partitioning vs clustering table, data-skipping stats, deletion vectors |
| [`02_vacuum_and_storage.py`](02_vacuum_and_storage.py) | `DRY RUN`, the 7-day retention guard tripped and (deliberately) overridden, time travel proven broken while history survives, log vs file retention, predictive optimization |
| [`03_query_tuning_aqe_and_caching.py`](03_query_tuning_aqe_and_caching.py) | Broadcast vs sort-merge read off `explain()`, AQE's three fixes (coalesce, dynamic strategy, skew), `shuffle.partitions = auto`, Spark cache vs disk cache, reading spill/skew in the Spark UI |
| [`04_compute_and_cost.py`](04_compute_and_cost.py) | Job vs all-purpose vs serverless economics, Photon, spot, auto-termination, `system.billing.usage` joined to list prices (runs live if granted), tags → chargeback, the symptom → lever drill table |
| [`q_ae1c6851d9_partition_pruning_cast.py`](q_ae1c6851d9_partition_pruning_cast.py) | Exam question: a partition filter that scans 800 of 1,000 files. The `PartitionFilters` of a DATE literal, a coerced string literal and a column-side `CAST` run first, then unregistered partition directories and `MSCK REPAIR TABLE` (stand-in catalog), then ZORDER on Parquet, AQE off, the missing `CartesianProduct` and `OPTIMIZE` with no transaction log, each on the same ~1,000 Parquet files |

## Setup

Run section 01's `00_setup.py` first — notebooks 01–03 work on scratch copies
of the `de_pro` clones. Widgets default to `dev_catalog` / `de_pro` as usual.

## How to study

1. Learn the two-part framing in notebook 04: **cluster levers change $/hour,
   layout and plan levers change hours.** Classify the question first; half
   the distractors are levers from the wrong half.
2. The wrong answers are stable across exams: partition by high-cardinality
   key, broadcast the big table, `df.cache()` as a cure-all, all-purpose
   clusters for scheduled jobs, VACUUM to "speed up queries". Recognize them
   on sight.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 13% weight ≈ 8 of 60 questions.
