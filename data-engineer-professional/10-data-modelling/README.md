# 10 — Data Modelling (6%)

The last and smallest section, and it closes the loop: the medallion layers
you've been building since section 01, formalized, plus the star-schema and
SCD decisions that shape silver and gold. Two notebooks, both runnable — you
mint surrogate keys with a real identity column and run the full SCD2
union-staging MERGE, the trickiest SQL in this repo.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_medallion_and_star_schema.py`](01_medallion_and_star_schema.py) | The per-layer contract table (and which hop cleanses vs aggregates), `GENERATED ALWAYS AS IDENTITY` surrogate keys live (gaps, concurrency, ALWAYS vs BY DEFAULT), building a fact with an informational FK, when to denormalize, Data Vault in one paragraph |
| [`02_scd_modelling.py`](02_scd_modelling.py) | The SCD type ladder, a change batch applied via the union-staging SCD2 MERGE with the metrics walked row by row, then the same thing as `dlt.apply_changes(stored_as_scd_type=2)` — the code that actually built `silver_customers` |
| [`q_fafe65ac21_star_schema_layout.py`](q_fafe65ac21_star_schema_layout.py) | Exam question: lay out a 500 GB star-schema fact table. `CLUSTER BY (order_date, product_category)` plus broadcast dimensions run first, then the failing `PARTITIONED BY` + `CLUSTER BY`, high-cardinality partitioning with shuffle joins, and partitioned ZORDER, each measured on the same 600 orders |
| [`q_e148d36611_zorder_partition_limit.py`](q_e148d36611_zorder_partition_limit.py) | Exam question: the key limitation of Z-order versus liquid clustering. The same 8,000 events Z-ordered inside `region` partitions (one region per file), Z-ordered unpartitioned, and liquid clustered run first, then ZORDER on a column without statistics fixed by `ALTER TABLE SET TBLPROPERTIES`, the DBR 15.2 version matrix, and ZORDER skipping on an unpartitioned table |
| [`q_8626e948c5_liquid_clustering_benefits.py`](q_8626e948c5_liquid_clustering_benefits.py) | Exam question: why Liquid Clustering fixes small-partition skew, full rewrites and fixed keys. `CLUSTER BY (event_date, customer_id)` against `PARTITIONED BY (event_date)` + ZORDER run first (skewed days, the full repartition by both keys, incremental `OPTIMIZE`, `ALTER TABLE ... CLUSTER BY` with an unchanged file set), then unclustered files on arrival, the missing `event_date=` directories, the rejected partition + cluster hybrid, and the rejected `ZORDER BY` on a clustered table |

## Setup

Run section 01's `00_setup.py` first — both notebooks model on top of the
`de_pro` clones. Widgets default to `dev_catalog` / `de_pro` as usual.

## How to study

1. Layer questions are placement questions: raw + replayable = bronze,
   validated entities = silver, consumption shapes = gold. If an option
   cleanses in gold or aggregates in bronze, it's the distractor.
2. Run the SCD2 MERGE, then close the notebook and rewrite it from memory —
   the merge-key trick (and why an unchanged row produces nothing) is the
   single most exam-like exercise in this repo.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 6% weight ≈ 4 of 60 questions. That's the full 100% — do a
   timed full mock next.
