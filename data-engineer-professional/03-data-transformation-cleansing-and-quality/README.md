# 03 — Data Transformation, Cleansing, and Quality (10%)

Section 01 taught the transformations; this section is about **trust**: what
stops bad data (enforced constraints), what merely watches it (informational
PK/FK), what sets it aside (expectations / quarantine), and how duplicates and
late events are handled without unbounded state. **Runnable notebooks** on a
deliberately dirtied clone — you make the mess in setup, then clean it three
different ways.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Builds `bronze_orders_dirty` from section 01's clone: exact duplicates, NULL keys, impossible amounts — all deterministic. **Requires section 01's `00_setup` to have run.** |
| [`01_constraints_and_enforcement.py`](01_constraints_and_enforcement.py) | `NOT NULL`, `CHECK` (add-time full scan!), violations failing whole transactions live, PK/FK proven unenforced by inserting a duplicate key, `RELY`, where constraints live in TBLPROPERTIES |
| [`02_deduplication_and_late_data.py`](02_deduplication_and_late_data.py) | Exact vs keyed dedup, `dropDuplicates` arbitrariness, insert-only MERGE (idempotent appends, re-run = 0 inserts), streaming dedup with watermark, `dropDuplicatesWithinWatermark`, watermark admission-vs-cleanup semantics |
| [`03_expectations_and_quarantine.py`](03_expectations_and_quarantine.py) | Rules-as-a-dict quarantine pattern run live (split + per-rule counts), then the same rules as DLT `expect` / `expect_or_drop` / `expect_or_fail` with the policy table and event-log gotchas |
| [`q_387cf01a44_autoloader_rescue_mode.py`](q_387cf01a44_autoloader_rescue_mode.py) | Exam question: Auto Loader that keeps running on new fields and type mismatches. `rescue` mode run live over two batches (nothing dropped, all in `_rescued_data`), then `badRecordsPath`, `addNewColumns` + deleted checkpoint (fail, restart, duplicates) and explicit schema + `addNewColumns` (rejected; nulls without it). Self-contained in `dev_catalog.de_pro_qnotes` |

## Setup

Run section 01's `00_setup.py` first (it creates the `de_pro` clones), then
this folder's `00_setup.py`. Widgets default to `dev_catalog` / `de_pro`
everywhere, as usual.

## How to study

1. Keep the three enforcement behaviors straight — it's the whole section:
   constraint = **write fails**, expectation = **row counted/dropped, pipeline
   continues** (except `_or_fail`), PK/FK = **nothing happens**. Every question
   here is one of those three in disguise.
2. Re-run the MERGE cell in notebook 02 and watch `num_inserted_rows` go to
   zero — idempotent append is the pattern the exam pairs with "the job
   sometimes retries".
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 6 of 60 questions.
