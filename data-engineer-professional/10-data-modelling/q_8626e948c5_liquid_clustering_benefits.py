# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Liquid Clustering fixes partition skew and fixed keys
# MAGIC
# MAGIC Data Engineer Professional, section 10 of the exam guide, "Data Modelling".
# MAGIC Objective: identify the benefits of Liquid Clustering over Hive-style partitioning and ZORDER.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the notebook builds 3,000 fact rows (20 days, 30 customers, four busy
# MAGIC days and sixteen quiet ones, all derived from `range()` and `hash()` so every run is identical) and lays them out
# MAGIC the way the question describes: Hive-style `PARTITIONED BY (event_date)` plus `OPTIMIZE ... ZORDER BY
# MAGIC (customer_id)`, the attempted repartition by both columns, and a Liquid Clustered table with
# MAGIC `CLUSTER BY (event_date, customer_id)`. Everything lives in `dev_catalog.de_pro_qnotes` under a `q8626_` prefix;
# MAGIC nothing else is touched. A small `delta.targetFileSize` makes per-file layout visible at lab size.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineer designs a large Delta fact table that is frequently filtered on `event_date` and, increasingly,
# MAGIC on `customer_id`. The current design uses Hive-style partitioning by `event_date` plus periodic
# MAGIC `OPTIMIZE ... ZORDER BY (customer_id)`. Problems observed: many small partitions for low-volume days, expensive
# MAGIC full rewrites when they tried to also partition by `customer_id`, and the inability to change the clustering keys
# MAGIC without rewriting the whole table. The engineer evaluates Liquid Clustering. Which statement correctly describes
# MAGIC why Liquid Clustering addresses these problems?
# MAGIC
# MAGIC * **A** Liquid Clustering sorts each write fully on the `CLUSTER BY` keys at ingestion time; it removes the need for `OPTIMIZE`, keeps files perfectly clustered on arrival, and needs no background maintenance or predictive optimization.
# MAGIC * **B** Liquid Clustering is a faster form of Hive partitioning with one directory per key value; it compacts small partitions on write, but the keys are fixed at table creation and changing them requires a full table rewrite.
# MAGIC * **C** Liquid Clustering speeds up point lookups on a single key column; to filter on both `event_date` and `customer_id` you keep Hive partitioning by `event_date` and add Liquid Clustering on `customer_id` alone.
# MAGIC * **D** Liquid Clustering records the `CLUSTER BY` keys as layout metadata; the data ordering is still produced by running `OPTIMIZE ... ZORDER BY` after each write, and changing the keys means reordering existing files.
# MAGIC * **E** Liquid Clustering replaces partitioning and ZORDER with `CLUSTER BY` keys; it avoids small-partition skew, clusters new data incrementally without full rewrites, and lets you change the keys without rewriting existing data.
# MAGIC
# MAGIC The correct option (E) is built and run first, one claim at a time against the three observed problems; then
# MAGIC A, B, C and D are each run against the same rows so you can see what really happens.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "8626e948c5",
    "track": "de_pro",
    "section": "Data Modelling",
    "stem": "A data engineer designs a large Delta fact table that is frequently filtered on `event_date` and, increasingly, on `customer_id`. The current design uses Hive-style partitioning by `event_date` plus periodic `OPTIMIZE ... ZORDER BY (customer_id)`. Problems observed: many small partitions for low-volume days, expensive full rewrites when they tried to also partition by `customer_id`, and the inability to change the clustering keys without rewriting the whole table. The engineer evaluates Liquid Clustering. Which statement correctly describes why Liquid Clustering addresses these problems?",
    "correct": "E",
    "options": {
        "A": "Liquid Clustering sorts each write fully on the `CLUSTER BY` keys at ingestion time; it removes the need for `OPTIMIZE`, keeps files perfectly clustered on arrival, and needs no background maintenance or predictive optimization.",
        "B": "Liquid Clustering is a faster form of Hive partitioning with one directory per key value; it compacts small partitions on write, but the keys are fixed at table creation and changing them requires a full table rewrite.",
        "C": "Liquid Clustering speeds up point lookups on a single key column; to filter on both `event_date` and `customer_id` you keep Hive partitioning by `event_date` and add Liquid Clustering on `customer_id` alone.",
        "D": "Liquid Clustering records the `CLUSTER BY` keys as layout metadata; the data ordering is still produced by running `OPTIMIZE ... ZORDER BY` after each write, and changing the keys means reordering existing files.",
        "E": "Liquid Clustering replaces partitioning and ZORDER with `CLUSTER BY` keys; it avoids small-partition skew, clusters new data incrementally without full rewrites, and lets you change the keys without rewriting existing data."
    },
    "objective": "Identify the benefits of Liquid Clustering over Hive-style partitioning and ZORDER.",
    "source": "testlogichub exams/fixes/qfix/8626e948c5.py"
}
# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and helpers (real errors, per-file statistics)
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")

TARGET_FILE = "'delta.targetFileSize' = '16kb'"   # tiny on purpose: several output files at lab size
COLUMNS = "event_id BIGINT, event_date DATE, customer_id BIGINT, amount DOUBLE"
PROBE_DATE, PROBE_CUSTOMER = "2026-10-02", 7       # a busy day and one customer: the query the table must serve

def run(stmt: str):
    """Run one SQL statement; return the DataFrame, or print the real error (first line) and return None."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {type(e).__name__}: {first[:300]}")
        return None

def files_of(table: str) -> set:
    return {r.f for r in spark.sql(f"SELECT DISTINCT _metadata.file_path AS f FROM {table}").collect()}

def layout(table: str) -> dict:
    """Per-file layout: file count, smallest and largest file in rows, and how many files the probe
    `event_date = PROBE_DATE AND customer_id = PROBE_CUSTOMER` must open (a file is skipped when either value
    falls outside that file's min/max)."""
    s = spark.sql(f"""
        SELECT _metadata.file_path AS f, count(*) AS n, min(event_date) AS d0, max(event_date) AS d1,
               min(customer_id) AS c0, max(customer_id) AS c1
        FROM {table} GROUP BY _metadata.file_path""").collect()
    probe = [r for r in s if str(r.d0) <= PROBE_DATE <= str(r.d1) and r.c0 <= PROBE_CUSTOMER <= r.c1]
    return {"files": len(s), "min rows/file": min(r.n for r in s), "max rows/file": max(r.n for r in s),
            "files read by probe": len(probe)}

def optimize_metrics(df, label: str = "") -> None:
    if df is not None:
        m = df.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        print(f"{label}OPTIMIZE added {m.numFilesAdded} files, removed {m.numFilesRemoved}")

print("working schema:", FQ)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'q8626_*'").collect():
    if not t.isTemporary:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: 3,000 fact rows with skewed days
# MAGIC
# MAGIC | column | values | role in the lesson |
# MAGIC |---|---|---|
# MAGIC | `event_date` | 20 days from 2026-10-01; four busy days hold about 80% of rows, sixteen quiet days share the rest | the old partition column; the quiet days are the "low-volume days" of the question |
# MAGIC | `customer_id` | 1 to 30 | the old ZORDER column, now a second filter column |
# MAGIC | `amount` | 0.00 to 199.99 | payload |
# MAGIC
# MAGIC Rows are inserted in batches of 750 in `event_id` order, the way a daily pipeline appends, so freshly written
# MAGIC files hold a mix of days and customers until something clusters them.

# COMMAND ----------

# DBTITLE 1,The fact rows (a temp view, deterministic) and the batch loader
spark.sql("""
    CREATE OR REPLACE TEMP VIEW q8626_events_src AS
    SELECT id AS event_id,
           date_add(DATE'2026-10-01',
                    CASE WHEN abs(hash(id, 'v')) % 10 < 8 THEN cast(abs(hash(id, 'b')) % 4 AS INT)
                         ELSE 4 + cast(abs(hash(id, 'q')) % 16 AS INT) END) AS event_date,
           cast(abs(hash(id, 'c')) % 30 + 1 AS BIGINT) AS customer_id,
           cast(round((abs(hash(id, 'amt')) % 20000) / 100.0, 2) AS DOUBLE) AS amount
    FROM range(1, 3001)""")
display(spark.sql("""SELECT event_date, count(*) AS rows FROM q8626_events_src
                     GROUP BY event_date ORDER BY event_date"""))

def load(table: str, batches=range(4)) -> None:
    for b in batches:
        spark.sql(f"INSERT INTO {FQ}.{table} SELECT * FROM q8626_events_src WHERE (event_id - 1) DIV 750 = {b}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option E, run first: `CLUSTER BY` replaces partitioning and ZORDER
# MAGIC
# MAGIC E makes three claims, one per observed problem. Each is run below against the old design.
# MAGIC
# MAGIC 1. **It avoids small-partition skew.** Liquid Clustering uses `CLUSTER BY` keys instead of rigid partition
# MAGIC    directories, so a quiet day does not get its own directory of tiny files.
# MAGIC 2. **It clusters new data incrementally, without full rewrites.** `OPTIMIZE` on a clustered table clusters what
# MAGIC    was written since the last run; it does not rebuild the table, and a second key column costs no repartition.
# MAGIC 3. **The keys can change without rewriting existing data.** `ALTER TABLE ... CLUSTER BY (...)` changes metadata;
# MAGIC    existing files stay where they are and the new keys apply to data clustered from then on.

# COMMAND ----------

# DBTITLE 1,Option E, claim 1: the old design (partition by date + ZORDER) against CLUSTER BY on both columns
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.q8626_hive ({COLUMNS}) PARTITIONED BY (event_date) TBLPROPERTIES ({TARGET_FILE})")
load("q8626_hive")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.q8626_hive ZORDER BY (customer_id)"), "hive:   ")

spark.sql(f"CREATE OR REPLACE TABLE {FQ}.q8626_liquid ({COLUMNS}) CLUSTER BY (event_date, customer_id) TBLPROPERTIES ({TARGET_FILE})")
load("q8626_liquid")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.q8626_liquid"), "liquid: ")

LAYOUT = {"partitioned + ZORDER": layout(f"{FQ}.q8626_hive"), "CLUSTER BY (event_date, customer_id)": layout(f"{FQ}.q8626_liquid")}
for name, r in LAYOUT.items():
    print(f"{name:38s}", r)

display(spark.sql(f"""
    SELECT event_date, count(*) AS rows, count(DISTINCT _metadata.file_path) AS files
    FROM {FQ}.q8626_hive GROUP BY event_date ORDER BY rows"""))

# COMMAND ----------

# DBTITLE 1,Option E, claim 2: adding customer_id as a partition column means a full rewrite; clustering does not
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.q8626_hive_2keys PARTITIONED BY (event_date, customer_id)
              TBLPROPERTIES ({TARGET_FILE}) AS SELECT * FROM {FQ}.q8626_hive""")
two = spark.sql(f"""SELECT count(*) AS rows_rewritten, count(DISTINCT event_date, customer_id) AS partitions,
                           count(DISTINCT _metadata.file_path) AS files,
                           round(count(*) / count(DISTINCT _metadata.file_path), 1) AS avg_rows_per_file
                    FROM {FQ}.q8626_hive_2keys""").first()
print("repartition by (event_date, customer_id):", two.asDict())

# Incremental: a new day's batch lands on the clustered table, OPTIMIZE clusters it, the old files stay
before = files_of(f"{FQ}.q8626_liquid")
spark.sql(f"""INSERT INTO {FQ}.q8626_liquid
              SELECT event_id + 3000, DATE'2026-10-21', customer_id, amount FROM q8626_events_src WHERE event_id <= 300""")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.q8626_liquid"), "liquid after a new batch: ")
after = files_of(f"{FQ}.q8626_liquid")
print(f"clustered files before the batch: {len(before)}, still in place afterwards: {len(before & after)}, "
      f"files now: {len(after)}")

# COMMAND ----------

# DBTITLE 1,Option E, claim 3: change the clustering keys; no existing file is rewritten
before = files_of(f"{FQ}.q8626_liquid")
spark.sql(f"ALTER TABLE {FQ}.q8626_liquid CLUSTER BY (customer_id, event_date)")
after = files_of(f"{FQ}.q8626_liquid")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.q8626_liquid").select("partitionColumns", "clusteringColumns", "numFiles"))
print(f"files before ALTER: {len(before)}, after ALTER: {len(after)}, identical set: {before == after}")
display(spark.sql(f"DESCRIBE HISTORY {FQ}.q8626_liquid").select("version", "operation", "operationParameters")
        .orderBy("version", ascending=False).limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows, and why E is correct:
# MAGIC
# MAGIC * **Claim 1.** The partitioned table keeps one directory per day, so every quiet day still owns at least one
# MAGIC   file with a few dozen rows (the per-day table, sorted by rows, puts them at the top), and `min rows/file` on the
# MAGIC   partitioned side is small. The clustered table has no directories: quiet days share files with their
# MAGIC   neighbours, so there is no small-partition skew, and the probe on both columns still skips most files.
# MAGIC * **Claim 2.** Trying to also partition by `customer_id` rewrote every row into hundreds of tiny partitions;
# MAGIC   that is the "expensive full rewrite" of the question, and it makes small files worse. On the clustered table the
# MAGIC   second key cost nothing extra, and after a new batch `OPTIMIZE` clustered the new rows. Liquid clustering is
# MAGIC   incremental: it rewrites only data that is not yet clustered and leaves already clustered files alone (at lab
# MAGIC   size it may still merge a small group, but it never rebuilds the whole table). Compare that with
# MAGIC   `OPTIMIZE ... ZORDER BY`, which Databricks documents as not idempotent: each run can rewrite the partition again.
# MAGIC * **Claim 3.** After `ALTER TABLE ... CLUSTER BY (customer_id, event_date)` the file set is identical and
# MAGIC   `DESCRIBE DETAIL` shows the new `clusteringColumns`: the keys changed without rewriting existing data. New
# MAGIC   writes and later `OPTIMIZE` runs use the new keys (`OPTIMIZE ... FULL` re-clusters everything if you ever want
# MAGIC   that, by choice, not by obligation). With Hive partitioning, changing the layout key means rebuilding the
# MAGIC   table, exactly as claim 2 showed.
# MAGIC
# MAGIC E is the only option that targets all three observed problems at once.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: "each write is fully sorted, no `OPTIMIZE`, no maintenance"
# MAGIC
# MAGIC The misconception: A overstates what happens at write time. Liquid Clustering still benefits from `OPTIMIZE`
# MAGIC (or predictive optimization, which runs it for you on Unity Catalog managed tables) to cluster accumulated
# MAGIC data; writes are not guaranteed to be perfectly sorted with zero maintenance. Clustering on write only applies to
# MAGIC writes above a size threshold, and small appends like a daily batch land unclustered.

# COMMAND ----------

# DBTITLE 1,Option A: a clustered table straight after four inserts, then after OPTIMIZE
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.q8626_a_events ({COLUMNS}) CLUSTER BY (event_date, customer_id) TBLPROPERTIES ({TARGET_FILE})")
load("q8626_a_events")
print("on arrival   :", layout(f"{FQ}.q8626_a_events"))
display(spark.sql(f"""SELECT _metadata.file_path AS file, count(*) AS rows, min(event_date) AS first_day,
                             max(event_date) AS last_day, min(customer_id) AS c_min, max(customer_id) AS c_max
                      FROM {FQ}.q8626_a_events GROUP BY 1 ORDER BY 1"""))
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.q8626_a_events"))
print("after OPTIMIZE:", layout(f"{FQ}.q8626_a_events"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output: on arrival each file spans most days and most customers, so the probe has to
# MAGIC open nearly every file; the files were not "perfectly clustered on arrival". `OPTIMIZE` then added and removed
# MAGIC files and the probe count dropped. That work is the maintenance A says is unnecessary. In production you either
# MAGIC schedule `OPTIMIZE` in a Lakeflow Job or let predictive optimization run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: "a faster form of Hive partitioning, one directory per key value, keys fixed"
# MAGIC
# MAGIC The misconception: B confuses Liquid Clustering with Hive-style partitioning. Liquid Clustering does NOT create
# MAGIC per-value physical directories; that directory model is exactly what it replaces, and it does not require
# MAGIC rewriting the table to change keys.

# COMMAND ----------

# DBTITLE 1,Option B: look at the physical file paths of both tables
for name in ("q8626_hive", "q8626_liquid"):
    paths = sorted(files_of(f"{FQ}.{name}"))
    dirs = sorted({p.rsplit("/", 1)[0].rsplit("/", 1)[-1] for p in paths})
    print(f"{name:14s} files={len(paths):3d}  per-value directories (event_date=...): "
          f"{sum(d.startswith('event_date=') for d in dirs)}  e.g. {paths[0].rsplit('/', 2)[-2:]}")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.q8626_hive").select("name", "partitionColumns", "clusteringColumns")
        .union(spark.sql(f"DESCRIBE DETAIL {FQ}.q8626_liquid").select("name", "partitionColumns", "clusteringColumns")))

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output: the partitioned table writes one `event_date=...` directory per day; the
# MAGIC clustered table has zero such directories, its files sit side by side and `partitionColumns` is empty. And the
# MAGIC "keys fixed at creation" half was already disproved in section 2, claim 3: the keys of `q8626_liquid` were
# MAGIC changed with one `ALTER TABLE` and the file set did not change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option C: "single key only, so keep partitioning by date and cluster on `customer_id`"
# MAGIC
# MAGIC The misconception: C assumes Liquid Clustering is limited to one column. It supports multiple clustering
# MAGIC columns (up to four), so you cluster on both `event_date` and `customer_id` without Hive partitioning. The
# MAGIC hybrid C proposes is not even allowed: a table is either partitioned or liquid clustered.

# COMMAND ----------

# DBTITLE 1,Option C: the hybrid C proposes, then the two-column clustered table answering the same probe
print("PARTITIONED BY (event_date) + CLUSTER BY (customer_id):")
run(f"""CREATE OR REPLACE TABLE {FQ}.q8626_c_hybrid ({COLUMNS})
        PARTITIONED BY (event_date) CLUSTER BY (customer_id)""")

spark.sql(f"CREATE OR REPLACE TABLE {FQ}.q8626_c_two_keys ({COLUMNS}) CLUSTER BY (event_date, customer_id) TBLPROPERTIES ({TARGET_FILE})")
load("q8626_c_two_keys")
spark.sql(f"OPTIMIZE {FQ}.q8626_c_two_keys")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.q8626_c_two_keys").select("clusteringColumns", "numFiles"))
print("two clustering keys:", layout(f"{FQ}.q8626_c_two_keys"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output: the hybrid DDL fails (Databricks rejects `CLUSTER BY` together with
# MAGIC `PARTITIONED BY`), and the table with two clustering columns lists both in `clusteringColumns` and serves the
# MAGIC probe on `event_date` and `customer_id` while skipping most files. Liquid Clustering is not a single-column point
# MAGIC lookup feature.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option D: "`CLUSTER BY` is just metadata; ordering still comes from `OPTIMIZE ... ZORDER BY`"
# MAGIC
# MAGIC The misconception: D mixes up Liquid Clustering with the older ZORDER technique. `CLUSTER BY` is not implemented
# MAGIC by a manual `ZORDER` after each write; clustering is maintained by Delta's clustering and `OPTIMIZE` machinery,
# MAGIC and ZORDER is a separate, older technique that is not used together with Liquid Clustering.

# COMMAND ----------

# DBTITLE 1,Option D: run ZORDER BY on a clustered table
print("OPTIMIZE ... ZORDER BY on a liquid clustered table:")
optimize_metrics(run(f"OPTIMIZE {FQ}.q8626_c_two_keys ZORDER BY (customer_id)"))
print("plain OPTIMIZE on the same table:")
optimize_metrics(run(f"OPTIMIZE {FQ}.q8626_c_two_keys"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output: `OPTIMIZE ... ZORDER BY` is rejected on a clustered table, so it cannot be what
# MAGIC orders the data; plain `OPTIMIZE` is the command, and it clusters on the `CLUSTER BY` keys by itself (here it
# MAGIC has little or nothing left to do, because the table was already optimized). The second half of D, "changing the
# MAGIC keys means reordering existing files", was disproved in section 2, claim 3: the file set was identical after
# MAGIC `ALTER TABLE ... CLUSTER BY`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rule to memorise
# MAGIC
# MAGIC Liquid Clustering replaces both Hive-style partitioning and ZORDER with `CLUSTER BY` keys (up to four columns),
# MAGIC so there are no per-value directories and no small-partition skew, and a table is either partitioned or
# MAGIC clustered, never both (not B, not C). `OPTIMIZE`, run by you or by predictive optimization, clusters new data
# MAGIC incrementally instead of rewriting the table, and writes are not perfectly sorted on arrival (not A).
# MAGIC `ALTER TABLE ... CLUSTER BY` changes the keys without rewriting existing data, and `ZORDER BY` is rejected on a
# MAGIC clustered table (not D). That is E: the answer that fixes skew, rewrites and fixed keys together.
