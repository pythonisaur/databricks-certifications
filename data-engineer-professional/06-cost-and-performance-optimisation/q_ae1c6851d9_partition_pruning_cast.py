# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Partition pruning that silently fails: a cast on the column, or partitions the metastore never saw
# MAGIC
# MAGIC Data Engineer Professional, section 06 of the exam guide, "Cost & Performance Optimisation".
# MAGIC Objective: use the query profile to analyze the query and identify bottlenecks, such as bad data skipping,
# MAGIC inefficient types of joins, and data shuffling.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the notebook writes a miniature Hive-style partitioned Parquet
# MAGIC dataset (4,000 sales rows over 20 dates, about 50 files per date, so about 1,000 files, all derived from
# MAGIC `range()` and `hash()` so every run is identical) into a managed volume in `dev_catalog.de_pro_qnotes`, and
# MAGIC reads the plans each option talks about. Everything this lesson creates carries a `qae1c_` prefix inside that
# MAGIC schema; nothing else is touched. An external table over cloud storage needs an external location, which
# MAGIC Free Edition does not have, so the metastore side of the question (registered partitions, `MSCK REPAIR TABLE`)
# MAGIC appears as the exact SQL plus a small local stand-in of the partition catalog.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC An analyst query against an external Parquet fact table with Hive-style partitions filters
# MAGIC `WHERE transaction_date = '2025-10-01' AND store_id = 'NYC-042'`. The table is partitioned by
# MAGIC `transaction_date`. The query consistently takes 45+ minutes and the Spark UI shows 800 of 1,000 total data
# MAGIC files being scanned. The expected behavior is that partitioning on `transaction_date` should limit the scan to
# MAGIC approximately 50 files for that date. Which root causes, diagnosable from the Spark UI and query plan, best
# MAGIC explain this behavior — and what is the correct remediation?
# MAGIC
# MAGIC * **A** Diagnosis: pruning on `transaction_date` works and the 800 files are partial skipping, since a date partition does not help `store_id` filters. Remediation: run `OPTIMIZE ... ZORDER BY (transaction_date, store_id)` to cut the file count.
# MAGIC * **B** Diagnosis: Adaptive Query Execution re-planned the shuffle partition count at runtime and pulled extra partition data into the file scan. Remediation: set `spark.sql.adaptive.enabled=false` so the static partition pruning plan is kept.
# MAGIC * **C** Diagnosis: an implicit CAST on the partition column (`cast(transaction_date as string)` in the scan node) blocks partition elimination, or partition directories are missing from the metastore. Remediation: use a DATE literal and run `MSCK REPAIR TABLE`.
# MAGIC * **D** Diagnosis: the plan builds a `CartesianProduct` between the `transaction_date` and `store_id` predicates, which defeats pruning. Remediation: replace the `AND` predicate with a semi-join against a one-row DataFrame holding the target values.
# MAGIC * **E** Diagnosis: the table's transaction log has grown too large for the driver to parse in time, so the planner falls back to a full scan. Remediation: run `OPTIMIZE` on the table to compact the transaction log files and shorten log parsing.
# MAGIC
# MAGIC The correct option (C) is built and run first, both of its root causes in turn; then A, B, D and E each run
# MAGIC against the same 1,000 files so you can see what they really do.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "ae1c6851d9",
    "track": "de_pro",
    "section": "Cost & Performance Optimisation",
    "stem": "An analyst query against an external Parquet fact table with Hive-style partitions filters `WHERE transaction_date = '2025-10-01' AND store_id = 'NYC-042'`. The table is partitioned by `transaction_date`. The query consistently takes 45+ minutes and the Spark UI shows 800 of 1,000 total data files being scanned. The expected behavior is that partitioning on `transaction_date` should limit the scan to approximately 50 files for that date. Which root causes, diagnosable from the Spark UI and query plan, best explain this behavior — and what is the correct remediation?",
    "correct": "C",
    "options": {
        "A": "Diagnosis: pruning on `transaction_date` works and the 800 files are partial skipping, since a date partition does not help `store_id` filters. Remediation: run `OPTIMIZE ... ZORDER BY (transaction_date, store_id)` to cut the file count.",
        "B": "Diagnosis: Adaptive Query Execution re-planned the shuffle partition count at runtime and pulled extra partition data into the file scan. Remediation: set `spark.sql.adaptive.enabled=false` so the static partition pruning plan is kept.",
        "C": "Diagnosis: an implicit CAST on the partition column (`cast(transaction_date as string)` in the scan node) blocks partition elimination, or partition directories are missing from the metastore. Remediation: use a DATE literal and run `MSCK REPAIR TABLE`.",
        "D": "Diagnosis: the plan builds a `CartesianProduct` between the `transaction_date` and `store_id` predicates, which defeats pruning. Remediation: replace the `AND` predicate with a semi-join against a one-row DataFrame holding the target values.",
        "E": "Diagnosis: the table's transaction log has grown too large for the driver to parse in time, so the planner falls back to a full scan. Remediation: run `OPTIMIZE` on the table to compact the transaction log files and shorten log parsing."
    },
    "objective": "Use the query profile to analyze the query and identify bottlenecks, such as bad data skipping, inefficient types of joins, and data shuffling.",
    "source": "testlogichub exams/fixes/qfix/ae1c6851d9.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema, a managed volume for the Parquet files, and plan helpers
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.qae1c_landing")
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/qae1c_landing/sales"   # the "external" Parquet table's root directory

def run(stmt: str):
    """Run one SQL statement; return the DataFrame, or print the real error (first line) and return None."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {type(e).__name__}: {first[:300]}")
        return None

def scan_lines(query: str) -> tuple:
    """EXPLAIN FORMATTED the query and keep the scan node's filter lines (Photon and non-Photon plans alike)."""
    plan = spark.sql(f"EXPLAIN FORMATTED {query}").first()[0]
    keep = [l.strip() for l in plan.splitlines()
            if l.strip().startswith(("PartitionFilters", "PushedFilters", "DataFilters", "Location"))]
    return keep, plan

def show_scan(label: str, query: str) -> str:
    keep, plan = scan_lines(query)
    print(f"--- {label}")
    for l in keep:
        print("   ", l[:160])
    return plan

print("working schema:", FQ, "| Parquet root:", BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a Hive-style partitioned Parquet fact table, about 50 files per date
# MAGIC
# MAGIC | what | here | in the question |
# MAGIC |---|---|---|
# MAGIC | partition column | `transaction_date`, 20 dates from 2025-09-22 to 2025-10-11 | `transaction_date` |
# MAGIC | files | about 50 per date, about 1,000 in all | 1,000, about 50 for one date |
# MAGIC | other filter | `store_id`, 60 stores spread over every file | `store_id = 'NYC-042'` |
# MAGIC
# MAGIC The directory layout is the Hive convention, `.../sales/transaction_date=2025-10-01/part-....parquet`: the
# MAGIC partition value lives in the directory name, not inside the files. That is the only reason pruning is possible:
# MAGIC the planner reads directory names, compares them with the filter, and never opens the other dates' files.

# COMMAND ----------

# DBTITLE 1,Write the partitioned Parquet files (deterministic, re-runnable with overwrite)
from pyspark.sql import functions as F

sales = (spark.range(0, 4000)
         .withColumn("transaction_date", F.date_add(F.lit("2025-09-22").cast("date"), (F.col("id") % 20).cast("int")))
         .withColumn("file_bucket", ((F.col("id") / 20).cast("long") % 50))
         .withColumn("store_id", F.format_string("NYC-%03d", (F.abs(F.hash("id", F.lit("store"))) % 60 + 1).cast("int")))
         .withColumn("amount", F.round((F.abs(F.hash("id", F.lit("amt"))) % 50000) / 100.0, 2))
         .withColumnRenamed("id", "sale_id"))

# 50 writer tasks, each holding rows of every date: each task writes one file per date -> about 50 files per date.
(sales.repartitionByRange(50, "file_bucket").drop("file_bucket")
      .write.mode("overwrite").partitionBy("transaction_date").parquet(BASE))

def file_inventory(root: str) -> dict:
    """Partition directory -> number of data files, read straight from storage."""
    inv = {}
    for d in dbutils.fs.ls(root):
        if d.name.startswith("transaction_date="):
            inv[d.name.rstrip("/").split("=", 1)[1]] = sum(1 for f in dbutils.fs.ls(d.path) if f.name.endswith(".parquet"))
    return dict(sorted(inv.items()))

INVENTORY = file_inventory(BASE)
TOTAL_FILES = sum(INVENTORY.values())
print(f"{len(INVENTORY)} partition directories, {TOTAL_FILES} data files; 2025-10-01 holds {INVENTORY.get('2025-10-01')} of them")

# COMMAND ----------

# DBTITLE 1,Read it the way the external table does: partition discovery from directory names
spark.read.parquet(BASE).createOrReplaceTempView("qae1c_sales")
spark.table("qae1c_sales").printSchema()
spark.sql("SELECT transaction_date, count(*) AS sale_rows, count(DISTINCT _metadata.file_path) AS files "
          "FROM qae1c_sales GROUP BY transaction_date ORDER BY transaction_date").show(25)

# COMMAND ----------

# MAGIC %md
# MAGIC `printSchema` shows `transaction_date: date`: the type was **inferred from the directory names**, it is not
# MAGIC stored in any file. Keep that in mind for section 2: the filter compares a DATE partition column with whatever
# MAGIC literal the analyst wrote.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option C, run first: root cause 1, a CAST on the partition column
# MAGIC
# MAGIC When a filter compares two different types, the analyzer inserts a cast on one side (type coercion). Which side
# MAGIC gets the cast decides everything:
# MAGIC
# MAGIC * **Cast on the literal** (`transaction_date = CAST('2025-10-01' AS DATE)`): constant folding turns the literal
# MAGIC   into a DATE value, the predicate still compares the raw partition value, and pruning works. This is why the
# MAGIC   failure is subtle and easy to miss: most of the time the string literal in the question is harmless.
# MAGIC * **Cast on the column** (`cast(transaction_date as string) = '2025-10-01'`): the predicate no longer
# MAGIC   references the raw partition value, so the optimizer cannot use it to eliminate partitions. The scan node
# MAGIC   shows `cast(transaction_date ...)`, and the filter is applied after reading the files.
# MAGIC
# MAGIC The three queries below differ only in how the date is written. Read the `PartitionFilters` line of each scan.

# COMMAND ----------

# DBTITLE 1,Three spellings of the same date filter, and the scan node of each
Q_DATE_LITERAL = "SELECT * FROM qae1c_sales WHERE transaction_date = DATE'2025-10-01' AND store_id = 'NYC-042'"
Q_STRING_LITERAL = "SELECT * FROM qae1c_sales WHERE transaction_date = '2025-10-01' AND store_id = 'NYC-042'"
Q_CAST_COLUMN = "SELECT * FROM qae1c_sales WHERE CAST(transaction_date AS STRING) = '2025-10-01' AND store_id = 'NYC-042'"

show_scan("DATE literal (the remediation)", Q_DATE_LITERAL)
show_scan("string literal, coerced on the literal side", Q_STRING_LITERAL)
cast_plan = show_scan("cast on the partition column", Q_CAST_COLUMN)
print("\nthe column-side cast appears in the plan:", "cast(transaction_date" in cast_plan.lower())
for q in (Q_DATE_LITERAL, Q_STRING_LITERAL, Q_CAST_COLUMN):
    print("rows:", spark.sql(q).count())

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * The DATE literal and the string literal both produce `PartitionFilters: [isnotnull(transaction_date),
# MAGIC   (transaction_date = 2025-10-01)]`: the raw partition value, compared with a constant. The planner lists only
# MAGIC   the `transaction_date=2025-10-01` directory, about 50 files.
# MAGIC * The third scan carries `cast(transaction_date as string)`. All three return the same rows; the difference is
# MAGIC   how many files have to be opened to get them. A predicate on the raw column value is what partition
# MAGIC   elimination is built on: the metastore's partition pushdown, for example, accepts only `column op literal`
# MAGIC   comparisons. When the cast wraps the column, that pushdown gets nothing, every partition is listed, and the
# MAGIC   filter is applied after the files are read: the 800-of-1,000 picture in the question. On this path-based read,
# MAGIC   Spark's in-memory file index may still evaluate the cast expression against the directory values, so compare
# MAGIC   the line you got with the stand-in below; the habit the exam wants is the same either way.
# MAGIC * **Remediation:** make the literal's type match the column's, `DATE'2025-10-01'` or a properly typed
# MAGIC   parameter, and never wrap the partition column in a function or cast.
# MAGIC
# MAGIC In the Databricks query profile (or the Spark UI SQL tab, scan node) the same diagnosis is two numbers: files
# MAGIC read versus files pruned. About 50 read means pruning worked; hundreds read means it did not.

# COMMAND ----------

# DBTITLE 1,Stand-in: metastore partition pushdown, which only understands `column op literal`
import re

def metastore_partitions_for(predicate: str, registered: dict) -> list:
    """Model of metastore-side partition pruning: a predicate of the form `transaction_date = <literal>` is matched
    against the stored partition values; anything else (a function or cast on the column) cannot be pushed, so every
    registered partition is returned and the filter runs after the files are read."""
    m = re.fullmatch(r"\s*transaction_date\s*=\s*(?:DATE)?'(\d{4}-\d{2}-\d{2})'\s*", predicate, re.I)
    if not m:
        return list(registered)
    return [p for p in registered if p == m.group(1)]

REGISTERED = dict(INVENTORY)   # the partitions the metastore knows about, with their file counts
for pred in ["transaction_date = DATE'2025-10-01'", "CAST(transaction_date AS STRING) = '2025-10-01'"]:
    parts = metastore_partitions_for(pred, REGISTERED)
    files = sum(REGISTERED[p] for p in parts)
    print(f"{pred:50s} -> {len(parts):2d} partitions, {files:4d} of {TOTAL_FILES} files scanned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option C, root cause 2: partition directories the metastore does not know about
# MAGIC
# MAGIC An external table with Hive-style partitioning keeps its list of partitions **in the metastore**, not in the
# MAGIC files. A job (or a person) that writes a new `transaction_date=...` directory straight into storage leaves the
# MAGIC planner unaware of that partition until it is registered. `MSCK REPAIR TABLE` (or `ALTER TABLE ... RECOVER
# MAGIC PARTITIONS`) walks the directories and registers what is missing. The next cell shows the exact SQL; it is not
# MAGIC executed because it needs an external location.

# COMMAND ----------

# DBTITLE 1,The real SQL for an external Hive-partitioned Parquet table (annotated, not executed on Free Edition)
EXTERNAL_TABLE_SQL = """
-- 1. The external table: Parquet files under a cloud path, partitions tracked in the metastore
CREATE TABLE sales_ext (sale_id BIGINT, store_id STRING, amount DOUBLE, transaction_date DATE)
USING PARQUET
PARTITIONED BY (transaction_date)
LOCATION 's3://<bucket>/sales';            -- needs an external location / storage credential

-- 2. A new directory written straight to storage: s3://<bucket>/sales/transaction_date=2025-10-12/part-....parquet
SHOW PARTITIONS sales_ext;                 -- the new date is not listed yet

-- 3. Register every directory the metastore is missing (either statement)
MSCK REPAIR TABLE sales_ext;
ALTER TABLE sales_ext RECOVER PARTITIONS;

-- 4. Query with a typed literal so the predicate stays on the raw partition value
SELECT * FROM sales_ext WHERE transaction_date = DATE'2025-10-01' AND store_id = 'NYC-042';
"""
print(EXTERNAL_TABLE_SQL)

# COMMAND ----------

# DBTITLE 1,A directory written straight to storage, then the stand-in catalog before and after MSCK REPAIR
NEW_DATE = "2025-10-12"
(sales.where("transaction_date = DATE'2025-10-11'").drop("transaction_date", "file_bucket").coalesce(1)
      .write.mode("overwrite").parquet(f"{BASE}/transaction_date={NEW_DATE}"))

ON_DISK = file_inventory(BASE)
print("directories on storage :", len(ON_DISK), "| newest:", max(ON_DISK))
print("registered in metastore:", len(REGISTERED), "| newest:", max(REGISTERED))
print("missing from metastore :", sorted(set(ON_DISK) - set(REGISTERED)))

def msck_repair(registered: dict, on_disk: dict) -> dict:
    """Stand-in for MSCK REPAIR TABLE: add every partition directory found on storage that is not registered."""
    added = {p: n for p, n in on_disk.items() if p not in registered}
    print(f"MSCK REPAIR TABLE -> partitions added: {len(added)} {sorted(added)}")
    return {**registered, **added}

REGISTERED = msck_repair(REGISTERED, ON_DISK)
print("registered after repair:", len(REGISTERED), "| newest:", max(REGISTERED))

# COMMAND ----------

# DBTITLE 1,A path-based read discovers directories itself; a Delta table keeps partitions in its own log
spark.read.parquet(BASE).createOrReplaceTempView("qae1c_sales")
print("dates visible to a path-based read:", spark.table("qae1c_sales").select("transaction_date").distinct().count())

spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.qae1c_sales_delta PARTITIONED BY (transaction_date)
              AS SELECT * FROM qae1c_sales""")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.qae1c_sales_delta").select("format", "partitionColumns", "numFiles"))
run(f"MSCK REPAIR TABLE {FQ}.qae1c_sales_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * The new `transaction_date=2025-10-12` directory is on storage but not in the registered partition list until
# MAGIC   the repair runs. With partition metadata out of step with the directories, the planner cannot rely on it to
# MAGIC   eliminate partitions: registering them is the second half of the remediation in C.
# MAGIC * A path-based read (`spark.read.parquet`) discovers directories at read time, which is why it sees 21 dates.
# MAGIC * Delta tables keep their own partition metadata in the transaction log and do not need `MSCK REPAIR`; the last
# MAGIC   line shows the command has nothing to do on one (it is rejected or is a no-op, depending on the runtime).
# MAGIC   External Hive-partitioned tables do need it.

# COMMAND ----------

# DBTITLE 1,What partitioning does NOT do: store_id skipping relies only on per-file min/max statistics
stats = spark.sql("""
    SELECT _metadata.file_path AS f, min(store_id) AS s_min, max(store_id) AS s_max
    FROM qae1c_sales WHERE transaction_date = DATE'2025-10-01' GROUP BY _metadata.file_path""").collect()
could_hold = sum(1 for s in stats if s.s_min <= "NYC-042" <= s.s_max)
print(f"files in the 2025-10-01 partition: {len(stats)}; files whose [min, max] store_id range could hold NYC-042: {could_hold}")

# COMMAND ----------

# MAGIC %md
# MAGIC Inside the right partition, `store_id = 'NYC-042'` can only skip a file when its min/max range excludes the
# MAGIC value. Without a layout optimization the stores are spread across every file, so almost none are skipped. That
# MAGIC costs a few dozen files at most, never 800: correct pruning plus no layout work still lands near 50 files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option A: "pruning works, the 800 files are partial skipping", then `OPTIMIZE ... ZORDER BY`
# MAGIC
# MAGIC The misconception: assuming pruning is working. If partition pruning were functioning, the scan would be about
# MAGIC 50 files; 800 files confirms pruning is **not** happening. ZORDER would help `store_id` filtering but cannot fix
# MAGIC the broken partition pruning.

# COMMAND ----------

# DBTITLE 1,Option A: the arithmetic, then ZORDER on the Parquet table
pruned_ceiling = INVENTORY["2025-10-01"]
print(f"most files a working date prune can read: {pruned_ceiling}; files the Spark UI showed: 800 of 1,000")
print("800 > ceiling -> pruning is broken:", 800 > pruned_ceiling)
print()
run(f"OPTIMIZE parquet.`{BASE}` ZORDER BY (store_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output:
# MAGIC
# MAGIC * No amount of `store_id` skipping inside one date can read more files than that date holds (about 50). 800 means
# MAGIC   the date filter did not eliminate partitions.
# MAGIC * `OPTIMIZE ... ZORDER BY` is a Delta command; on a Parquet path it fails. Even on a Delta copy, Z-ordering
# MAGIC   would tighten the `store_id` min/max ranges (fewer files within the date) and would do nothing for a cast on the
# MAGIC   partition column or partitions missing from the metastore. A Z-order on the partition column itself is also
# MAGIC   pointless: every file already holds a single date.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option B: blame Adaptive Query Execution, disable it
# MAGIC
# MAGIC The misconception: that AQE can change which partitions a scan reads. AQE tunes shuffle partition counts and
# MAGIC join strategies at runtime, but partition elimination is a static optimization done during query planning,
# MAGIC before execution, and it is independent of AQE.

# COMMAND ----------

# DBTITLE 1,Option B: the scan node with AQE on, and with AQE off where the compute allows it
before = scan_lines(Q_DATE_LITERAL)[0]
print("AQE on :", [l for l in before if l.startswith("PartitionFilters")])
try:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    after = scan_lines(Q_DATE_LITERAL)[0]
    print("AQE off:", [l for l in after if l.startswith("PartitionFilters")])
    print("partition filters identical:", [l for l in before if l.startswith("PartitionFilters")] ==
                                          [l for l in after if l.startswith("PartitionFilters")])
    spark.conf.unset("spark.sql.adaptive.enabled")
except Exception as e:
    first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
    print(f"AQE cannot be switched off here ({type(e).__name__}: {first[:160]}); serverless keeps it on")

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output: the `PartitionFilters` line is decided when the plan is built, so it is the same
# MAGIC with AQE on or off (and serverless compute does not even let you switch AQE off). AQE only re-plans the stages
# MAGIC after a shuffle: coalescing shuffle partitions, switching a join to broadcast, splitting skewed partitions. It
# MAGIC never adds files to a scan, so disabling it changes nothing here and loses its real benefits.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option D: a `CartesianProduct` between two predicates
# MAGIC
# MAGIC The misconception: misreading the query plan. `CartesianProduct` is a **join** operator between two unrelated
# MAGIC inputs. A simple `AND` between two filter conditions on the SAME table never produces one.

# COMMAND ----------

# DBTITLE 1,Option D: search the analyst query's plan for joins, then build a real Cartesian product for contrast
analyst_plan = scan_lines(Q_STRING_LITERAL)[1]
print("CartesianProduct in the analyst query plan:", "cartesian" in analyst_plan.lower())
print("any join in the analyst query plan       :", "join" in analyst_plan.lower())

stores = spark.range(3).selectExpr("format_string('NYC-%03d', cast(id + 40 AS INT)) AS store_id")
dates = spark.range(2).selectExpr("date_add(DATE'2025-10-01', cast(id AS INT)) AS transaction_date")
stores.createOrReplaceTempView("qae1c_stores")
dates.createOrReplaceTempView("qae1c_dates")
cross_plan = spark.sql("EXPLAIN SELECT * FROM qae1c_stores CROSS JOIN qae1c_dates").first()[0]
print("\nan explicit CROSS JOIN of two inputs plans as:",
      [l.strip(" +-:") for l in cross_plan.splitlines() if "Cartesian" in l or "NestedLoop" in l][:1])

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output: the analyst query is one scan with a filter, with no join anywhere in its plan.
# MAGIC A Cartesian product (shown as `CartesianProduct` or a broadcast nested loop join) appears only when two inputs
# MAGIC are joined with no join key, as in the explicit `CROSS JOIN`. Rewriting the `AND` as a semi-join against a
# MAGIC one-row DataFrame would add a join that was not there and would still leave a column-side cast or unregistered
# MAGIC partitions untouched.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Option E: an oversized transaction log, fixed with `OPTIMIZE`
# MAGIC
# MAGIC The misconception: treating a Parquet table as a Delta table, and treating `OPTIMIZE` as log maintenance.

# COMMAND ----------

# DBTITLE 1,Option E: is there a transaction log at all? And what does OPTIMIZE compact on a Delta table?
print("entries at the Parquet root:", sorted({f.name.split("=")[0] for f in dbutils.fs.ls(BASE)}))
print("has a _delta_log directory:", any(f.name.startswith("_delta_log") for f in dbutils.fs.ls(BASE)))
run(f"DESCRIBE HISTORY parquet.`{BASE}`")

optimized = spark.sql(f"OPTIMIZE {FQ}.qae1c_sales_delta")
optimized.select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
display(spark.sql(f"DESCRIBE DETAIL {FQ}.qae1c_sales_delta").select("numFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong, from the output:
# MAGIC
# MAGIC * This is a Parquet table: there is no `_delta_log`, and `DESCRIBE HISTORY` fails, so there is no transaction log
# MAGIC   to grow.
# MAGIC * In any case, Delta's `OPTIMIZE` compacts **data files** (the metrics count data files removed and added), not
# MAGIC   the transaction log. Log checkpointing is a separate automatic process.
# MAGIC * An oversized transaction log would slow query planning, not cause a fallback to a full table scan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC If a partition filter reads far more files than one partition holds, pruning is broken; look at the scan node's
# MAGIC `PartitionFilters` and the files-read count in the query profile. The two root causes are a cast on the
# MAGIC partition **column** (`cast(transaction_date ...)` in the scan node; fix it with a literal of the column's type,
# MAGIC `DATE'2025-10-01'`) and, for external Hive-partitioned tables, directories the metastore never registered (fix it
# MAGIC with `MSCK REPAIR TABLE` or `ALTER TABLE ... RECOVER PARTITIONS`; Delta tables never need it). That is C. ZORDER
# MAGIC (A) only helps within a partition, AQE (B) never changes partition elimination, an `AND` on one table is never a
# MAGIC Cartesian product (D), and `OPTIMIZE` compacts data files, not a log that a Parquet table does not have (E).
