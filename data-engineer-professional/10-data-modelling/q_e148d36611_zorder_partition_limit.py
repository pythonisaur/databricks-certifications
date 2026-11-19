# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Z-order stays inside partition boundaries
# MAGIC
# MAGIC Data Engineer Professional, section 10 of the exam guide, "Data Modelling".
# MAGIC Objective: identify the benefits of using liquid clustering over partitioning and ZORDER.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the notebook builds 8,000 click events (4 regions, 20 days, 400
# MAGIC customers, all derived from `range()` and `hash()` so every run is identical) and writes them three ways: a
# MAGIC table partitioned by `region` and Z-ordered, an unpartitioned table Z-ordered, and a liquid clustered table.
# MAGIC Everything lives in `dev_catalog.de_pro_qnotes` under a `qe148_` prefix; nothing else is touched. The tables
# MAGIC set a small `delta.targetFileSize` so that `OPTIMIZE` writes several files even at lab size, which is what
# MAGIC makes per-file layout visible. The Databricks Runtime version matrix in option C is shown as a small stand-in
# MAGIC table next to the version of the compute you are on.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC Which of the following is a key LIMITATION of Z-order (`OPTIMIZE ... ZORDER BY`) compared to liquid clustering?
# MAGIC
# MAGIC * **A** Z-ordering works within each partition of a partitioned table, and files from different partitions are not co-sorted together.
# MAGIC * **B** Z-ordering relies on statistics columns fixed at table creation, and the ZORDER BY columns are not changeable after that point.
# MAGIC * **C** Z-ordering needs Databricks Runtime 15.2 or above, while liquid clustering also runs on older Databricks Runtime versions.
# MAGIC * **D** Z-ordering applies to partitioned tables alone, and unpartitioned tables get no data skipping benefit from ZORDER BY at all.
# MAGIC
# MAGIC The correct option (A) is built and run first; then B, C and D are each run against the same events so you
# MAGIC can see what really happens.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "e148d36611",
    "track": "de_pro",
    "section": "Data Modelling",
    "stem": "Which of the following is a key LIMITATION of Z-order (`OPTIMIZE ... ZORDER BY`) compared to liquid clustering?",
    "correct": "A",
    "options": {
        "A": "Z-ordering works within each partition of a partitioned table, and files from different partitions are not co-sorted together.",
        "B": "Z-ordering relies on statistics columns fixed at table creation, and the ZORDER BY columns are not changeable after that point.",
        "C": "Z-ordering needs Databricks Runtime 15.2 or above, while liquid clustering also runs on older Databricks Runtime versions.",
        "D": "Z-ordering applies to partitioned tables alone, and unpartitioned tables get no data skipping benefit from ZORDER BY at all."
    },
    "objective": "Identify the benefits of using liquid Clustering over Partitioning and ZOrder",
    "source": "testlogichub exams/fixes/qfix/e148d36611.py"
}
# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and helpers (real errors, per-file statistics)
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")

TARGET_FILE = "'delta.targetFileSize' = '24kb'"   # tiny on purpose: several output files at lab size

def run(stmt: str):
    """Run one SQL statement; return the DataFrame, or print the real error (first line) and return None."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {type(e).__name__}: {first[:300]}")
        return None

PROBE_CUSTOMER = 137   # the cross-region query: one customer, every region

def layout(table: str) -> dict:
    """Per-file layout of a Delta table: how many files, how many regions each file mixes, and how many files a
    `customer_id = PROBE_CUSTOMER` filter must open (a file is skipped when the value is outside its min/max)."""
    stats = spark.sql(f"""
        SELECT _metadata.file_path AS f, count(*) AS n_rows, count(DISTINCT region) AS regions,
               min(customer_id) AS c_min, max(customer_id) AS c_max
        FROM {table} GROUP BY _metadata.file_path""").collect()
    return {"files": len(stats),
            "max regions in one file": max((s.regions for s in stats), default=0),
            f"files to read for customer {PROBE_CUSTOMER}": sum(1 for s in stats if s.c_min <= PROBE_CUSTOMER <= s.c_max)}

def optimize_metrics(df) -> None:
    if df is not None:
        df.select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()

RESULTS = {}
print("working schema:", FQ)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'qe148_*'").collect():
    if not t.isTemporary:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: 8,000 click events shared by every option
# MAGIC
# MAGIC | column | values | role in the lesson |
# MAGIC |---|---|---|
# MAGIC | `region` | `NA`, `EU`, `APAC`, `LATAM` | the partition column of the partitioned table |
# MAGIC | `customer_id` | 1 to 400, every customer active in every region | the Z-order / clustering key |
# MAGIC | `event_date` | 20 days from 2026-10-01 | a second key, used to change ZORDER columns in option B |
# MAGIC
# MAGIC The probe query asks for one customer across **all** regions, so it is exactly the cross-partition query that
# MAGIC the question is about. The rows are inserted in four batches, in `event_id` order, so every freshly written
# MAGIC file holds a random mix of customers before any `OPTIMIZE` runs.

# COMMAND ----------

# DBTITLE 1,The events (a temp view, deterministic)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW qe148_events_src AS
    SELECT id AS event_id,
           element_at(array('NA', 'EU', 'APAC', 'LATAM'), cast(abs(hash(id, 'r')) % 4 + 1 AS INT)) AS region,
           cast(abs(hash(id, 'c')) % 400 + 1 AS BIGINT) AS customer_id,
           date_add(DATE'2026-10-01', cast(abs(hash(id, 'd')) % 20 AS INT)) AS event_date,
           cast(round((abs(hash(id, 'amt')) % 20000) / 100.0, 2) AS DOUBLE) AS amount
    FROM range(1, 8001)""")
spark.sql("""SELECT count(*) AS events, count(DISTINCT region) AS regions, count(DISTINCT customer_id) AS customers,
                    count(DISTINCT region) FILTER (WHERE customer_id = 137) AS regions_of_customer_137
             FROM qe148_events_src""").show()

def load(table: str) -> None:
    for batch in range(4):
        spark.sql(f"INSERT INTO {FQ}.{table} SELECT * FROM qe148_events_src WHERE (event_id - 1) DIV 2000 = {batch}")

COLUMNS = "event_id BIGINT, region STRING, customer_id BIGINT, event_date DATE, amount DOUBLE"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option A, run first: Z-order works within each partition, never across partitions
# MAGIC
# MAGIC The Databricks documentation states it directly: "Z-order works in tandem with the OPTIMIZE command. You
# MAGIC cannot combine files across partition boundaries, and so Z-order clustering can only occur within a
# MAGIC partition. For unpartitioned tables, files can be combined across the entire table."
# MAGIC
# MAGIC Three tables with identical rows, identical file-size target and the same key, `customer_id`:
# MAGIC
# MAGIC * `qe148_a_partitioned`: `PARTITIONED BY (region)`, then `OPTIMIZE ... ZORDER BY (customer_id)`
# MAGIC * `qe148_a_unpartitioned`: no partitions, then `OPTIMIZE ... ZORDER BY (customer_id)`
# MAGIC * `qe148_a_liquid`: `CLUSTER BY (customer_id)`, then `OPTIMIZE`

# COMMAND ----------

# DBTITLE 1,Option A: partitioned by region, then Z-ordered by customer_id
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.qe148_a_partitioned ({COLUMNS}) PARTITIONED BY (region) TBLPROPERTIES ({TARGET_FILE})")
load("qe148_a_partitioned")
print("before ZORDER:", layout(f"{FQ}.qe148_a_partitioned"))
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.qe148_a_partitioned ZORDER BY (customer_id)"))
RESULTS["partitioned + ZORDER"] = layout(f"{FQ}.qe148_a_partitioned")
print("after ZORDER :", RESULTS["partitioned + ZORDER"])

# COMMAND ----------

# DBTITLE 1,Option A: where did each file land? One region per file, every time
display(spark.sql(f"""
    SELECT region, count(DISTINCT _metadata.file_path) AS files, min(customer_id) AS first_customer,
           max(customer_id) AS last_customer
    FROM {FQ}.qe148_a_partitioned GROUP BY region ORDER BY region"""))

# COMMAND ----------

# DBTITLE 1,For contrast: the same Z-order on an unpartitioned table, and liquid clustering
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.qe148_a_unpartitioned ({COLUMNS}) TBLPROPERTIES ({TARGET_FILE})")
load("qe148_a_unpartitioned")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.qe148_a_unpartitioned ZORDER BY (customer_id)"))
RESULTS["unpartitioned + ZORDER"] = layout(f"{FQ}.qe148_a_unpartitioned")

spark.sql(f"CREATE OR REPLACE TABLE {FQ}.qe148_a_liquid ({COLUMNS}) CLUSTER BY (customer_id) TBLPROPERTIES ({TARGET_FILE})")
load("qe148_a_liquid")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.qe148_a_liquid"))
RESULTS["liquid CLUSTER BY"] = layout(f"{FQ}.qe148_a_liquid")

cols = ["files", "max regions in one file", f"files to read for customer {PROBE_CUSTOMER}"]
print(f"{'layout':26s}" + "".join(f"{c:>34s}" for c in cols))
for name, r in RESULTS.items():
    print(f"{name:26s}" + "".join(f"{r[c]:>34}" for c in cols))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows, and why A is the limitation:
# MAGIC
# MAGIC * On the partitioned table, `max regions in one file` is **1**: OPTIMIZE ZORDER BY ran independently in each
# MAGIC   `region=` directory, and the per-region table shows every region spanning the whole customer range. Files from
# MAGIC   different partitions are never co-sorted together, so customer 137's rows sit in a separate Z-ordered file
# MAGIC   set in each of the four partitions, and the cross-region probe has to open at least one file per partition.
# MAGIC * The unpartitioned table and the liquid clustered table mix regions inside a file: their files were combined
# MAGIC   across the entire table, sorted by `customer_id` as one range, so the same probe opens fewer files.
# MAGIC * At 8,000 rows the gap is a few files. At production scale, with hundreds of partitions, a query that spans
# MAGIC   partitions pays for one Z-ordered island per partition, which limits Z-order's effectiveness for exactly
# MAGIC   the queries that cross partition boundaries.
# MAGIC * Liquid clustering organizes data across all files in the table regardless of any partition structure (it
# MAGIC   replaces partitioning; a liquid table cannot be partitioned), so it has no partition-boundary limitation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option B: "statistics columns are fixed at table creation, ZORDER BY columns cannot change"
# MAGIC
# MAGIC The misconception: confusing statistics collection with a fixed table property. Z-ordering does need
# MAGIC statistics on the ZORDER BY columns (Z-ordering on a column without collected statistics is ineffective), but
# MAGIC statistics collection is configurable at any time, and the ZORDER BY columns can differ on every OPTIMIZE run.

# COMMAND ----------

# DBTITLE 1,Option B: a table created WITHOUT statistics on customer_id, then Z-ordered on it
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.qe148_b_events ({COLUMNS})
              TBLPROPERTIES ({TARGET_FILE}, 'delta.dataSkippingStatsColumns' = 'event_id,region')""")
load("qe148_b_events")
print("ZORDER BY a column with no statistics:")
optimize_metrics(run(f"OPTIMIZE {FQ}.qe148_b_events ZORDER BY (customer_id)"))

# COMMAND ----------

# DBTITLE 1,Option B: change the statistics columns AFTER creation, then Z-order on two different keys
spark.sql(f"""ALTER TABLE {FQ}.qe148_b_events
              SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'event_id,region,customer_id,event_date')""")
run(f"ANALYZE TABLE {FQ}.qe148_b_events COMPUTE DELTA STATISTICS")   # recompute stats for files already written
print("run 1, ZORDER BY (customer_id):")
optimize_metrics(run(f"OPTIMIZE {FQ}.qe148_b_events ZORDER BY (customer_id)"))
print("run 2, ZORDER BY (event_date) - a different column on the next run:")
optimize_metrics(run(f"OPTIMIZE {FQ}.qe148_b_events ZORDER BY (event_date)"))

# The other knob: index the first N columns instead of naming them
spark.sql(f"ALTER TABLE {FQ}.qe148_b_events UNSET TBLPROPERTIES ('delta.dataSkippingStatsColumns')")
spark.sql(f"ALTER TABLE {FQ}.qe148_b_events SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '5')")
display(spark.sql(f"SHOW TBLPROPERTIES {FQ}.qe148_b_events").where("key LIKE 'delta.dataSkipping%'"))
display(spark.sql(f"DESCRIBE HISTORY {FQ}.qe148_b_events")
        .where("operation IN ('OPTIMIZE', 'SET TBLPROPERTIES', 'UNSET TBLPROPERTIES')")
        .select("version", "operation", "operationParameters").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * The first OPTIMIZE is rejected (Databricks raises an error that Z-ordering on `customer_id` would be
# MAGIC   ineffective because no statistics are collected for it). That part of B's premise is real: Z-order needs
# MAGIC   statistics on its columns.
# MAGIC * But the statistics columns were changed with `ALTER TABLE SET TBLPROPERTIES` long after creation, using
# MAGIC   `delta.dataSkippingStatsColumns`, and then switched to `delta.dataSkippingNumIndexedCols`. Nothing about them
# MAGIC   is fixed at creation time.
# MAGIC * `DESCRIBE HISTORY` shows two OPTIMIZE operations with different `zOrderBy` columns. ZORDER BY is a parameter of
# MAGIC   each OPTIMIZE command, not a table property, so it can differ on every run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: "Z-ordering needs Databricks Runtime 15.2 or above"
# MAGIC
# MAGIC The misconception: swapping the version requirements of the two features. Z-ordering is a long-standing
# MAGIC Delta Lake feature available in all supported Databricks Runtime versions; Databricks Runtime 15.2 is the
# MAGIC version at which liquid clustering became generally available for Delta Lake tables.

# COMMAND ----------

# DBTITLE 1,Option C: the compute you are on, and a stand-in of the version matrix
try:
    print("this compute:", spark.sql("SELECT current_version() AS v").first().v)
except Exception as e:
    print("current_version() is not available here:", type(e).__name__)

FEATURE_MATRIX = {   # stand-in for the documentation's requirements, as stated in the question's explanation
    "OPTIMIZE ... ZORDER BY": {"needs DBR": None, "note": "long-standing Delta Lake feature, every supported DBR"},
    "liquid clustering (CLUSTER BY)": {"needs DBR": 15.2, "note": "generally available for Delta tables from DBR 15.2"},
}

def available(feature: str, dbr: float) -> bool:
    need = FEATURE_MATRIX[feature]["needs DBR"]
    return need is None or dbr >= need

for dbr in (13.3, 14.3, 15.4, 16.4):
    print(f"DBR {dbr}: " + ", ".join(f"{f} -> {'yes' if available(f, dbr) else 'no (GA)'}" for f in FEATURE_MATRIX))
print("\nC claims the reverse: ZORDER gated at 15.2, liquid clustering on older runtimes.")

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong: both commands already ran on this compute in section 2, and the matrix above shows where the
# MAGIC 15.2 boundary really sits. It belongs to liquid clustering's general availability, not to Z-order, which works
# MAGIC on every supported runtime. Version trivia is never what separates Z-order from liquid clustering; the
# MAGIC partition boundary is.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: "Z-ordering applies to partitioned tables alone"
# MAGIC
# MAGIC The misconception: the relationship is backwards. Z-ordering works on both partitioned and unpartitioned
# MAGIC tables, and it is **less** constrained on unpartitioned tables, because files can be combined across the
# MAGIC entire table; on partitioned tables it is confined within partition boundaries.

# COMMAND ----------

# DBTITLE 1,Option D: an unpartitioned table, before and after ZORDER BY (customer_id)
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.qe148_d_events ({COLUMNS}) TBLPROPERTIES ({TARGET_FILE})")
load("qe148_d_events")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.qe148_d_events").select("partitionColumns", "clusteringColumns", "numFiles"))
before_d = layout(f"{FQ}.qe148_d_events")
optimize_metrics(spark.sql(f"OPTIMIZE {FQ}.qe148_d_events ZORDER BY (customer_id)"))
after_d = layout(f"{FQ}.qe148_d_events")
print("before ZORDER:", before_d)
print("after ZORDER :", after_d)

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output: `partitionColumns` is empty and `OPTIMIZE ... ZORDER BY` succeeds. Before it,
# MAGIC the freshly inserted files each hold a random mix of customers, so nearly every file's min/max range contains
# MAGIC customer 137; after it, each file covers a narrow customer range and the probe can skip most of them. That is
# MAGIC a data skipping benefit on a table with no partitions at all, and the comparison in section 2 showed the
# MAGIC unpartitioned table doing at least as well as the partitioned one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The rule to memorise
# MAGIC
# MAGIC Z-order runs inside OPTIMIZE and cannot combine files across partition boundaries, so on a partitioned table
# MAGIC each partition is Z-ordered on its own and queries that span partitions get less out of it; that is A, and it
# MAGIC is the limitation liquid clustering removes by clustering across all files in the table. Z-order needs
# MAGIC statistics on its columns, but `delta.dataSkippingStatsColumns` and `delta.dataSkippingNumIndexedCols` can be
# MAGIC changed at any time and ZORDER BY columns can change on every run (not B). Z-order works on every supported
# MAGIC runtime; 15.2 is liquid clustering's GA version (not C). Z-order works on unpartitioned tables and is least
# MAGIC constrained there (not D).
