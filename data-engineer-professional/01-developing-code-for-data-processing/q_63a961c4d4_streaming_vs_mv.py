# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Streaming Tables vs Materialized Views: a checkpoint for Silver, a recompute for Gold
# MAGIC
# MAGIC Data Engineer Professional, section 01 of the exam guide, "Developing Code for Data Processing using Python and
# MAGIC SQL". Objective: explain the advantages and disadvantages of streaming tables compared to materialized views.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute is
# MAGIC enough and Free Edition works. A Lakeflow Spark Declarative Pipeline only runs inside a pipeline update, not in an
# MAGIC interactive notebook, so the pipeline source appears as the exact annotated SQL and Python, and the notebook runs
# MAGIC a faithful stand-in of each construct with plain Spark: an Auto Loader stream with a checkpoint for the Streaming
# MAGIC Table, and a `CREATE OR REPLACE TABLE ... AS SELECT` with no checkpoint for the Materialized View. The practice
# MAGIC data (70 orders over two days, 6 stores, all derived from `range()` and `hash()` so every run is identical) lives in
# MAGIC `dev_catalog.de_pro_qnotes`; everything this lesson creates carries a `q63a9_` prefix and nothing else is touched.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineering team is designing the Silver and Gold layers of their Lakeflow Spark Declarative Pipeline. The
# MAGIC Silver layer ingests from Auto Loader and the Gold layer computes daily revenue aggregations joined with a slowly
# MAGIC changing dimension table `dim_stores`. A product manager asks the team to explain the key operational differences
# MAGIC between Streaming Tables and Materialized Views, and to justify which construct should be used for each layer.
# MAGIC Which explanation is most technically accurate and complete?
# MAGIC
# MAGIC * **A** Streaming Tables process data in micro-batches and Materialized Views process it continuously, and both keep running aggregation state between pipeline updates. Use either one for the Silver Auto Loader ingestion and Streaming Tables for the Gold revenue aggregation joined with `dim_stores`.
# MAGIC * **B** Streaming Tables keep a checkpoint and process just the records that arrived after the previous update; Materialized Views keep no checkpoint and re-evaluate their query against current upstream state. Use Streaming Tables for the Silver Auto Loader ingestion and Materialized Views for the Gold revenue aggregation.
# MAGIC * **C** Streaming Tables support stateful operations such as `dropDuplicates` and windowed aggregations; Materialized Views accept stateless SELECT queries without aggregates or window functions. Use Materialized Views for the Silver Auto Loader ingestion and Streaming Tables for the Gold revenue aggregation.
# MAGIC * **D** Streaming Tables persist their results as Delta tables in cloud storage; Materialized Views hold their results in an in-memory metastore cache that is lost on a cluster restart. Use Streaming Tables for both the Silver Auto Loader ingestion and the Gold revenue aggregation joined with `dim_stores`.
# MAGIC * **E** Streaming Tables accept append sources such as Auto Loader and Kafka and have no way to process CDC updates or deletes; Materialized Views apply them. Use Materialized Views for the Silver ingestion when source records get corrected or deleted, and Streaming Tables for the Gold aggregation.
# MAGIC
# MAGIC The correct option (B) is built and run first, over three pipeline updates; then A, C, D and E each run against
# MAGIC the same tables so you can see what they really get wrong.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "63a961c4d4",
    "track": "de_pro",
    "section": "Developing Code for Data Processing using Python and SQL",
    "stem": "A data engineering team is designing the Silver and Gold layers of their Lakeflow Spark Declarative Pipeline. The Silver layer ingests from Auto Loader and the Gold layer computes daily revenue aggregations joined with a slowly changing dimension table `dim_stores`. A product manager asks the team to explain the key operational differences between Streaming Tables and Materialized Views, and to justify which construct should be used for each layer. Which explanation is most technically accurate and complete?",
    "correct": "B",
    "options": {
        "A": "Streaming Tables process data in micro-batches and Materialized Views process it continuously, and both keep running aggregation state between pipeline updates. Use either one for the Silver Auto Loader ingestion and Streaming Tables for the Gold revenue aggregation joined with `dim_stores`.",
        "B": "Streaming Tables keep a checkpoint and process just the records that arrived after the previous update; Materialized Views keep no checkpoint and re-evaluate their query against current upstream state. Use Streaming Tables for the Silver Auto Loader ingestion and Materialized Views for the Gold revenue aggregation.",
        "C": "Streaming Tables support stateful operations such as `dropDuplicates` and windowed aggregations; Materialized Views accept stateless SELECT queries without aggregates or window functions. Use Materialized Views for the Silver Auto Loader ingestion and Streaming Tables for the Gold revenue aggregation.",
        "D": "Streaming Tables persist their results as Delta tables in cloud storage; Materialized Views hold their results in an in-memory metastore cache that is lost on a cluster restart. Use Streaming Tables for both the Silver Auto Loader ingestion and the Gold revenue aggregation joined with `dim_stores`.",
        "E": "Streaming Tables accept append sources such as Auto Loader and Kafka and have no way to process CDC updates or deletes; Materialized Views apply them. Use Materialized Views for the Silver ingestion when source records get corrected or deleted, and Streaming Tables for the Gold aggregation."
    },
    "objective": "Explain the advantages and disadvantages of streaming tables compared to materialized views.",
    "source": "testlogichub exams/fixes/qfix/63a961c4d4.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema, a fresh landing volume, and a small error-printing helper
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")

# Re-runnable from scratch: the lesson's own volume (landing files + checkpoints) and its own tables are recreated.
spark.sql(f"DROP VOLUME IF EXISTS {FQ}.q63a9_landing")
spark.sql(f"CREATE VOLUME {FQ}.q63a9_landing")
for t in ["silver_orders", "gold_daily_revenue", "gold_streaming_rows", "dim_stores",
          "store_changes", "silver_stores", "dedup_check"]:
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.q63a9_{t}")

VOL = f"/Volumes/{CATALOG}/{SCHEMA}/q63a9_landing"
ORDERS_DIR = f"{VOL}/orders"                 # where Auto Loader looks for new JSON files
CKPT = f"{VOL}/_checkpoints"                 # streaming checkpoints live next to the data

SILVER = f"{FQ}.q63a9_silver_orders"         # stand-in for the Streaming Table
GOLD_MV = f"{FQ}.q63a9_gold_daily_revenue"   # stand-in for the Materialized View
GOLD_ST = f"{FQ}.q63a9_gold_streaming_rows"  # what option A/D's "Streaming Table for Gold" would hold
DIM = f"{FQ}.q63a9_dim_stores"

def run(label: str, fn):
    """Run fn(); on failure print the real error class and first line instead of stopping the notebook."""
    try:
        out = fn()
        print(f"OK      {label}")
        return out
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {label}: {type(e).__name__}: {first[:240]}")
        return None

print("working schema:", FQ, "| landing volume:", VOL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a store dimension and order files that arrive in batches
# MAGIC
# MAGIC | what | here | in the question |
# MAGIC |---|---|---|
# MAGIC | raw source | JSON order files landing in a volume, read by Auto Loader | Auto Loader into Silver |
# MAGIC | Silver | `q63a9_silver_orders`, one row per order | Streaming Table |
# MAGIC | dimension | `q63a9_dim_stores`, 6 stores in 3 regions; store `S02` moves from North to East | `dim_stores`, slowly changing |
# MAGIC | Gold | revenue per day and region, Silver joined with the dimension | Materialized View |
# MAGIC
# MAGIC Orders arrive in two file batches: 40 orders for 2025-10-01, then 30 orders for 2025-10-02.

# COMMAND ----------

# DBTITLE 1,The dimension table and a function that lands one batch of order files
from pyspark.sql import functions as F

spark.sql(f"""CREATE TABLE {DIM} (store_id STRING, store_name STRING, region STRING)""")
spark.sql(f"""INSERT INTO {DIM} VALUES
  ('S01', 'Harbor',    'North'), ('S02', 'Mill Road', 'North'),
  ('S03', 'Riverside', 'South'), ('S04', 'Old Town',  'South'),
  ('S05', 'Airport',   'East'),  ('S06', 'Station',   'East')""")

ORDER_SCHEMA = "order_id BIGINT, store_id STRING, order_date DATE, amount DOUBLE"

def land_batch(first_id: int, n: int, day: str) -> None:
    """Write n deterministic orders for one day as a new JSON file in the landing folder (append = new files only)."""
    (spark.range(first_id, first_id + n)
          .select(F.col("id").alias("order_id"),
                  F.concat(F.lit("S0"), (F.abs(F.hash("id", F.lit("store"))) % 6 + 1).cast("string")).alias("store_id"),
                  F.lit(day).cast("date").alias("order_date"),
                  F.round((F.abs(F.hash("id", F.lit("amt"))) % 20000) / 100.0, 2).alias("amount"))
          .coalesce(1).write.mode("append").json(ORDERS_DIR))

def landed_files() -> int:
    return sum(1 for f in dbutils.fs.ls(ORDERS_DIR) if f.name.endswith(".json"))

display(spark.table(DIM))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option B, run first: a Streaming Table for Silver, a Materialized View for Gold
# MAGIC
# MAGIC Option B names the one operational difference that decides everything:
# MAGIC
# MAGIC * A **Streaming Table** keeps a persistent **checkpoint** and processes only the new data appended since the last
# MAGIC   update. That is exactly what Auto Loader ingestion needs: each file is read once, never again.
# MAGIC * A **Materialized View** keeps **no checkpoint** and cannot skip already-processed data. Every pipeline update
# MAGIC   re-evaluates its defining query against the **current** state of all upstream tables. That is exactly what an
# MAGIC   aggregation joined with a dimension needs: the result must reflect all history combined with today's dimension.
# MAGIC
# MAGIC This is the pipeline source the team would deploy. It is not executed here: a pipeline runs these definitions
# MAGIC inside a pipeline update, not in a notebook session.

# COMMAND ----------

# DBTITLE 1,The real pipeline source, SQL and Python (annotated, not executed in this notebook)
PIPELINE_SQL = """
-- Silver: Streaming Table. STREAM read_files = Auto Loader; the pipeline keeps the checkpoint for you.
CREATE OR REFRESH STREAMING TABLE silver_orders
AS SELECT * FROM STREAM read_files('/Volumes/<catalog>/<schema>/landing/orders', format => 'json');

-- Gold: Materialized View. No STREAM keyword; recomputed against current upstream state on every update.
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT o.order_date, s.region, count(*) AS orders, round(sum(o.amount), 2) AS revenue,
          rank() OVER (PARTITION BY o.order_date ORDER BY sum(o.amount) DESC) AS region_rank
   FROM silver_orders o JOIN dim_stores s USING (store_id)
   GROUP BY o.order_date, s.region;
"""

PIPELINE_PY = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="silver_orders")                     # returns a streaming DataFrame -> Streaming Table
def silver_orders():
    return (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .load("/Volumes/<catalog>/<schema>/landing/orders"))

@dp.materialized_view(name="gold_daily_revenue")    # returns a batch DataFrame -> Materialized View
def gold_daily_revenue():
    return (spark.read.table("silver_orders").join(spark.read.table("dim_stores"), "store_id")
                 .groupBy("order_date", "region")
                 .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue")))
'''
print(PIPELINE_SQL)
print(PIPELINE_PY)

# COMMAND ----------

# MAGIC %md
# MAGIC The stand-ins below behave the way the pipeline's two constructs do:
# MAGIC
# MAGIC * `silver_update()` is an Auto Loader stream with a checkpoint and `trigger(availableNow=True)`: one pipeline
# MAGIC   update of the Streaming Table. It reports how many rows that update actually processed.
# MAGIC * `gold_mv_refresh()` is a full `CREATE OR REPLACE TABLE ... AS SELECT` over Silver joined with the dimension:
# MAGIC   one refresh of the Materialized View, with no checkpoint anywhere.
# MAGIC * `gold_streaming_update()` is what you would get if Gold were a Streaming Table instead: a stream from Silver with
# MAGIC   a stream-static join to the dimension, appending enriched rows. It is kept for section 3 (option A).

# COMMAND ----------

# DBTITLE 1,Stand-ins: one Streaming Table update, one Materialized View refresh, and a streaming Gold for contrast
def table_rows(name: str) -> int:
    return spark.table(name).count() if spark.catalog.tableExists(name) else 0

def silver_update() -> int:
    """One Streaming Table update: Auto Loader reads only files the checkpoint has not seen yet."""
    before = table_rows(SILVER)
    q = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .schema(ORDER_SCHEMA)
              .load(ORDERS_DIR)
              .writeStream
              .option("checkpointLocation", f"{CKPT}/silver_orders")
              .trigger(availableNow=True)
              .toTable(SILVER))
    q.awaitTermination()
    return table_rows(SILVER) - before

GOLD_SQL = f"""
    SELECT o.order_date, s.region, count(*) AS orders, round(sum(o.amount), 2) AS revenue,
           rank() OVER (PARTITION BY o.order_date ORDER BY sum(o.amount) DESC) AS region_rank
    FROM {SILVER} o JOIN {DIM} s USING (store_id)
    GROUP BY o.order_date, s.region"""

def gold_mv_refresh() -> None:
    """One Materialized View refresh: the whole query, against the current Silver and the current dimension."""
    spark.sql(f"CREATE OR REPLACE TABLE {GOLD_MV} AS {GOLD_SQL}")

def gold_streaming_update() -> int:
    """Gold as a Streaming Table: each Silver row is joined to the dimension once, when it is processed, and kept."""
    before = table_rows(GOLD_ST)
    q = (spark.readStream.table(SILVER)
              .join(spark.read.table(DIM), "store_id")          # stream-static join: dimension as of processing time
              .writeStream
              .option("checkpointLocation", f"{CKPT}/gold_streaming_rows")
              .trigger(availableNow=True)
              .toTable(GOLD_ST))
    q.awaitTermination()
    return table_rows(GOLD_ST) - before

def pipeline_update(label: str) -> None:
    silver_new = silver_update()
    gold_mv_refresh()
    gold_st_new = gold_streaming_update()
    print(f"{label}: files landed {landed_files()} | Silver processed {silver_new} new rows "
          f"(table now {table_rows(SILVER)}) | Gold MV recomputed from {table_rows(SILVER)} Silver rows | "
          f"streaming Gold appended {gold_st_new}")

# COMMAND ----------

# DBTITLE 1,Update 1: the first file batch lands (40 orders for 2025-10-01)
land_batch(first_id=1, n=40, day="2025-10-01")
pipeline_update("update 1")
print("\ncheckpoint folders the Streaming Table keeps:", sorted(f.name for f in dbutils.fs.ls(f"{CKPT}/silver_orders")))
display(spark.table(GOLD_MV).orderBy("order_date", "region_rank"))

# COMMAND ----------

# DBTITLE 1,Update 2: nothing new landed. The Streaming Table skips everything; the Materialized View recomputes anyway
pipeline_update("update 2")

# COMMAND ----------

# DBTITLE 1,Update 3: store S02 moves North -> East, then the second batch lands (30 orders for 2025-10-02)
spark.sql(f"UPDATE {DIM} SET region = 'East' WHERE store_id = 'S02'")
land_batch(first_id=1001, n=30, day="2025-10-02")
pipeline_update("update 3")

s02 = spark.sql(f"SELECT order_date, count(*) AS s02_orders FROM {SILVER} WHERE store_id = 'S02' GROUP BY order_date")
display(s02.orderBy("order_date"))
display(spark.table(GOLD_MV).orderBy("order_date", "region_rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * **Silver, the Streaming Table.** Update 1 processed 40 rows, update 2 processed 0, update 3 processed exactly
# MAGIC   the 30 new rows. The checkpoint folder (`offsets`, `commits`, `sources`, `metadata`) is how it knows which files
# MAGIC   it has already read. That is why a Streaming Table is the right construct for Auto Loader ingestion at Silver:
# MAGIC   every file is processed once, and the cost of an update grows with the new data, not with the history.
# MAGIC * **Gold, the Materialized View.** It has no checkpoint, so it cannot skip already-processed data: update 2 rebuilt
# MAGIC   the result even though nothing changed upstream. In exchange, update 3 shows the payoff: the dimension change
# MAGIC   was applied to **all** history, so store S02's 2025-10-01 orders now count toward East, exactly as the current
# MAGIC   `dim_stores` says. The Gold daily revenue must reflect all historical data combined with the current dimension
# MAGIC   state, and full re-evaluation is what guarantees it.
# MAGIC * **On serverless pipelines** the engine can often perform a Materialized View refresh incrementally when it can
# MAGIC   prove the result is equivalent, but the result is always equivalent to a full recomputation. The model to reason
# MAGIC   with stays "re-evaluate the query against current upstream state".

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: "Streaming Tables are micro-batch, Materialized Views are continuous, both keep aggregation state"
# MAGIC
# MAGIC The misconception: this confuses execution modes. Materialized Views do not use continuous processing; they
# MAGIC recompute on each triggered pipeline update (update 2 above). They keep no running aggregation state between
# MAGIC updates. And Streaming Tables are not preferred for Gold aggregations, because they cannot easily
# MAGIC "un-aggregate" previously processed data when a dimension row changes. A also says either construct works for
# MAGIC Silver; the cell below measures what a Materialized View over the raw files would re-read on every update.

# COMMAND ----------

# DBTITLE 1,Option A: Gold as a Streaming Table vs Gold as a Materialized View after the dimension change
streaming_gold = (spark.table(GOLD_ST).groupBy("order_date", "region")
                  .agg(F.round(F.sum("amount"), 2).alias("revenue_streaming_table")))
mv_gold = spark.table(GOLD_MV).select("order_date", "region", F.col("revenue").alias("revenue_materialized_view"))
display(streaming_gold.join(mv_gold, ["order_date", "region"], "full").orderBy("order_date", "region"))

print("S02 rows in streaming Gold, by the region captured when they were processed:")
spark.table(GOLD_ST).where("store_id = 'S02'").groupBy("order_date", "region").count().orderBy("order_date").show()

print("checkpoint folders that exist:", sorted(f.name for f in dbutils.fs.ls(CKPT)), "(none for the Materialized View)")

# A Materialized View over the raw files (option A's "either one for Silver") has no checkpoint: it re-reads every file.
mv_silver_reread = spark.read.schema(ORDER_SCHEMA).json(ORDERS_DIR)
print(f"rows a Materialized View for Silver re-reads on every update: {mv_silver_reread.count()} "
      f"(the Streaming Table read 30 in update 3, 0 in update 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output:
# MAGIC
# MAGIC * The streaming Gold still files S02's 2025-10-01 orders under **North**: each row was joined to the dimension
# MAGIC   once, when it was processed, and the Streaming Table never revisits it. North and East disagree with the current
# MAGIC   `dim_stores`; the Materialized View has them right. That is the "cannot un-aggregate" problem, and it is why a
# MAGIC   Streaming Table is the wrong choice for the Gold aggregation joined with `dim_stores`.
# MAGIC * The only checkpoints on disk belong to the streaming tables. The Materialized View keeps no running state; it
# MAGIC   recomputes on each triggered update, which is neither continuous nor stateful.
# MAGIC * A Materialized View for Silver re-reads all 70 rows on every update, and the count grows with every new file.
# MAGIC   "Either one for Silver" ignores the checkpoint, the one thing that makes ingestion incremental.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: "Materialized Views accept only stateless SELECT queries without aggregates or window functions"
# MAGIC
# MAGIC The misconception: this underestimates Materialized Views. They fully support aggregate expressions, window
# MAGIC functions and complex SQL and are not restricted to stateless SELECT statements, which is exactly why they suit
# MAGIC the Gold aggregation rather than the Silver ingestion. The first half of C is true (Streaming Tables do support
# MAGIC stateful operations such as `dropDuplicates`), but the conclusion swaps the layers.

# COMMAND ----------

# DBTITLE 1,Option C: the Gold query already uses GROUP BY, a join and a window function; Auto Loader is streaming-only
print("Gold Materialized View query uses an aggregate:", "sum(" in GOLD_SQL, "| a window function:", "OVER (" in GOLD_SQL)
display(spark.table(GOLD_MV).orderBy("order_date", "region_rank"))

# A Materialized View is a batch query. Auto Loader (cloudFiles) is a streaming source, so a batch read of it fails:
run("batch read of Auto Loader (what a Materialized View for Silver would need)",
    lambda: spark.read.format("cloudFiles").option("cloudFiles.format", "json").schema(ORDER_SCHEMA)
                 .load(ORDERS_DIR).count())

# The true half of C: a streaming read can deduplicate with state (the checkpoint remembers keys already seen).
dedup_new = run("streaming dropDuplicates into a scratch table", lambda: (
    spark.readStream.table(SILVER).dropDuplicates(["order_id"])
         .writeStream.option("checkpointLocation", f"{CKPT}/dedup_check").trigger(availableNow=True)
         .toTable(f"{FQ}.q63a9_dedup_check").awaitTermination()))
print("rows in the deduplicated stream:", table_rows(f"{FQ}.q63a9_dedup_check"))
spark.sql(f"DROP TABLE IF EXISTS {FQ}.q63a9_dedup_check")

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output: the Gold Materialized View ran an aggregate (`sum`, `count`), a join and a
# MAGIC window function (`rank() OVER ...`) without complaint, so "stateless SELECT only" is false. The batch read of
# MAGIC Auto Loader fails because Auto Loader is a streaming source, and a Materialized View is a batch query: putting the
# MAGIC Materialized View on the Silver Auto Loader ingestion and a Streaming Table on Gold is backwards on both layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: "Materialized Views live in an in-memory metastore cache lost on restart"
# MAGIC
# MAGIC The misconception: this confuses Materialized Views with a query result cache. Both Streaming Tables and
# MAGIC Materialized Views store data in Delta format on cloud storage; Materialized Views are fully persisted Delta tables
# MAGIC and survive cluster restarts. D also puts a Streaming Table on Gold, which section 3 already showed goes stale.

# COMMAND ----------

# DBTITLE 1,Option D: storage format and history of both stand-ins
for name in [SILVER, GOLD_MV]:
    d = spark.sql(f"DESCRIBE DETAIL {name}").select("name", "format", "numFiles", "sizeInBytes").first()
    versions = spark.sql(f"DESCRIBE HISTORY {name}").count()
    print(f"{d['name']:55s} format={d['format']}  files={d['numFiles']}  bytes={d['sizeInBytes']}  versions={versions}")

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output: both tables report `format=delta`, real data files, and a version history (the
# MAGIC Materialized View stand-in has one version per refresh). In a pipeline it is the same: a Materialized View is
# MAGIC backed by a Delta table in Unity Catalog managed storage; `DESCRIBE DETAIL` and `DESCRIBE HISTORY` work on it,
# MAGIC and nothing is lost when compute restarts, because nothing lived in compute memory in the first place. Persistence
# MAGIC is not a difference between the two constructs, so it cannot justify the layer choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: "Streaming Tables have no way to process CDC updates or deletes"
# MAGIC
# MAGIC The misconception: while Streaming Tables are primarily append-oriented, the Lakeflow Spark Declarative Pipelines
# MAGIC **AUTO CDC** API (formerly APPLY CHANGES) applies CDC updates and deletes into a Streaming Table, incrementally,
# MAGIC with a checkpoint. Preferring Materialized Views for Silver ingestion because source records get corrected or
# MAGIC deleted misrepresents their purpose. This is the pipeline source:

# COMMAND ----------

# DBTITLE 1,AUTO CDC into a Streaming Table, SQL and Python (annotated, not executed in this notebook)
AUTO_CDC_SQL = """
CREATE OR REFRESH STREAMING TABLE silver_stores;

CREATE FLOW silver_stores_cdc AS AUTO CDC INTO silver_stores
FROM STREAM(bronze_store_changes)
KEYS (store_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq)
STORED AS SCD TYPE 1;
"""
AUTO_CDC_PY = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

dp.create_streaming_table("silver_stores")
dp.create_auto_cdc_flow(
    target="silver_stores", source="bronze_store_changes", keys=["store_id"],
    sequence_by=F.col("seq"), apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq"], stored_as_scd_type=1)
'''
print(AUTO_CDC_SQL)
print(AUTO_CDC_PY)

# COMMAND ----------

# DBTITLE 1,Stand-in: a CDC feed applied incrementally to a streaming target (checkpoint + MERGE per micro-batch)
CHANGES = f"{FQ}.q63a9_store_changes"
TARGET = f"{FQ}.q63a9_silver_stores"
spark.sql(f"CREATE TABLE {CHANGES} (store_id STRING, region STRING, op STRING, seq INT)")
spark.sql(f"CREATE TABLE {TARGET} (store_id STRING, region STRING, seq INT)")

def apply_changes(batch_df, batch_id):
    """What AUTO CDC does for each micro-batch: keep the latest change per key, then upsert or delete by key."""
    batch_df.createOrReplaceTempView("q63a9_cdc_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {TARGET} t
        USING (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY seq DESC) AS rn
                              FROM q63a9_cdc_batch) WHERE rn = 1) s
        ON t.store_id = s.store_id
        WHEN MATCHED AND s.op = 'DELETE' AND s.seq > t.seq THEN DELETE
        WHEN MATCHED AND s.op != 'DELETE' AND s.seq > t.seq THEN UPDATE SET region = s.region, seq = s.seq
        WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT (store_id, region, seq) VALUES (s.store_id, s.region, s.seq)""")

def cdc_update(label: str) -> None:
    (spark.readStream.table(CHANGES).writeStream.foreachBatch(apply_changes)
          .option("checkpointLocation", f"{CKPT}/silver_stores").trigger(availableNow=True).start()
          .awaitTermination())
    print(label)
    spark.table(TARGET).orderBy("store_id").show()

spark.sql(f"INSERT INTO {CHANGES} VALUES ('S01','North','INSERT',1), ('S02','North','INSERT',1), ('S03','South','INSERT',1)")
cdc_update("after change batch 1 (three inserts):")
spark.sql(f"INSERT INTO {CHANGES} VALUES ('S02','East','UPDATE',2), ('S03',NULL,'DELETE',2), ('S07','West','INSERT',1)")
cdc_update("after change batch 2 (S02 corrected, S03 deleted, S07 added; only these 3 changes were read):")

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong, from the output: the streaming target took the correction to S02, removed S03 and added S07, and
# MAGIC the second update read only the three new change rows because the checkpoint remembered the first three. AUTO
# MAGIC CDC gives you this declaratively (with sequencing, out-of-order handling and SCD type 1 or 2) on a Streaming
# MAGIC Table. Corrected or deleted source records are a reason to use AUTO CDC on a Streaming Table, not a reason to move
# MAGIC Silver ingestion to a Materialized View, and E's Streaming Table for Gold goes stale as section 3 showed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rule to memorise
# MAGIC
# MAGIC A Streaming Table keeps a checkpoint and processes only the data appended since the last update, so it belongs on
# MAGIC incremental ingestion such as Auto Loader at Silver (and with AUTO CDC it applies updates and deletes too). A
# MAGIC Materialized View keeps no checkpoint and re-evaluates its query against the current state of all upstream tables
# MAGIC on every triggered update (serverless may do it incrementally, but always with the full-recompute result), so it
# MAGIC belongs on Gold aggregations and joins with dimensions that change. Both are persisted Delta tables, and
# MAGIC Materialized Views fully support aggregates, joins and window functions. That is B; A invents continuous MVs and
# MAGIC aggregation state, C restricts MVs to stateless SELECTs, D invents an in-memory cache, and E forgets AUTO CDC.
