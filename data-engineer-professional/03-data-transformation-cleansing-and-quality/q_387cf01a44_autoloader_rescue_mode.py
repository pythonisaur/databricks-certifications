# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Auto Loader rescue mode: keep running, lose nothing
# MAGIC
# MAGIC Data Engineer Professional, section 3 of the exam guide, "Data Transformation, Cleansing, and Quality".
# MAGIC Objective: develop a quarantining / robust ingestion approach for malformed or unexpected data using Auto
# MAGIC Loader schema inference/evolution and the rescued data column.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the JSON files are written by the notebook into a Unity Catalog
# MAGIC volume, and every stream runs with `trigger(availableNow=True)`, so each "job run" is one cell that
# MAGIC processes whatever is new and stops. Everything lives in `dev_catalog.de_pro_qnotes`; nothing else is touched.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A classic job uses Auto Loader to ingest JSON files where new fields can appear without notice and some
# MAGIC fields occasionally arrive with type mismatches. The pipeline must keep running (no manual restarts), and
# MAGIC unexpected fields must not be silently dropped. Which configuration best satisfies these requirements while
# MAGIC minimizing custom state handling?
# MAGIC
# MAGIC * **A** Set `cloudFiles.schemaEvolutionMode='rescue'` with a persistent `cloudFiles.schemaLocation` and checkpoint; new fields and type mismatches are kept in `_rescued_data`
# MAGIC * **B** Set `badRecordsPath` with a persistent `cloudFiles.schemaLocation` and checkpoint; new fields and type mismatches are kept in the bad records files for replay
# MAGIC * **C** Set `cloudFiles.schemaEvolutionMode='addNewColumns'` and remove the checkpoint; new fields are added to the target and type mismatches are cast to strings
# MAGIC * **D** Set an explicit schema with `cloudFiles.schemaEvolutionMode='addNewColumns'`; new fields are appended to that schema and type mismatches are written as nulls
# MAGIC
# MAGIC The correct configuration (A) is built and run first, over two "job runs"; then B, C and D run against the
# MAGIC same two batches of files, each to the output (or the failure) it really produces.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "387cf01a44",
    "track": "de_pro",
    "section": "Data Transformation, Cleansing, and Quality",
    "stem": "A classic job uses Auto Loader to ingest JSON files where new fields can appear without notice and some fields occasionally arrive with type mismatches. The pipeline must keep running (no manual restarts), and unexpected fields must not be silently dropped. Which configuration best satisfies these requirements while minimizing custom state handling?",
    "correct": "A",
    "options": {
        "A": "Set `cloudFiles.schemaEvolutionMode='rescue'` with a persistent `cloudFiles.schemaLocation` and checkpoint; new fields and type mismatches are kept in `_rescued_data`",
        "B": "Set `badRecordsPath` with a persistent `cloudFiles.schemaLocation` and checkpoint; new fields and type mismatches are kept in the bad records files for replay",
        "C": "Set `cloudFiles.schemaEvolutionMode='addNewColumns'` and remove the checkpoint; new fields are added to the target and type mismatches are cast to strings",
        "D": "Set an explicit schema with `cloudFiles.schemaEvolutionMode='addNewColumns'`; new fields are appended to that schema and type mismatches are written as nulls"
    },
    "objective": "Develop a quarantining/robust ingestion approach for malformed or unexpected data using Auto Loader schema inference/evolution and the rescued data column.",
    "source": "testlogichub exams/fixes/qfix/387cf01a44.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets and the sandbox schema
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
VOLUME = "q_387cf01a44"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("working folder:", BASE)

# COMMAND ----------

# DBTITLE 1,Reset: start every run of this notebook from empty folders and tables
# Only this lesson's own volume folder and tables, so the notebook can be re-run from the top.
for option in ("a", "b", "c", "d", "d2"):
    try:
        dbutils.fs.rm(f"{BASE}/{option}", True)
    except Exception:
        pass
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.q387_orders_{option}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: two batches of JSON files and one "job run" helper
# MAGIC
# MAGIC **Batch 1** is two clean files, six orders with `order_id`, `customer` and `amount`. Auto Loader infers the
# MAGIC schema from it (`amount` becomes a double because `cloudFiles.inferColumnTypes` is on).
# MAGIC
# MAGIC **Batch 2** arrives later and carries exactly the two problems in the question:
# MAGIC
# MAGIC * order 7 has a field nobody announced, `coupon` (schema drift: a **new field**),
# MAGIC * order 8 has `"amount": "12,50"`, a string where the schema says double (a **type mismatch**).
# MAGIC
# MAGIC Each option gets its own landing folder, checkpoint and target table, so the four configurations never share
# MAGIC state. The data is fixed text: every run of the notebook produces the same output.

# COMMAND ----------

# DBTITLE 1,The two batches and the function that lands them
import json

BATCH_1 = {
    "orders_001.json": [{"order_id": i, "customer": f"c{i:02d}", "amount": round((i * 37) % 50 + 0.99, 2)} for i in (1, 2, 3)],
    "orders_002.json": [{"order_id": i, "customer": f"c{i:02d}", "amount": round((i * 37) % 50 + 0.99, 2)} for i in (4, 5, 6)],
}
BATCH_2 = {
    "orders_003.json": [
        {"order_id": 7, "customer": "c07", "amount": 31.99, "coupon": "WELCOME10"},   # new field
        {"order_id": 8, "customer": "c08", "amount": "12,50"},                        # type mismatch
        {"order_id": 9, "customer": "c09", "amount": 8.99},
    ],
}

def land(option: str, batch: dict) -> str:
    """Write a batch of JSON-lines files into the option's landing folder (what the upstream system would do)."""
    src = f"{BASE}/{option}/landing"
    for name, rows in batch.items():
        dbutils.fs.put(f"{src}/{name}", "\n".join(json.dumps(r) for r in rows), True)
    return src

for name, rows in {**BATCH_1, **BATCH_2}.items():
    print(name)
    for r in rows:
        print("   ", json.dumps(r))

# COMMAND ----------

# DBTITLE 1,One job run = one availableNow stream; failures are caught and printed, not hidden
from pyspark.sql import functions as F

def job_run(option: str, reader_options: dict, schema=None) -> str:
    """Run the Auto Loader stream once (availableNow) into q387_orders_<option> and report OK or the real error."""
    src = f"{BASE}/{option}/landing"
    table = f"{CATALOG}.{SCHEMA}.q387_orders_{option}"
    try:
        reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
        for k, v in reader_options.items():
            reader = reader.option(k, v)
        if schema is not None:
            reader = reader.schema(schema)
        query = (reader.load(src)
                 .writeStream
                 .option("checkpointLocation", f"{BASE}/{option}/checkpoint")
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .toTable(table))
        query.awaitTermination()
        status = "OK"
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        status = f"FAILED  {type(e).__name__}: {first[:300]}"
    rows = spark.table(table).count() if spark.catalog.tableExists(table) else 0
    print(f"[{option}] {status}\n[{option}] rows in target now: {rows}")
    return status

def files_under(path: str) -> list:
    """Every file below a folder (recursive); [] when the folder does not exist."""
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        return []
    out = []
    for e in entries:
        out += files_under(e.path) if e.isDir() else [e.path.replace(BASE, "")]
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option A, run first: `rescue` mode with a persistent schema location and checkpoint
# MAGIC
# MAGIC Three job runs on the same `cloudFiles.schemaLocation` and the same checkpoint:
# MAGIC
# MAGIC 1. batch 1 only: the schema is inferred and stored as version 0 in the schema location;
# MAGIC 2. batch 2 lands: the new field and the bad `amount` arrive, and the stream **does not fail**;
# MAGIC 3. nothing new lands: the checkpoint knows every file, so zero rows are added.

# COMMAND ----------

# DBTITLE 1,Option A: the configuration
OPTION_A = {
    "cloudFiles.schemaLocation": f"{BASE}/a/schema",   # persistent: schema versions live here
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "rescue",
}
land("a", BATCH_1)
job_run("a", OPTION_A)   # run 1

# COMMAND ----------

# DBTITLE 1,Option A: batch 2 arrives, then a run with nothing new
land("a", BATCH_2)
job_run("a", OPTION_A)   # run 2: new field + type mismatch, still OK
job_run("a", OPTION_A)   # run 3: no new files, the checkpoint skips everything already ingested

# COMMAND ----------

# DBTITLE 1,Option A: nothing was dropped, it sits in _rescued_data
a = spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_a")
print("target columns:", a.columns)
display(
    a.orderBy("order_id")
     .withColumn("rescued_coupon", F.get_json_object("_rescued_data", "$.coupon"))
     .withColumn("rescued_amount", F.get_json_object("_rescued_data", "$.amount"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * All three runs report **OK**. In `rescue` mode Auto Loader never fails the stream because of a new column,
# MAGIC   and never ignores the unexpected field either: it records it in the rescued data column, default name
# MAGIC   `_rescued_data`. Order 7's `coupon` is there as JSON, together with the `_file_path` it came from.
# MAGIC * Order 8's `amount` is null in the typed column, and the original `"12,50"` is kept in `_rescued_data`.
# MAGIC   Type mismatches (and column names that differ only in case) go to the same place, so you can quarantine or
# MAGIC   inspect rescued content later with an ordinary `WHERE _rescued_data IS NOT NULL`.
# MAGIC * Run 3 adds no rows: the checkpoint is Auto Loader's file-discovery state. Together with the persistent
# MAGIC   `cloudFiles.schemaLocation` it is everything needed for reliable incremental processing and schema
# MAGIC   tracking, so the job keeps no state of its own: that is the "minimizing custom state handling" part.
# MAGIC
# MAGIC The schema location proves the schema was frozen: one version, written on run 1, never replaced.

# COMMAND ----------

# DBTITLE 1,Option A: the schema location holds one version, the target schema did not drift
print("schema versions:", [p for p in files_under(f"{BASE}/a/schema") if "/_schemas/" in p])
spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_a").printSchema()
display(spark.sql(f"""
    SELECT order_id, _rescued_data            -- the quarantine query a downstream task would run
    FROM {CATALOG}.{SCHEMA}.q387_orders_a
    WHERE _rescued_data IS NOT NULL
    ORDER BY order_id"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option B: `badRecordsPath` instead of a schema evolution mode
# MAGIC
# MAGIC Same persistent schema location and checkpoint, plus `badRecordsPath`. No `schemaEvolutionMode` is set, so
# MAGIC Auto Loader uses its default for an inferred schema, `addNewColumns`. The misconception: that the bad
# MAGIC records sink is where schema drift goes.

# COMMAND ----------

# DBTITLE 1,Option B: two job runs, batch 2 lands in between
OPTION_B = {
    "cloudFiles.schemaLocation": f"{BASE}/b/schema",
    "cloudFiles.inferColumnTypes": "true",
    "badRecordsPath": f"{BASE}/b/bad_records",
}
land("b", BATCH_1)
job_run("b", OPTION_B)   # run 1
land("b", BATCH_2)
job_run("b", OPTION_B)   # run 2

# COMMAND ----------

# DBTITLE 1,Option B: what is in the bad records folder, and where order 8's amount went
print("files under badRecordsPath:", files_under(f"{BASE}/b/bad_records") or "none")
job_run("b", OPTION_B)   # run 3: the manual restart option B was supposed to avoid
display(spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_b").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * `badRecordsPath` stores **malformed or corrupt records**: lines the JSON parser cannot read at all. A new
# MAGIC   field and a value of the wrong type are not corrupt, so they are not necessarily treated as bad records, and
# MAGIC   on this data the folder stays empty. The type mismatch is handled by the rescued data column, which Auto
# MAGIC   Loader adds by default to an inferred schema: order 8's `"12,50"` is in `_rescued_data`, not in a replay file.
# MAGIC * `badRecordsPath` does not change how new columns are handled. With the default `addNewColumns` mode, run 2
# MAGIC   fails on `coupon` and only run 3, a restart, ingests batch 2. The pipeline did not keep running.
# MAGIC
# MAGIC B confuses the corrupt-record sink with schema drift handling. (If your compute rejects `badRecordsPath` for
# MAGIC this source outright, run 1 already fails, which makes the same point.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: `addNewColumns` and remove the checkpoint
# MAGIC
# MAGIC Two claims hide in C: that `addNewColumns` keeps the job running, and that removing the checkpoint is
# MAGIC harmless. Four runs test both.

# COMMAND ----------

# DBTITLE 1,Option C: run 1, batch 2 lands, run 2 fails, run 3 (the restart) evolves the schema
OPTION_C = {
    "cloudFiles.schemaLocation": f"{BASE}/c/schema",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
}
land("c", BATCH_1)
job_run("c", OPTION_C)   # run 1
land("c", BATCH_2)
job_run("c", OPTION_C)   # run 2: stops on the new column after recording the new schema
job_run("c", OPTION_C)   # run 3: the restart picks up the evolved schema
print("schema versions:", [p for p in files_under(f"{BASE}/c/schema") if "/_schemas/" in p])

# COMMAND ----------

# DBTITLE 1,Option C: the new column was added; the mismatched amount was not cast to a string
c = spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_c")
print("amount column type:", dict(c.dtypes)["amount"])
display(c.orderBy("order_id"))

# COMMAND ----------

# DBTITLE 1,Option C: now "remove the checkpoint" and run again
dbutils.fs.rm(f"{BASE}/c/checkpoint", True)
job_run("c", OPTION_C)   # run 4: no memory of which files were ingested
display(spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_c")
        .groupBy("order_id").count().where("count > 1").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output:
# MAGIC
# MAGIC * `addNewColumns` **stops the stream** when a new column arrives: run 2 fails after Auto Loader writes the new
# MAGIC   schema version to the schema location, and only a restart (run 3) continues. In production that restart is
# MAGIC   a Lakeflow Jobs retry, but the requirement was no restarts.
# MAGIC * The mismatched value was not cast to a string: `amount` stays a double, order 8's amount is null, and the
# MAGIC   original text is in `_rescued_data`. Capturing mismatched values is the job of the rescued data column.
# MAGIC * Removing the checkpoint breaks exactly-once: run 4 re-read all three files and every order is now in the
# MAGIC   table twice. The checkpoint is what lets Auto Loader track state without custom code; delete it and you
# MAGIC   own deduplication yourself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: an explicit schema with `addNewColumns`
# MAGIC
# MAGIC D pins the schema with `.schema(...)` and asks for `addNewColumns` at the same time.

# COMMAND ----------

# DBTITLE 1,Option D: explicit schema plus addNewColumns
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

EXPLICIT = StructType([
    StructField("order_id", LongType()),
    StructField("customer", StringType()),
    StructField("amount", DoubleType()),
])
OPTION_D = {"cloudFiles.schemaEvolutionMode": "addNewColumns"}
land("d", BATCH_1)
land("d", BATCH_2)
job_run("d", OPTION_D, schema=EXPLICIT)

# COMMAND ----------

# DBTITLE 1,Option D without the forbidden mode: what an explicit schema does to batch 2
# With an explicit schema the default evolution mode is `none`, and no rescued data column is added.
land("d2", BATCH_1)
land("d2", BATCH_2)
job_run("d2", {}, schema=EXPLICIT)
d2 = spark.table(f"{CATALOG}.{SCHEMA}.q387_orders_d2")
print("target columns:", d2.columns)
display(d2.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output:
# MAGIC
# MAGIC * When you provide an explicit schema, `addNewColumns` is not allowed the same way: the stream is rejected
# MAGIC   instead of starting. The defaults differ too: with a provided schema the evolution mode defaults to `none`.
# MAGIC   To let a partly known schema evolve you give the known types as `cloudFiles.schemaHints` and let Auto Loader
# MAGIC   infer the rest, or handle drift another way.
# MAGIC * Dropping the evolution mode to make it start shows the real cost: there is no rescued data column, order 7's
# MAGIC   `coupon` is gone without a trace, and order 8's amount is a plain null. Mismatched values written as nulls
# MAGIC   silently lose data the requirements say must be kept, so this is not the keep-running configuration either.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The four options side by side
# MAGIC
# MAGIC The same two batches, the same question for each target table: is order 7's `coupon` anywhere, and is order
# MAGIC 8's original `"12,50"` anywhere?

# COMMAND ----------

# DBTITLE 1,Where did the new field and the bad value end up?
def found(option: str) -> tuple:
    table = f"{CATALOG}.{SCHEMA}.q387_orders_{option}"
    if not spark.catalog.tableExists(table):
        return ("no table", "no table", 0)
    t = spark.table(table)
    text = t.select(F.to_json(F.struct(*t.columns)).alias("j")).collect()
    blob = " ".join(r.j for r in text)
    return ("WELCOME10" in blob, "12,50" in blob, t.count())

print(f"{'option':8s} {'coupon kept':>12s} {'12,50 kept':>11s} {'rows':>5s}")
for opt, label in [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("d2", "D*")]:
    coupon, amount, rows = found(opt)
    print(f"{label:8s} {str(coupon):>12s} {str(amount):>11s} {rows:5d}")
print("\nD* = D with the evolution mode removed so that it starts at all")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rule to memorise
# MAGIC
# MAGIC "Keep running" plus "don't drop unexpected fields" is `cloudFiles.schemaEvolutionMode = 'rescue'`: the schema
# MAGIC stays frozen, the stream never fails on a new column, and new fields, type mismatches and case mismatches land
# MAGIC in `_rescued_data` for later quarantine. `addNewColumns` (the default when the schema is inferred) fails once
# MAGIC and needs a restart; it is not allowed with an explicit schema, where the default is `none` and mismatches
# MAGIC become nulls. `badRecordsPath` is for corrupt records, not schema drift. The persistent `schemaLocation` and
# MAGIC checkpoint are the state; never delete the checkpoint, because that re-ingests everything.
