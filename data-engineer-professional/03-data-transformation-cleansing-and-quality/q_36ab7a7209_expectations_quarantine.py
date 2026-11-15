# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Quarantine bad rows with expect_or_drop and an inverted predicate
# MAGIC
# MAGIC Data Engineer Professional, section 3 of the exam guide, "Data Transformation, Cleansing, and Quality".
# MAGIC Objective: develop a quarantining process for bad data using Lakeflow Spark Declarative Pipelines expectations.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works. A pipeline's `@dp.table` code cannot run in an ordinary notebook
# MAGIC cell (it only runs inside a pipeline update), so every option is shown twice: the exact pipeline source you
# MAGIC would deploy, printed as text, and a small local stand-in that applies the same expectation semantics to the
# MAGIC same rows and materialises the result as real Delta tables. Everything lives in `dev_catalog.de_pro_qnotes`
# MAGIC under the prefix `q36_`; nothing else is touched.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineer builds a Lakeflow Spark Declarative Pipeline (Delta Live Tables) that loads a Silver
# MAGIC `transactions` table. The requirement is: rows that fail a critical data-quality rule (`amount > 0`) must NOT
# MAGIC enter the Silver table, but they must also NOT be lost — they must be routed to a separate quarantine table for
# MAGIC investigation, and the pipeline must keep running rather than aborting the update. Which implementation meets
# MAGIC all of these requirements?
# MAGIC
# MAGIC * **A** Apply `@dp.expect_or_drop('valid_amount', 'amount > 0')` to the Silver table, and define a second table that reads the same source with the inverted predicate (`amount <= 0 OR amount IS NULL`) as the quarantine table.
# MAGIC * **B** Apply `@dp.expect_or_fail('valid_amount', 'amount > 0')` to the Silver table, and define a quarantine table that re-reads the failing rows recorded in the pipeline event log when the update stops on a violation.
# MAGIC * **C** Apply `@dp.expect('valid_amount', 'amount > 0')` to the Silver table, and define a quarantine table that reads the system-managed `_quarantine` table where warn-level expectations write the rows they keep out of Silver.
# MAGIC * **D** Add `CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW` to the Silver table, and set the `pipelines.quarantine.autoCapture` pipeline flag to mirror the dropped rows into a quarantine Delta table.
# MAGIC * **E** Apply `@dp.expect_all_or_drop({'valid_amount': 'amount > 0'})` to the Silver table, and define a quarantine table that reads the change data feed of dropped rows this decorator emits for the Silver table.
# MAGIC
# MAGIC The correct implementation (A) is built and run first on twelve deterministic transactions; then B, C, D and E
# MAGIC run against the same rows, each to the output (or the failure) it really produces.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "36ab7a7209",
    "track": "de_pro",
    "section": "Data Transformation, Cleansing, and Quality",
    "stem": "A data engineer builds a Lakeflow Spark Declarative Pipeline (Delta Live Tables) that loads a Silver `transactions` table. The requirement is: rows that fail a critical data-quality rule (`amount > 0`) must NOT enter the Silver table, but they must also NOT be lost — they must be routed to a separate quarantine table for investigation, and the pipeline must keep running rather than aborting the update. Which implementation meets all of these requirements?",
    "correct": "A",
    "options": {
        "A": "Apply `@dp.expect_or_drop('valid_amount', 'amount > 0')` to the Silver table, and define a second table that reads the same source with the inverted predicate (`amount <= 0 OR amount IS NULL`) as the quarantine table.",
        "B": "Apply `@dp.expect_or_fail('valid_amount', 'amount > 0')` to the Silver table, and define a quarantine table that re-reads the failing rows recorded in the pipeline event log when the update stops on a violation.",
        "C": "Apply `@dp.expect('valid_amount', 'amount > 0')` to the Silver table, and define a quarantine table that reads the system-managed `_quarantine` table where warn-level expectations write the rows they keep out of Silver.",
        "D": "Add `CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW` to the Silver table, and set the `pipelines.quarantine.autoCapture` pipeline flag to mirror the dropped rows into a quarantine Delta table.",
        "E": "Apply `@dp.expect_all_or_drop({'valid_amount': 'amount > 0'})` to the Silver table, and define a quarantine table that reads the change data feed of dropped rows this decorator emits for the Silver table."
    },
    "objective": "Develop a quarantining process for bad data using Lakeflow Spark Declarative Pipelines expectations.",
    "source": "testlogichub exams/fixes/qfix/36ab7a7209.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets and the sandbox schema
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
FQ = f"{CATALOG}.{SCHEMA}"
print("working schema:", FQ)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own q36_ tables so the notebook can be re-run
for t in ("bronze_transactions", "silver_a", "quarantine_a", "silver_b", "silver_c",
          "silver_d", "silver_e"):
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.q36_{t}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a Bronze source and an expectation engine stand-in
# MAGIC
# MAGIC **Bronze** is twelve transactions written as fixed literals, so every run produces the same output. Seven
# MAGIC have a positive `amount`; five break the rule in every way the inverted predicate has to catch: `0.00`, two
# MAGIC negative amounts and two `NULL` amounts. One otherwise valid row has the currency `XXX`; it only matters in
# MAGIC option E, where a second rule is added to show what "any rule" means.
# MAGIC
# MAGIC **The stand-in** `run_expectations(df, rules, action)` does what the pipeline engine does when it evaluates
# MAGIC expectations on a dataset during an update:
# MAGIC
# MAGIC * `warn` (`@dp.expect`, `EXPECT (...)` with no `ON VIOLATION`): keep every row, count the failures;
# MAGIC * `drop` (`@dp.expect_or_drop`, `@dp.expect_all_or_drop`, `ON VIOLATION DROP ROW`): remove failing rows, count
# MAGIC   them, carry on;
# MAGIC * `fail` (`@dp.expect_or_fail`, `ON VIOLATION FAIL UPDATE`): stop the update on the first violation, so nothing
# MAGIC   is committed.
# MAGIC
# MAGIC A row whose rule evaluates to `NULL` (here, a `NULL` amount) is treated as failing, which is exactly why the
# MAGIC quarantine predicate in option A spells out `OR amount IS NULL`. The stand-in returns an event-log-shaped dict:
# MAGIC the real event log's `flow_progress` events carry `details.flow_progress.data_quality.expectations`, a list of
# MAGIC `{name, dataset, passed_records, failed_records}` per expectation. Counts, not rows.

# COMMAND ----------

# DBTITLE 1,Bronze: twelve deterministic transactions
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal

BRONZE_ROWS = [
    (1,  "c01", Decimal("120.50"), "USD"),
    (2,  "c02", Decimal("35.00"),  "EUR"),
    (3,  "c03", Decimal("0.00"),   "USD"),   # bad: zero
    (4,  "c04", Decimal("89.99"),  "XXX"),   # good amount, unknown currency (used in option E)
    (5,  "c05", Decimal("-15.00"), "USD"),   # bad: negative (a refund posted with the wrong sign)
    (6,  "c06", None,              "EUR"),   # bad: NULL amount
    (7,  "c07", Decimal("42.10"),  "USD"),
    (8,  "c08", Decimal("250.00"), "EUR"),
    (9,  "c09", Decimal("-0.01"),  "USD"),   # bad: negative
    (10, "c10", Decimal("18.75"),  "USD"),
    (11, "c11", None,              "USD"),   # bad: NULL amount
    (12, "c12", Decimal("61.40"),  "EUR"),
]
BRONZE_SCHEMA = StructType([
    StructField("txn_id", IntegerType()),
    StructField("customer", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("currency", StringType()),
])
spark.createDataFrame(BRONZE_ROWS, BRONZE_SCHEMA).write.mode("overwrite").saveAsTable(f"{FQ}.q36_bronze_transactions")
bronze = spark.table(f"{FQ}.q36_bronze_transactions")
display(bronze.orderBy("txn_id"))

# COMMAND ----------

# DBTITLE 1,The stand-in: expectation semantics for warn / drop / fail, plus an event-log-shaped metrics record
from functools import reduce

class PipelineUpdateFailed(Exception):
    """What an expect_or_fail violation does to a pipeline update: it stops, nothing is committed."""

def run_expectations(df, rules: dict, action: str, dataset: str):
    """Apply {name: SQL condition} expectations the way a pipeline update does. Returns (rows_out, event)."""
    passes = {name: F.coalesce(F.expr(cond), F.lit(False)) for name, cond in rules.items()}  # NULL -> fails
    total = df.count()
    expectations = []
    for name, ok in passes.items():
        failed = df.where(~ok).count()
        expectations.append({"name": name, "dataset": dataset,
                             "passed_records": total - failed, "failed_records": failed})
    event = {"event_type": "flow_progress",
             "details": {"flow_progress": {"data_quality": {"expectations": expectations}}}}
    all_ok = reduce(lambda a, b: a & b, passes.values())
    if action == "warn":
        return df, event
    if action == "drop":
        return df.where(all_ok), event
    if action == "fail":
        first_bad = df.where(~all_ok).orderBy("txn_id").limit(1).collect()
        if first_bad:
            raise PipelineUpdateFailed(
                f"Expectation violated on dataset '{dataset}': {list(rules)} failed for row {first_bad[0].asDict()}. "
                "The update stopped; no table in this flow was committed.")
        return df, event
    raise ValueError(action)

def ids(table: str) -> list:
    return [r.txn_id for r in spark.table(table).orderBy("txn_id").select("txn_id").collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option A, run first: `expect_or_drop` on Silver plus an inverted-predicate quarantine table
# MAGIC
# MAGIC This is the pipeline source you would put in a Lakeflow Spark Declarative Pipelines source file. The `dp`
# MAGIC decorators come from `pyspark.pipelines`; the legacy `dlt` module exposes the same expectations
# MAGIC (`@dlt.expect_or_drop(...)`), so older code reads the same way. Both tables read the **same** source; the two
# MAGIC predicates are exact complements.

# COMMAND ----------

# DBTITLE 1,Option A: the pipeline source (deploy as a pipeline source file; printed here, not executed)
PIPELINE_SOURCE_A = '''
from pyspark import pipelines as dp

RULE = "amount > 0"
QUARANTINE_RULE = "amount <= 0 OR amount IS NULL"     # the inverted predicate, NULL included

@dp.table(name="silver_transactions", comment="Valid transactions only")
@dp.expect_or_drop("valid_amount", RULE)              # failing rows removed, counted, update continues
def silver_transactions():
    return spark.readStream.table("bronze_transactions")

@dp.table(name="quarantine_transactions", comment="Rows that failed valid_amount, kept for investigation")
def quarantine_transactions():
    return spark.readStream.table("bronze_transactions").where(QUARANTINE_RULE)
'''
print(PIPELINE_SOURCE_A)

# COMMAND ----------

# DBTITLE 1,Option A: one pipeline update through the stand-in, both tables materialised
RULES = {"valid_amount": "amount > 0"}
QUARANTINE_RULE = "amount <= 0 OR amount IS NULL"

silver_a, event_a = run_expectations(bronze, RULES, "drop", "silver_transactions")
quarantine_a = bronze.where(QUARANTINE_RULE)                    # second table, same source, inverted predicate

silver_a.write.mode("overwrite").saveAsTable(f"{FQ}.q36_silver_a")
quarantine_a.write.mode("overwrite").saveAsTable(f"{FQ}.q36_quarantine_a")

print("update status   : COMPLETED (no exception)")
print("silver ids      :", ids(f"{FQ}.q36_silver_a"))
print("quarantine ids  :", ids(f"{FQ}.q36_quarantine_a"))
print("event log record:", event_a["details"]["flow_progress"]["data_quality"]["expectations"])

# COMMAND ----------

# DBTITLE 1,Option A: nothing lost, nothing duplicated
b, s, q = (set(ids(f"{FQ}.q36_{t}")) for t in ("bronze_transactions", "silver_a", "quarantine_a"))
print(f"bronze {len(b)} = silver {len(s)} + quarantine {len(q)} ->", len(b) == len(s) + len(q))
print("overlap between silver and quarantine:", sorted(s & q) or "none")
print("rows in neither table               :", sorted(b - s - q) or "none")

# The common slip: invert with NOT (amount > 0). For a NULL amount that is NULL, not TRUE, so the row is filtered out.
naive = set(r.txn_id for r in bronze.where("NOT (amount > 0)").collect())
print("\nquarantine with NOT (amount > 0) only:", sorted(naive), "-> lost rows:", sorted(b - s - naive))
display(spark.table(f"{FQ}.q36_quarantine_a").orderBy("txn_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * The update **completes**: `expect_or_drop` removes the five failing rows from Silver and records
# MAGIC   `failed_records = 5` in the data-quality metrics, but it does not stop anything. Silver holds ids 1, 2, 4, 7,
# MAGIC   8, 10, 12.
# MAGIC * The quarantine table reads the **same source** with the **inverted predicate** `amount <= 0 OR amount IS NULL`
# MAGIC   and holds exactly the other five: 3, 5, 6, 9, 11. Seven plus five is twelve, no overlap, no row in neither
# MAGIC   table. Valid rows reach Silver, invalid rows are preserved in quarantine, and nothing is lost.
# MAGIC * The `OR amount IS NULL` is not decoration: `NOT (amount > 0)` alone evaluates to `NULL` for ids 6 and 11, so a
# MAGIC   quarantine table built with it silently loses them.
# MAGIC * The SQL form is the same pattern: `CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW` on the
# MAGIC   good table works as well as `expect_or_drop`; what makes it a quarantine is the explicit second table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option B: `expect_or_fail`, then rebuild quarantine from the event log
# MAGIC
# MAGIC The misconception: that a failing expectation leaves the bad rows somewhere you can re-read them, so failing
# MAGIC the update is a safe way to catch them.

# COMMAND ----------

# DBTITLE 1,Option B: the same update with expect_or_fail
# Pipeline source: @dp.expect_or_fail("valid_amount", "amount > 0") on silver_transactions
try:
    silver_b, event_b = run_expectations(bronze, RULES, "fail", "silver_transactions")
    silver_b.write.mode("overwrite").saveAsTable(f"{FQ}.q36_silver_b")
    print("update status: COMPLETED")
except PipelineUpdateFailed as e:
    print("update status: FAILED")
    print(e)
print("\nsilver_b exists after the update:", spark.catalog.tableExists(f"{FQ}.q36_silver_b"))

# COMMAND ----------

# DBTITLE 1,Option B: what the event log can give a quarantine table
# The data-quality part of a flow_progress event: one dict per expectation with counts. A failed update may quote
# the first offending record in its error message, but no event carries the full set of failing rows.
_, event_metrics = run_expectations(bronze, RULES, "warn", "silver_transactions")
expectations = event_metrics["details"]["flow_progress"]["data_quality"]["expectations"]
print(expectations)
print("columns a quarantine table could be built from:", sorted(expectations[0].keys()))
print("is 'txn_id' or 'amount' among them?", any(k in expectations[0] for k in ("txn_id", "amount")))

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * `expect_or_fail` **aborts the pipeline update on the first violation** (id 3 here). The update is
# MAGIC   transactional, so Silver is not written at all, not even the seven good rows. That breaks "the pipeline must
# MAGIC   keep running".
# MAGIC * The event log records **data-quality metrics**: per expectation, how many records passed and failed. It is
# MAGIC   not a store of the full offending rows, so a quarantine table cannot re-materialise them from it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: warn-level `expect`, then read a system `_quarantine` table
# MAGIC
# MAGIC The misconception: that a warning keeps the row out of Silver and parks it somewhere.

# COMMAND ----------

# DBTITLE 1,Option C: the same update with expect (warn)
# Pipeline source: @dp.expect("valid_amount", "amount > 0") on silver_transactions
silver_c, event_c = run_expectations(bronze, RULES, "warn", "silver_transactions")
silver_c.write.mode("overwrite").saveAsTable(f"{FQ}.q36_silver_c")
print("update status:", "COMPLETED")
print("silver ids   :", ids(f"{FQ}.q36_silver_c"))
print("metrics      :", event_c["details"]["flow_progress"]["data_quality"]["expectations"])
display(spark.table(f"{FQ}.q36_silver_c").where(f"NOT ({RULES['valid_amount']}) OR amount IS NULL").orderBy("txn_id"))

# COMMAND ----------

# DBTITLE 1,Option C: the quarantine table it wants to read from
for name in (f"{FQ}._quarantine", f"{FQ}.q36_silver_c_quarantine"):
    try:
        spark.table(f"`{name.split('.')[0]}`.`{name.split('.')[1]}`.`{name.split('.')[2]}`").count()
        print(name, "exists")
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(name, "->", type(e).__name__, first[:160])

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output:
# MAGIC
# MAGIC * `expect` (warn) **does not drop the row**. All twelve ids are in Silver, the five bad ones included; the
# MAGIC   violation is only counted in the metrics (`failed_records = 5`). The first requirement, "must NOT enter the
# MAGIC   Silver table", already fails.
# MAGIC * There is **no automatic system `_quarantine` table**: warn-level expectations write nowhere except the event
# MAGIC   log metrics, and the lookup fails with a table-not-found error. A quarantine table is something you define.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: `ON VIOLATION DROP ROW` plus a `pipelines.quarantine.autoCapture` flag
# MAGIC
# MAGIC The first half of D is right. The second half invents a configuration key.

# COMMAND ----------

# DBTITLE 1,Option D: the SQL source and the pipeline settings it relies on
PIPELINE_SOURCE_D = '''
CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_transactions)
'''
# The pipeline settings (Asset Bundle `resources.pipelines.<name>` or the Pipelines API) carry a free-form
# `configuration` map. Any key is accepted there and exposed to the source code as a Spark conf; no engine
# feature reads this one.
PIPELINE_SETTINGS_D = {
    "name": "q36_transactions",
    "catalog": CATALOG, "schema": SCHEMA, "serverless": True,
    "configuration": {"pipelines.quarantine.autoCapture": "true"},
}
print(PIPELINE_SOURCE_D)
print(PIPELINE_SETTINGS_D)

silver_d, event_d = run_expectations(bronze, RULES, "drop", "silver_transactions")   # DROP ROW = drop semantics
silver_d.write.mode("overwrite").saveAsTable(f"{FQ}.q36_silver_d")
print("update status:", "COMPLETED")
print("silver ids   :", ids(f"{FQ}.q36_silver_d"))
# Look in every table of the sandbox schema except bronze and option A's quarantine for the five dropped ids.
DROPPED = {3, 5, 6, 9, 11}
holders = []
for t in spark.catalog.listTables(FQ):
    if t.name in ("q36_bronze_transactions", "q36_quarantine_a") or not t.name.startswith("q36_"):
        continue
    if "txn_id" in spark.table(f"{FQ}.{t.name}").columns and DROPPED & set(ids(f"{FQ}.{t.name}")):
        holders.append(t.name)
print("quarantine tables created by the flag:", [t.name for t in spark.catalog.listTables(FQ) if "autocapture" in t.name.lower()] or "none")
print("q36_ tables holding dropped ids (excluding bronze and A's quarantine):", holders, "<- only C's Silver, which warned instead of dropping")

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output:
# MAGIC
# MAGIC * `ON VIOLATION DROP ROW` correctly drops without failing: Silver is the same seven rows as in option A and the
# MAGIC   update completes.
# MAGIC * `pipelines.quarantine.autoCapture` is **not a real configuration flag**. The `configuration` map accepts any
# MAGIC   key, so nothing complains, and nothing captures the five dropped rows either. Capturing them requires the
# MAGIC   explicit inverted-predicate table from option A.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: `expect_all_or_drop`, then read the change data feed of dropped rows
# MAGIC
# MAGIC The misconception: that dropping is a change to the target table, so it shows up in the target's change data
# MAGIC feed. To make Silver's change data feed real, the stand-in writes Silver into a Delta table with
# MAGIC `delta.enableChangeDataFeed = true` and reads it back with `table_changes`.

# COMMAND ----------

# DBTITLE 1,Option E: a dict of rules, a row fails if it breaks ANY of them
# Pipeline source: @dp.expect_all_or_drop({'valid_amount': 'amount > 0'}) on silver_transactions
two_rules = {"valid_amount": "amount > 0", "valid_currency": "currency IN ('USD', 'EUR')"}
kept_two, event_two = run_expectations(bronze, two_rules, "drop", "silver_transactions")
print("with the question's single rule: silver keeps",
      sorted(r.txn_id for r in run_expectations(bronze, RULES, "drop", "x")[0].collect()))
print("with a second rule added       : silver keeps", sorted(r.txn_id for r in kept_two.collect()),
      "(id 4 now dropped for its currency)")
print("per-rule metrics:", event_two["details"]["flow_progress"]["data_quality"]["expectations"])

# COMMAND ----------

# DBTITLE 1,Option E: Silver with change data feed on, and what the feed actually contains
spark.sql(f"""
    CREATE TABLE {FQ}.q36_silver_e (txn_id INT, customer STRING, amount DECIMAL(10,2), currency STRING)
    TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
silver_e, _ = run_expectations(bronze, RULES, "drop", "silver_transactions")
silver_e.write.mode("append").saveAsTable(f"{FQ}.q36_silver_e")

cdf = spark.sql(f"SELECT txn_id, amount, _change_type, _commit_version FROM table_changes('{FQ}.q36_silver_e', 0)")
display(cdf.orderBy("txn_id"))
print("change types in the feed:", sorted({r._change_type for r in cdf.collect()}))
print("dropped ids found in the feed:", sorted({r.txn_id for r in cdf.collect()} & {3, 5, 6, 9, 11}) or "none")

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong, from the output:
# MAGIC
# MAGIC * `expect_all_or_drop` takes a **dictionary of expectations** and drops a row that fails **any** of them; with
# MAGIC   the question's one-entry dict it behaves exactly like `expect_or_drop`, and adding a second rule drops id 4 too.
# MAGIC   The drop half of E is fine.
# MAGIC * The decorator **does not emit dropped rows as a change data feed**. A row that is dropped is never written to
# MAGIC   Silver, so Silver's feed only shows `insert` rows for the seven valid ids. None of 3, 5, 6, 9 or 11 appears, so
# MAGIC   the quarantine table cannot be populated this way.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The five options side by side
# MAGIC
# MAGIC | Option | Bad rows kept out of Silver | Bad rows preserved | Update keeps running |
# MAGIC |---|---|---|---|
# MAGIC | **A** `expect_or_drop` + inverted-predicate table | yes | yes, in the quarantine table | yes |
# MAGIC | **B** `expect_or_fail` + event log | yes, because nothing is written | no, the event log holds counts | no |
# MAGIC | **C** `expect` + `_quarantine` | no, warned rows are in Silver | no such table | yes |
# MAGIC | **D** `DROP ROW` + `autoCapture` flag | yes | no, the flag does not exist | yes |
# MAGIC | **E** `expect_all_or_drop` + CDF | yes | no, dropped rows are not in the feed | yes |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC Expectations only decide what happens to a failing row in **this** table: `expect` keeps it, `expect_or_drop`
# MAGIC (or `expect_all_or_drop`, or `ON VIOLATION DROP ROW`) drops it and carries on, `expect_or_fail` stops the
# MAGIC update. None of them saves the row anywhere; the event log only counts it. So quarantine is always two tables
# MAGIC over the same source: drop on the good table, and a second table with the inverted predicate, written as
# MAGIC `amount <= 0 OR amount IS NULL` so NULLs are not lost. There is no `_quarantine` system table, no
# MAGIC `autoCapture` flag and no change data feed of dropped rows.
