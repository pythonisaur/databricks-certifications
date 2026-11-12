# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Star schema layout: liquid clustering plus broadcast joins
# MAGIC
# MAGIC Data Engineer Professional, section 10 of the exam guide, "Data Modelling".
# MAGIC Objective: design dimensional models for analytical workloads, ensuring efficient querying and aggregation.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the notebook builds a miniature star schema (a 600-row
# MAGIC `fact_orders`, a 40-row `dim_product`, a 120-row `dim_customer`, all derived from `range()` and `hash()` so
# MAGIC every run is identical) and lays the fact table out the way each option says. Everything lives in
# MAGIC `dev_catalog.de_pro_qnotes` under a `qfafe_` prefix; nothing else is touched. The real sizes in the question
# MAGIC (500 GB, 10 MB, 200 MB) cannot be built on Free Edition, so the broadcast decision at those sizes runs as a
# MAGIC small stand-in of Spark's own size rule next to the real plans.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineer is designing a star schema for a large analytical workload in Databricks. The central fact
# MAGIC table `fact_orders` (500 GB, growing ~5 GB/day) is joined frequently with `dim_product` (10 MB) and
# MAGIC `dim_customer` (200 MB) on `product_id` and `customer_id` respectively. Analysts filter primarily by
# MAGIC `order_date` and `product_category`. Which combination of design decisions BEST optimizes query performance for
# MAGIC this star schema?
# MAGIC
# MAGIC * **A** Partition `fact_orders` by `order_date`, add `CLUSTER BY (product_category)` on the same table, and broadcast both dimensions with a `broadcast()` hint in each query
# MAGIC * **B** Partition `fact_orders` by `product_id` and `customer_id` so joins with both dimensions prune partitions, and keep the dimension joins as shuffle sort-merge joins
# MAGIC * **C** Partition `fact_orders` by `product_id`, run `OPTIMIZE ... ZORDER BY (order_date, product_category)` on it, and broadcast both dimensions with a `broadcast()` hint
# MAGIC * **D** Use `CLUSTER BY (order_date, product_category)` on `fact_orders` and broadcast both dimensions, raising `spark.sql.autoBroadcastJoinThreshold` or hinting for `dim_customer`
# MAGIC
# MAGIC The correct option (D) is built and run first; then A, B and C each lay out the same 600 orders and answer the
# MAGIC same analyst query, so you can see where each one fails or what it costs.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "fafe65ac21",
    "track": "de_pro",
    "section": "Data Modelling",
    "stem": "A data engineer is designing a star schema for a large analytical workload in Databricks. The central fact table `fact_orders` (500 GB, growing ~5 GB/day) is joined frequently with `dim_product` (10 MB) and `dim_customer` (200 MB) on `product_id` and `customer_id` respectively. Analysts filter primarily by `order_date` and `product_category`. Which combination of design decisions BEST optimizes query performance for this star schema?",
    "correct": "D",
    "options": {
        "A": "Partition `fact_orders` by `order_date`, add `CLUSTER BY (product_category)` on the same table, and broadcast both dimensions with a `broadcast()` hint in each query",
        "B": "Partition `fact_orders` by `product_id` and `customer_id` so joins with both dimensions prune partitions, and keep the dimension joins as shuffle sort-merge joins",
        "C": "Partition `fact_orders` by `product_id`, run `OPTIMIZE ... ZORDER BY (order_date, product_category)` on it, and broadcast both dimensions with a `broadcast()` hint",
        "D": "Use `CLUSTER BY (order_date, product_category)` on `fact_orders` and broadcast both dimensions, raising `spark.sql.autoBroadcastJoinThreshold` or hinting for `dim_customer`"
    },
    "objective": "Design Dimensional Models for analytical workloads, ensuring efficient querying and aggregation",
    "source": "testlogichub exams/fixes/qfix/fafe65ac21.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and helpers (real errors, join strategy, file pruning)
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")

def run(stmt: str):
    """Run one SQL statement; return the DataFrame, or print the real error (first line) and return None."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {type(e).__name__}: {first[:300]}")
        return None

def join_strategy(query: str) -> str:
    """EXPLAIN the query and summarise the physical join operators (Photon prefixes its nodes with 'Photon')."""
    plan = spark.sql(f"EXPLAIN {query}").first()[0]
    joins = [l.strip()[:120] for l in plan.splitlines() if "Join" in l and ("Hash" in l or "Merge" in l or "NestedLoop" in l)]
    shuffles = sum(1 for l in plan.splitlines() if "Exchange" in l and "hashpartitioning" in l.lower())
    for j in joins:
        print("   ", j.lstrip("+- :"))
    print(f"    hash-partitioning exchanges in the plan: {shuffles} (the GROUP BY needs one; each shuffle join adds two)")
    return "broadcast" if joins and all("Broadcast" in j for j in joins) else "shuffle"

PROBE_DATE, PROBE_CATEGORY = "2026-10-07", "grocery"

def files_touched(table: str) -> dict:
    """Per-file min/max of the two filter columns, the statistics Delta data skipping reads.
    A file can be skipped when the probe value falls outside its [min, max] for either column."""
    stats = spark.sql(f"""
        SELECT _metadata.file_path AS f, count(*) AS n_rows,
               min(order_date) AS d_min, max(order_date) AS d_max,
               min(product_category) AS c_min, max(product_category) AS c_max
        FROM {table} GROUP BY _metadata.file_path""").collect()
    keep = [s for s in stats
            if str(s.d_min) <= PROBE_DATE <= str(s.d_max) and s.c_min <= PROBE_CATEGORY <= s.c_max]
    return {"files": len(stats), "files to read": len(keep),
            "rows per file": round(sum(s.n_rows for s in stats) / max(len(stats), 1), 1)}

RESULTS = {}   # filled in by every option, compared in the last section
print("working schema:", FQ)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'qfafe_*'").collect():
    if not t.isTemporary:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: one miniature star schema shared by every option
# MAGIC
# MAGIC | table | rows here | in the question | joined on |
# MAGIC |---|---|---|---|
# MAGIC | `qfafe_dim_product` | 40 | 10 MB | `product_id` |
# MAGIC | `qfafe_dim_customer` | 120 | 200 MB | `customer_id` |
# MAGIC | `qfafe_orders_src` (temp view, the fact rows) | 600, 30 per day over 20 days | 500 GB, +5 GB/day | |
# MAGIC
# MAGIC `product_category` is carried on the fact row as well as on `dim_product`, because the analysts filter the fact
# MAGIC by it and every option clusters, Z-orders or partitions the fact by it. Each option writes its own copy of the
# MAGIC same 600 rows (`qfafe_a_...`, `qfafe_b_...`, ...), so the layouts never interfere.

# COMMAND ----------

# DBTITLE 1,The two dimensions and the fact rows
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_dim_product AS
    SELECT id AS product_id, concat('product_', lpad(cast(id AS STRING), 2, '0')) AS product_name,
           element_at(array('electronics', 'grocery', 'apparel', 'home', 'toys', 'sports'), cast(id % 6 + 1 AS INT)) AS product_category
    FROM range(1, 41)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_dim_customer AS
    SELECT id AS customer_id, concat('customer_', id) AS customer_name,
           element_at(array('NA', 'EU', 'APAC'), cast(id % 3 + 1 AS INT)) AS region
    FROM range(1, 121)""")
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW qfafe_orders_src AS
    SELECT o.id AS order_id,
           date_add(DATE'2026-10-01', cast((o.id - 1) DIV 30 AS INT)) AS order_date,
           p.product_id, p.product_category,
           cast(abs(hash(o.id, 'c')) % 120 + 1 AS BIGINT) AS customer_id,
           cast(round((abs(hash(o.id, 'amt')) % 50000) / 100.0, 2) AS DOUBLE) AS amount
    FROM range(1, 601) o
    JOIN {FQ}.qfafe_dim_product p ON p.product_id = abs(hash(o.id, 'p')) % 40 + 1""")
spark.sql("""SELECT count(*) AS orders, count(DISTINCT order_date) AS days, count(DISTINCT product_id) AS products,
                    count(DISTINCT customer_id) AS customers, count(DISTINCT product_id, customer_id) AS product_customer_pairs
             FROM qfafe_orders_src""").show()
display(spark.table("qfafe_orders_src").orderBy("order_id").limit(5))

# COMMAND ----------

# DBTITLE 1,The analyst query every option must answer
def analyst_query(fact: str, hints: str = "") -> str:
    return f"""
        SELECT {hints} c.region, p.product_name, count(*) AS orders, round(sum(f.amount), 2) AS revenue
        FROM {fact} f
        JOIN {FQ}.qfafe_dim_product p ON f.product_id = p.product_id
        JOIN {FQ}.qfafe_dim_customer c ON f.customer_id = c.customer_id
        WHERE f.order_date = DATE'{PROBE_DATE}' AND f.product_category = '{PROBE_CATEGORY}'
        GROUP BY c.region, p.product_name ORDER BY c.region, p.product_name"""

EXPECTED = spark.sql(analyst_query("qfafe_orders_src")).collect()
print(f"filter: order_date = {PROBE_DATE} AND product_category = '{PROBE_CATEGORY}' -> {len(EXPECTED)} result rows")
spark.sql(analyst_query("qfafe_orders_src")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option D, run first: `CLUSTER BY (order_date, product_category)` and broadcast both dimensions
# MAGIC
# MAGIC Two independent decisions, one per side of the star:
# MAGIC
# MAGIC * **The fact table gets liquid clustering on the filter columns.** No `PARTITIONED BY`: liquid clustering
# MAGIC   replaces it. `OPTIMIZE` physically co-locates rows that share the same `order_date` and `product_category`,
# MAGIC   so each file covers a narrow range of both, and a query that filters on them skips every file whose min/max
# MAGIC   statistics exclude the value (data skipping). The table grows by appending daily batches; new data is
# MAGIC   clustered incrementally, and the keys can later be changed with `ALTER TABLE ... CLUSTER BY` without
# MAGIC   rewriting the table. That is why liquid clustering is the Databricks-recommended layout for a large, growing
# MAGIC   fact table with analytical filters.
# MAGIC * **The dimensions are broadcast**, which removes the shuffle of the 500 GB fact table for each join.

# COMMAND ----------

# DBTITLE 1,Option D: the clustered fact table, loaded the way it grows (four batches of five days)
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_d_fact_orders (
        order_id BIGINT, order_date DATE, product_id BIGINT, product_category STRING, customer_id BIGINT, amount DOUBLE)
    CLUSTER BY (order_date, product_category)""")
for batch in range(4):
    spark.sql(f"INSERT INTO {FQ}.qfafe_d_fact_orders SELECT * FROM qfafe_orders_src WHERE (order_id - 1) DIV 150 = {batch}")
print("before OPTIMIZE:", files_touched(f"{FQ}.qfafe_d_fact_orders"))
spark.sql(f"OPTIMIZE {FQ}.qfafe_d_fact_orders").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
RESULTS["D"] = files_touched(f"{FQ}.qfafe_d_fact_orders")
print("after OPTIMIZE :", RESULTS["D"])
display(spark.sql(f"DESCRIBE DETAIL {FQ}.qfafe_d_fact_orders").select("partitionColumns", "clusteringColumns", "numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows: `partitionColumns` is empty and `clusteringColumns` is `[order_date, product_category]`.
# MAGIC `files to read` counts the files whose min/max statistics could hold the probe date and category; every other
# MAGIC file is skipped without being opened. At lab size `OPTIMIZE` may pack all 600 rows into one or a few files
# MAGIC (it targets files of roughly 1 GB). At 500 GB the same command writes hundreds of files, each covering a small
# MAGIC box of dates and categories, so a one-day, one-category query reads a handful of them. The new 5 GB each day
# MAGIC is clustered by the next `OPTIMIZE` (or automatically with predictive optimization) without touching old data.

# COMMAND ----------

# DBTITLE 1,Option D: the joins at the question's real sizes (stand-in for Spark's size rule)
DEFAULT_THRESHOLD = 10 * 1024 * 1024        # spark.sql.autoBroadcastJoinThreshold default: 10 MB
try:
    print("autoBroadcastJoinThreshold on this compute:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
except Exception as e:
    print("autoBroadcastJoinThreshold is not readable on this compute:", type(e).__name__)

def planned_join(size_mb: float, threshold_bytes: int, hinted: bool = False) -> str:
    """Spark's rule: broadcast when the side's estimated size <= the threshold, or when a hint asks for it."""
    return "broadcast" if hinted or size_mb * 1024 * 1024 <= threshold_bytes else "shuffle sort-merge"

print(f"\n{'dimension':14s}{'size':>8s}{'default 10 MB':>22s}{'threshold 256 MB':>22s}{'broadcast() hint':>22s}")
for name, mb in [("dim_product", 10), ("dim_customer", 200)]:
    print(f"{name:14s}{mb:>6d}MB{planned_join(mb, DEFAULT_THRESHOLD):>22s}"
          f"{planned_join(mb, 256 * 1024 * 1024):>22s}{planned_join(mb, DEFAULT_THRESHOLD, True):>22s}")

# COMMAND ----------

# DBTITLE 1,Option D: raise the threshold (classic compute) or hint dim_customer (works everywhere)
# On classic compute this is the session-level way to make the 200 MB dimension broadcast automatically:
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(256 * 1024 * 1024))
    print("threshold raised to 256 MB for this session")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
except Exception as e:
    first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
    print(f"not settable here ({type(e).__name__}: {first[:160]}); the hint below does the same job")

# The hint form: dim_product needs nothing (10 MB broadcasts at the default), dim_customer gets BROADCAST(c).
D_QUERY = analyst_query(f"{FQ}.qfafe_d_fact_orders", hints="/*+ BROADCAST(c) */")
print("\njoin plan for option D:")
RESULTS["D"]["joins"] = join_strategy(D_QUERY)
d_rows = spark.sql(D_QUERY).collect()
print("same answer as the source rows:", d_rows == EXPECTED)

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is right, from the output:
# MAGIC
# MAGIC * `dim_product` (10 MB) sits at the default `spark.sql.autoBroadcastJoinThreshold` of 10 MB, and Spark
# MAGIC   broadcasts any side whose estimated size is at or below the threshold, so it is broadcast automatically.
# MAGIC * `dim_customer` (200 MB) is above it. Either raise the threshold (the `spark.conf.set` cell; serverless
# MAGIC   compute restricts which Spark configs a session may set, which is why the cell may report it as not
# MAGIC   settable) or add `/*+ BROADCAST(c) */` / `broadcast(dim_customer)`. 200 MB is a routine broadcast.
# MAGIC * The plan shows broadcast hash joins (Photon prints `PhotonBroadcastHashJoin`) and no hash-partitioning shuffle
# MAGIC   of the fact table for either join: that is the standard star-schema tuning.
# MAGIC * The dimensions need no layout work of their own: they are read whole and shipped to every executor, so
# MAGIC   partitioning or clustering a 10 MB or 200 MB table buys nothing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: partition by `order_date` and `CLUSTER BY (product_category)` on the same table
# MAGIC
# MAGIC The misconception: that partitioning and liquid clustering are layers you can stack (partition by date, then
# MAGIC cluster inside each partition). The broadcast hints in A are fine; the table definition is what fails.

# COMMAND ----------

# DBTITLE 1,Option A: declare both in one CREATE TABLE
a_create = run(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_a_fact_orders (
        order_id BIGINT, order_date DATE, product_id BIGINT, product_category STRING, customer_id BIGINT, amount DOUBLE)
    PARTITIONED BY (order_date)
    CLUSTER BY (product_category)""")
print("created" if a_create is not None else "-> no table")

# COMMAND ----------

# DBTITLE 1,Option A: partition first, then try to add clustering afterwards
run(f"CREATE OR REPLACE TABLE {FQ}.qfafe_a_fact_orders PARTITIONED BY (order_date) AS SELECT * FROM qfafe_orders_src")
run(f"ALTER TABLE {FQ}.qfafe_a_fact_orders CLUSTER BY (product_category)")
display(spark.sql(f"DESCRIBE DETAIL {FQ}.qfafe_a_fact_orders").select("partitionColumns", "clusteringColumns", "numFiles"))

# COMMAND ----------

# DBTITLE 1,The same rule from the other side: ZORDER on the clustered table from section 2
run(f"OPTIMIZE {FQ}.qfafe_d_fact_orders ZORDER BY (order_date)")
RESULTS["A"] = {"files": "error", "files to read": "error", "rows per file": "error", "joins": "never reached"}

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output:
# MAGIC
# MAGIC * Declaring `PARTITIONED BY` and `CLUSTER BY` together fails, and `ALTER TABLE ... CLUSTER BY` on the
# MAGIC   partitioned table fails too: liquid clustering can be enabled only on an **unpartitioned** table, and
# MAGIC   `clusteringColumns` stays empty.
# MAGIC * The documentation states it in one line: "Clustering is not compatible with partitioning or ZORDER." The last
# MAGIC   cell shows the ZORDER half on the clustered table from section 2.
# MAGIC * The fix is not to choose one of the two columns for partitioning; it is option D, both filter columns as
# MAGIC   clustering keys on an unpartitioned table. Hinting `broadcast()` on the 10 MB `dim_product` in every query is
# MAGIC   also unnecessary, since it broadcasts automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: partition by `product_id` and `customer_id`, keep shuffle sort-merge joins
# MAGIC
# MAGIC The misconception: that partitioning the fact table on the join keys makes the dimension joins faster by
# MAGIC "pruning partitions". Joins on dimension foreign keys are served by broadcasting the small dimension, not by
# MAGIC partition pruning on the fact table.

# COMMAND ----------

# DBTITLE 1,Option B: the partitioned table (this write creates one directory per product-customer pair)
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_b_fact_orders
    PARTITIONED BY (product_id, customer_id)
    AS SELECT * FROM qfafe_orders_src""")
detail_b = spark.sql(f"DESCRIBE DETAIL {FQ}.qfafe_b_fact_orders").first()
pairs = spark.sql(f"SELECT count(DISTINCT product_id, customer_id) AS n FROM {FQ}.qfafe_b_fact_orders").first().n
print(f"partitions: {pairs} for 600 rows; files: {detail_b.numFiles}; average file size: {detail_b.sizeInBytes // max(detail_b.numFiles, 1)} bytes")
RESULTS["B"] = files_touched(f"{FQ}.qfafe_b_fact_orders")
print("analyst filter on order_date + product_category:", RESULTS["B"])

# COMMAND ----------

# DBTITLE 1,Option B: the same arithmetic at the question's scale
fact_bytes = 500 * 1024**3
for distinct_pairs in [1_000_000, 10_000_000]:
    print(f"{distinct_pairs:>12,d} product-customer pairs -> {distinct_pairs:,d} partition directories, "
          f"about {fact_bytes / distinct_pairs / 1024:,.0f} KB each (Delta aims for files near 1 GB)")

# COMMAND ----------

# DBTITLE 1,Option B: shuffle sort-merge joins, as the option prescribes
B_QUERY = analyst_query(f"{FQ}.qfafe_b_fact_orders", hints="/*+ MERGE(p), MERGE(c) */")
print("join plan for option B:")
RESULTS["B"]["joins"] = join_strategy(B_QUERY)
print("same answer as the source rows:", spark.sql(B_QUERY).collect() == EXPECTED)

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * `product_id` and `customer_id` are high-cardinality keys (potentially millions of distinct values each), so
# MAGIC   the table splits into one partition per pair: here roughly one partition and one file per row, at 500 GB
# MAGIC   millions of tiny partitions. Databricks documentation warns against exactly this: "Tables where a typical
# MAGIC   partition key could leave the table with too many or too few partitions" are not suited to partitioning.
# MAGIC * The analysts filter on `order_date` and `product_category`, which are not partition columns, so no partition
# MAGIC   is pruned; data skipping has to weigh the statistics of every tiny file (`rows per file` above is about 1).
# MAGIC * The joins: the plan shows hash-partitioning exchanges, a shuffle of the fact table for each sort-merge join.
# MAGIC   Joins on dimension foreign keys are served by broadcasting the small dimension tables, and keeping them as
# MAGIC   shuffle sort-merge joins wastes that advantage. The answer is the same; the cost is not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option C: partition by `product_id`, then `OPTIMIZE ... ZORDER BY (order_date, product_category)`
# MAGIC
# MAGIC The misconception: that Z-ordering inside a partitioned table recovers what the partitioning lost. The
# MAGIC broadcast hints are fine; the layout is the problem.

# COMMAND ----------

# DBTITLE 1,Option C: partition by product_id, Z-order, and look at what ZORDER can and cannot merge
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qfafe_c_fact_orders
    PARTITIONED BY (product_id)
    AS SELECT * FROM qfafe_orders_src""")
spark.sql(f"OPTIMIZE {FQ}.qfafe_c_fact_orders ZORDER BY (order_date, product_category)") \
     .select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
detail_c = spark.sql(f"DESCRIBE DETAIL {FQ}.qfafe_c_fact_orders").first()
print(f"partitions: {spark.table(f'{FQ}.qfafe_c_fact_orders').select('product_id').distinct().count()}; files after ZORDER: {detail_c.numFiles}")
RESULTS["C"] = files_touched(f"{FQ}.qfafe_c_fact_orders")
print("analyst filter on order_date + product_category:", RESULTS["C"])

# COMMAND ----------

# DBTITLE 1,Option C: the joins work, and the table cannot move to liquid clustering later
C_QUERY = analyst_query(f"{FQ}.qfafe_c_fact_orders", hints="/*+ BROADCAST(p), BROADCAST(c) */")
print("join plan for option C:")
RESULTS["C"]["joins"] = join_strategy(C_QUERY)
print("same answer as the source rows:", spark.sql(C_QUERY).collect() == EXPECTED)
print("\nconverting the partitioned table to liquid clustering:")
run(f"ALTER TABLE {FQ}.qfafe_c_fact_orders CLUSTER BY (order_date, product_category)")

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output:
# MAGIC
# MAGIC * Partitioning by high-cardinality `product_id` creates too many partitions for the same reason as B.
# MAGIC * `ZORDER` itself copes with high-cardinality columns, but it operates **within each partition's boundary**:
# MAGIC   after `OPTIMIZE` there is still at least one file per `product_id`, because ZORDER cannot combine files across
# MAGIC   product partitions. On this over-partitioned table, Z-ordering on `order_date` cannot build files that cover
# MAGIC   one date across all products: the category filter prunes the other categories' partitions, but a one-day
# MAGIC   query still opens a file in every `grocery` product partition, each spanning all 20 days.
# MAGIC * Databricks now recommends liquid clustering over ZORDER for new tables, and the last line shows the partitioned
# MAGIC   table cannot simply be switched to it: it would have to be rewritten.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The four options side by side
# MAGIC
# MAGIC Same 600 orders, same analyst query, the numbers each layout actually produced above.

# COMMAND ----------

# DBTITLE 1,Files, files read for the probe filter, rows per file, join strategy
cols = ["files", "files to read", "rows per file", "joins"]
print(f"{'option':8s}" + "".join(f"{c:>16s}" for c in cols))
for letter in "ABCD":
    print(f"{letter:8s}" + "".join(f"{str(RESULTS[letter].get(c, '')):>16s}" for c in cols))
print("\nOnly D creates the table, keeps files large and few, skips on both filter columns, and broadcasts both dimensions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rule to memorise
# MAGIC
# MAGIC For a large, growing fact table, put the analysts' filter columns in `CLUSTER BY` (liquid clustering) on an
# MAGIC unpartitioned table, and broadcast the small dimensions instead of shuffling the fact table. A dimension at or
# MAGIC below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default) broadcasts on its own; a larger one such as a
# MAGIC 200 MB `dim_customer` needs a raised threshold or a `broadcast()` hint. Liquid clustering is not compatible with
# MAGIC partitioning or ZORDER, so any option that mixes them fails (A) or locks the table out of clustering (C).
# MAGIC Never partition on high-cardinality join keys such as `product_id` or `customer_id` (B, C): it yields
# MAGIC millions of tiny partitions, and join keys are served by broadcast joins, not partition pruning.
