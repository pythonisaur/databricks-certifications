# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Audit inventory with information_schema
# MAGIC
# MAGIC Data Engineer Professional, section 8 of the exam guide, "Data Governance".
# MAGIC Objective: use system tables and information_schema to audit and inventory governed data assets.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works. The notebook creates three small commented tables (prefix `qd6c_`,
# MAGIC eight deterministic rows each) inside `dev_catalog.de_pro_qnotes`, grants `SELECT` on two of them to the
# MAGIC built-in `account users` group, and then answers the auditor's request with each option in turn. Every query is
# MAGIC live; options that point at things that do not exist (`dbfs:/system/audit/`, a `SYSTEM.GOVERNANCE` database)
# MAGIC are run to the real error the platform returns. Account-level system tables (`system.access.audit`) may not be
# MAGIC enabled in every workspace; that probe is read-only and prints its error instead of stopping the notebook.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A compliance auditor asks a data engineer to produce, from within Databricks SQL, a queryable inventory of (1)
# MAGIC every table in a catalog with its owner and comment, and (2) a record of who granted which privilege on those
# MAGIC tables. The engineer wants to use built-in Unity Catalog metadata surfaces rather than scraping the UI. Which
# MAGIC sources correctly provide this information?
# MAGIC
# MAGIC * **A** Run `SHOW TABLES` and `SHOW GRANTS` per schema and save the output; Unity Catalog returns privileges through `SHOW` commands, not through a relational view.
# MAGIC * **B** Run `DESCRIBE HISTORY` on each table; its Delta transaction log records owner changes, comment updates and `GRANT` statements as commit entries.
# MAGIC * **C** Query the catalog's `information_schema`: `tables` for names, owners and comments, and `table_privileges` for the grantor, grantee and privilege type.
# MAGIC * **D** Parse the JSON files under `dbfs:/system/audit/` with a schema-on-read DataFrame; Unity Catalog writes owners, comments and grants there as flat files.
# MAGIC * **E** Query the Hive metastore `SYSTEM.GOVERNANCE` database; its `tables` and `grants` views surface Unity Catalog owners, comments and privilege grants.
# MAGIC
# MAGIC The correct option (C) is built and run first; then A, B, D and E are each run against the same three tables
# MAGIC and judged by the same two requirements: (1) every table with its owner and comment, (2) who granted which
# MAGIC privilege, both as something you can query with SQL.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "d6c559b1b5",
    "track": "de_pro",
    "section": "Data Governance",
    "stem": "A compliance auditor asks a data engineer to produce, from within Databricks SQL, a queryable inventory of (1) every table in a catalog with its owner and comment, and (2) a record of who granted which privilege on those tables. The engineer wants to use built-in Unity Catalog metadata surfaces rather than scraping the UI. Which sources correctly provide this information?",
    "correct": "C",
    "options": {
        "A": "Run `SHOW TABLES` and `SHOW GRANTS` per schema and save the output; Unity Catalog returns privileges through `SHOW` commands, not through a relational view.",
        "B": "Run `DESCRIBE HISTORY` on each table; its Delta transaction log records owner changes, comment updates and `GRANT` statements as commit entries.",
        "C": "Query the catalog's `information_schema`: `tables` for names, owners and comments, and `table_privileges` for the grantor, grantee and privilege type.",
        "D": "Parse the JSON files under `dbfs:/system/audit/` with a schema-on-read DataFrame; Unity Catalog writes owners, comments and grants there as flat files.",
        "E": "Query the Hive metastore `SYSTEM.GOVERNANCE` database; its `tables` and `grants` views surface Unity Catalog owners, comments and privilege grants."
    },
    "objective": "Use system tables and information_schema to audit and inventory governed data assets.",
    "source": "testlogichub exams/fixes/qfix/d6c559b1b5.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and a helper that shows real failures
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("grantee", "account users")   # a built-in group that exists in every workspace
CATALOG = dbutils.widgets.get("catalog")
GRANTEE = dbutils.widgets.get("grantee")
SCHEMA = "de_pro_qnotes"
FQ = f"{CATALOG}.{SCHEMA}"
PREFIX = "qd6c_"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")
ME = spark.sql("SELECT current_user() AS u").first().u

def run(stmt: str):
    """Run one SQL statement; return the DataFrame, or print the real error (first line) and return None."""
    try:
        return spark.sql(stmt)
    except Exception as e:
        first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
        print(f"FAILED  {type(e).__name__}: {first[:300]}")
        return None

VERDICT = {}   # one row per option, compared in the last section
print("working schema:", FQ, "| you are:", ME, "| grantee:", GRANTEE)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE '{PREFIX}*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: three governed tables the auditor wants inventoried
# MAGIC
# MAGIC | table | comment | grant made below |
# MAGIC |---|---|---|
# MAGIC | `qd6c_orders` | raw order lines | `SELECT` to `account users` |
# MAGIC | `qd6c_customers` | customer master, contains e-mail | none (owner only) |
# MAGIC | `qd6c_daily_revenue` | gold revenue per day | `SELECT` to `account users` |
# MAGIC
# MAGIC The owner of each table is you (the creator). Rows come from `range(1, 9)` and `hash()`, so every run is
# MAGIC identical. The comments are set inside `CREATE TABLE ... COMMENT`, and the grants are ordinary `GRANT`
# MAGIC statements, exactly what a real catalog contains.

# COMMAND ----------

# DBTITLE 1,Build the three commented tables
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qd6c_orders
    COMMENT 'Raw order lines, one row per order'
    AS SELECT id AS order_id, (id % 4) + 1 AS customer_id,
              date_add(DATE'2026-11-01', CAST(id % 3 AS INT)) AS order_date,
              round((abs(hash(id, 'amt')) % 10000) / 100.0, 2) AS amount
       FROM range(1, 9)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qd6c_customers
    COMMENT 'Customer master; contains e-mail (PII)'
    AS SELECT id AS customer_id, concat('customer', id, '@example.com') AS email
       FROM range(1, 9)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qd6c_daily_revenue
    COMMENT 'Gold: revenue per order date'
    AS SELECT order_date, count(*) AS orders, round(sum(amount), 2) AS revenue
       FROM {FQ}.qd6c_orders GROUP BY order_date""")
TABLES = ["qd6c_customers", "qd6c_daily_revenue", "qd6c_orders"]
for t in TABLES:
    print(f"{t:22s} rows={spark.table(f'{FQ}.{t}').count()}")

# COMMAND ----------

# DBTITLE 1,Grant SELECT on two tables (the privileges the auditor asks about)
GRANTED = []
for t in ["qd6c_orders", "qd6c_daily_revenue"]:
    stmt = f"GRANT SELECT ON TABLE {FQ}.{t} TO `{GRANTEE}`"
    print(">>>", stmt)
    if run(stmt) is not None:
        GRANTED.append(t)
print("grants in place on:", GRANTED or "none (see the error above; the inventory still lists owners and comments)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option C, run first: query the catalog's `information_schema`
# MAGIC
# MAGIC Every Unity Catalog catalog contains a standard, read-only `information_schema` whose views are ordinary
# MAGIC relations. Two of them answer the auditor:
# MAGIC
# MAGIC * `information_schema.tables`: `table_catalog`, `table_schema`, `table_name`, **`table_owner`**, **`comment`**
# MAGIC   (plus `table_type`, `created`, `created_by`, `last_altered`, ...).
# MAGIC * `information_schema.table_privileges`: **`grantor`**, **`grantee`**, the table's three-part name,
# MAGIC   **`privilege_type`**, `is_grantable` and `inherited_from`.
# MAGIC
# MAGIC Because they are relations, the inventory is one SQL statement with ordinary joins and filters, runnable from a
# MAGIC Databricks SQL warehouse, schedulable as a Lakeflow Job, or saved as a view the auditor can query directly.

# COMMAND ----------

# DBTITLE 1,Option C, part 1: every table with its owner and comment
tables_df = spark.sql(f"""
    SELECT table_catalog, table_schema, table_name, table_type, table_owner, comment
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE '{PREFIX}%'
    ORDER BY table_name""")
tables_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Option C, part 2: who granted which privilege to whom
privs_df = spark.sql(f"""
    SELECT grantor, grantee, table_name, privilege_type, is_grantable, inherited_from
    FROM {CATALOG}.information_schema.table_privileges
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE '{PREFIX}%'
    ORDER BY table_name, grantee, privilege_type""")
privs_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Option C, part 3: the auditor's inventory as one query (and a view they can keep querying)
inventory_sql = f"""
    SELECT t.table_catalog, t.table_schema, t.table_name, t.table_owner, t.comment,
           p.grantor, p.grantee, p.privilege_type
    FROM {CATALOG}.information_schema.tables t
    LEFT JOIN {CATALOG}.information_schema.table_privileges p
      ON  p.table_catalog = t.table_catalog
      AND p.table_schema  = t.table_schema
      AND p.table_name    = t.table_name
    WHERE t.table_schema = '{SCHEMA}' AND t.table_name LIKE '{PREFIX}%'
    ORDER BY t.table_name, p.grantee"""
inventory = spark.sql(inventory_sql)
inventory.show(truncate=False)
spark.sql(f"CREATE OR REPLACE TEMP VIEW qd6c_audit_inventory AS {inventory_sql}")

n_tables = tables_df.count()
has_owner_comment = tables_df.filter("table_owner IS NOT NULL AND comment IS NOT NULL").count() == n_tables
has_grantor = "grantor" in privs_df.columns
VERDICT["C"] = dict(how="information_schema.tables JOIN table_privileges", statements=1,
                    owner_and_comment=has_owner_comment, grantor_grantee_privilege=has_grantor, queryable=True)
print(f"{n_tables} tables, owner+comment on all: {has_owner_comment}; "
      f"privilege rows: {privs_df.count()}; grantor column present: {has_grantor}")

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * Part 1 lists all three tables with `table_owner` (you) and the `comment` set at creation.
# MAGIC * Part 2 lists one row per grant with `grantor`, `grantee` (`account users`) and `privilege_type` (`SELECT`).
# MAGIC   The owner's implicit rights are not grants, so `qd6c_customers` has no row there, and the `LEFT JOIN` in part 3
# MAGIC   keeps it with nulls, which is exactly the "table nobody was granted" line an auditor wants to see.
# MAGIC * Part 3 is the deliverable: one statement, filterable and joinable like any table. `information_schema` is
# MAGIC   per catalog and shows only objects the querying user is allowed to see.
# MAGIC
# MAGIC For **cross-catalog auditing**, the account-level system tables complement it:
# MAGIC `system.information_schema.*` covers every catalog in the metastore, and `system.access.audit` records the
# MAGIC grant and ownership *events* over time (who changed permissions, and when). The next cell probes both
# MAGIC read-only; if system tables are not enabled for you, it prints the error and the lesson continues.

# COMMAND ----------

# DBTITLE 1,System tables: the cross-catalog complement (read-only probe)
for stmt in [
    f"""SELECT table_catalog, count(*) AS tables
        FROM system.information_schema.tables
        WHERE table_catalog = '{CATALOG}' GROUP BY table_catalog""",
    """SELECT event_time, user_identity.email AS actor, action_name, request_params
       FROM system.access.audit
       WHERE service_name = 'unityCatalog' AND action_name = 'updatePermissions'
         AND event_date >= current_date() - INTERVAL 1 DAY
       ORDER BY event_time DESC LIMIT 5""",
]:
    print(">>>", " ".join(stmt.split())[:110], "...")
    df = run(stmt)
    if df is not None:
        df.show(truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: `SHOW TABLES` and `SHOW GRANTS` per schema, saved by hand
# MAGIC
# MAGIC The misconception (from the WHY NOT): *"Unity Catalog returns privileges through `SHOW` commands, not through a
# MAGIC relational view."* `SHOW GRANTS` does work, but it is an **imperative command** whose output you collect table by
# MAGIC table; Unity Catalog also exposes the same grants as a queryable relation, `information_schema.table_privileges`,
# MAGIC which section 2 just queried. Run option A as written and count what it costs.

# COMMAND ----------

# DBTITLE 1,Option A: SHOW TABLES gives names only; owners and comments need another command per table
show_tables = spark.sql(f"SHOW TABLES IN {FQ} LIKE '{PREFIX}*'")
print("SHOW TABLES columns:", show_tables.columns)
show_tables.show(truncate=False)

statements = 1
collected = []
for t in sorted(r.tableName for r in show_tables.collect()):
    desc = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {FQ}.{t}").collect()}
    grants = spark.sql(f"SHOW GRANTS ON TABLE {FQ}.{t}")
    statements += 2
    rows = grants.collect()
    collected.append((t, desc.get("Owner"), desc.get("Comment"), [tuple(r) for r in rows]))
print("SHOW GRANTS columns:", grants.columns)
for c in collected:
    print(c)
print(f"\nstatements run for {len(collected)} tables: {statements}  (option C: 1)")

# COMMAND ----------

# DBTITLE 1,Option A: a SHOW command is not a relation, so it cannot be joined or filtered in SQL
run(f"SELECT * FROM (SHOW GRANTS ON TABLE {FQ}.qd6c_orders) WHERE ActionType = 'SELECT'")
VERDICT["A"] = dict(how="SHOW TABLES + DESCRIBE + SHOW GRANTS per table", statements=statements,
                    owner_and_comment="only via DESCRIBE per table", grantor_grantee_privilege="grantor" in [c.lower() for c in grants.columns],
                    queryable=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output:
# MAGIC
# MAGIC * `SHOW TABLES` returns names only; owner and comment need a `DESCRIBE TABLE EXTENDED` per table, and grants a
# MAGIC   `SHOW GRANTS` per table: `1 + 2 x tables` statements, growing with the catalog.
# MAGIC * The result is not a relation: wrapping `SHOW GRANTS` in a `SELECT` fails to parse, so the "inventory" ends up as
# MAGIC   a loop whose output is copied into a spreadsheet. Compare the `SHOW GRANTS` columns printed above with the
# MAGIC   `grantor` column of `table_privileges`.
# MAGIC * The option's premise is false: grants **are** exposed as queryable relations, via
# MAGIC   `information_schema.table_privileges` and system tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: `DESCRIBE HISTORY` on each table
# MAGIC
# MAGIC The misconception: *"the Delta transaction log records owner changes, comment updates and `GRANT` statements as
# MAGIC commit entries."* `DESCRIBE HISTORY` reads **one table's** `_delta_log`, which records **data-changing commits**
# MAGIC (writes, merges, schema and property changes). Ownership, comments-as-governance and grant history are Unity
# MAGIC Catalog metadata held in the metastore, not commits in the table's own log. We granted `SELECT` on `qd6c_orders`
# MAGIC in section 1; look for it.

# COMMAND ----------

# DBTITLE 1,Option B: the transaction log of qd6c_orders
hist = spark.sql(f"DESCRIBE HISTORY {FQ}.qd6c_orders")
hist.select("version", "timestamp", "userName", "operation", "operationParameters").show(truncate=60)
ops = [r.operation for r in hist.collect()]
grant_like = [o for o in ops if any(w in o.upper() for w in ("GRANT", "OWNER", "PRIVILEGE"))]
print("operations in the log:", ops)
print("commits that record a grant or an owner:", grant_like or "none")
print("grant on this table according to information_schema:",
      [tuple(r) for r in privs_df.filter("table_name = 'qd6c_orders'").select("grantor", "grantee", "privilege_type").collect()])
VERDICT["B"] = dict(how="DESCRIBE HISTORY per table", statements=len(TABLES), owner_and_comment=False,
                    grantor_grantee_privilege=bool(grant_like), queryable=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * The log shows the `CREATE OR REPLACE TABLE AS SELECT` commit and its committer (`userName`), never the
# MAGIC   `GRANT` we ran: the grant is visible in `table_privileges` and absent from the history.
# MAGIC * `userName` is who made a commit, not the table's owner; ownership can change without any commit.
# MAGIC * It is also one statement per table again, and each result describes a single table, not the catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: parse JSON files under `dbfs:/system/audit/`
# MAGIC
# MAGIC The misconception: *"Unity Catalog writes owners, comments and grants there as flat files."* There is no
# MAGIC `dbfs:/system/audit/` folder of governance files. Unity Catalog governance metadata is not stored as parseable
# MAGIC flat files under DBFS; it is exposed through `information_schema` and system tables (the audit *events* live in
# MAGIC the `system.access.audit` table probed in section 2). Run the option anyway.

# COMMAND ----------

# DBTITLE 1,Option D: list the folder, then the schema-on-read the option describes
try:
    print(dbutils.fs.ls("dbfs:/system/audit/"))
except Exception as e:
    first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
    print(f"FAILED  dbutils.fs.ls: {type(e).__name__}: {first[:300]}")
try:
    spark.read.json("dbfs:/system/audit/").show()
except Exception as e:
    first = next((l for l in str(e).splitlines() if l.strip()), repr(e))
    print(f"FAILED  spark.read.json: {type(e).__name__}: {first[:300]}")
VERDICT["D"] = dict(how="spark.read.json('dbfs:/system/audit/')", statements=1, owner_and_comment=False,
                    grantor_grantee_privilege=False, queryable=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong: both calls fail (the path does not exist, and on serverless the DBFS root may not even be
# MAGIC browsable). Even where a folder of logs exists (classic audit-log delivery to *your own* storage), that is a raw
# MAGIC event stream, not an inventory of current owners, comments and grants. The governed answer is a SQL query.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: a Hive metastore `SYSTEM.GOVERNANCE` database
# MAGIC
# MAGIC The misconception: *"its `tables` and `grants` views surface Unity Catalog owners, comments and privilege
# MAGIC grants."* There is no `SYSTEM.GOVERNANCE` database in the legacy Hive metastore, and the legacy metastore does
# MAGIC not hold Unity Catalog metadata at all. Unity Catalog exposes governance metadata via `information_schema` and
# MAGIC the `system` catalog's schemas, so list what the `system` catalog really contains, then run the option.

# COMMAND ----------

# DBTITLE 1,Option E: what the system catalog really has, then the option's query
df = run("SHOW SCHEMAS IN system")
if df is not None:
    print("schemas in the system catalog:", sorted(r[0] for r in df.collect()))
for stmt in ["SELECT * FROM system.governance.tables LIMIT 5",
             "SELECT * FROM hive_metastore.governance.grants LIMIT 5"]:
    print(">>>", stmt)
    df = run(stmt)
    if df is not None:
        df.show()
VERDICT["E"] = dict(how="SYSTEM.GOVERNANCE.tables / grants", statements=1, owner_and_comment=False,
                    grantor_grantee_privilege=False, queryable=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong, from the output: `system.governance` is not among the `system` catalog's schemas (you see
# MAGIC `information_schema`, `access`, `billing` and friends), and the `hive_metastore` query fails too (not found, or
# MAGIC the legacy metastore is disabled in this workspace). The legacy Hive metastore predates Unity Catalog and never
# MAGIC carried its owners, comments or grants.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The five options side by side

# COMMAND ----------

# DBTITLE 1,Which option delivers a queryable inventory of owners, comments and grants
print(f"{'opt':4s} {'statements':11s} {'owner+comment':28s} {'grantor/grantee/priv':21s} {'one SQL relation':17s} how")
for letter in ["C", "A", "B", "D", "E"]:
    v = VERDICT[letter]
    print(f"{letter:4s} {str(v['statements']):11s} {str(v['owner_and_comment']):28s} "
          f"{str(v['grantor_grantee_privilege']):21s} {str(v['queryable']):17s} {v['how']}")
print("\nOnly C answers both parts with ordinary SQL over relations Unity Catalog maintains for you.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC To inventory governed assets with SQL, query the catalog's `information_schema`: `tables` gives names,
# MAGIC `table_owner` and `comment`, and `table_privileges` gives `grantor`, `grantee` and `privilege_type`, so the audit
# MAGIC is one join, not a loop. `SHOW GRANTS` works but is an imperative, per-object command; `DESCRIBE HISTORY` is a
# MAGIC single table's Delta log of data-changing commits and never records grants or ownership. There is no
# MAGIC `dbfs:/system/audit/` folder and no Hive `SYSTEM.GOVERNANCE` database. For cross-catalog auditing and the history
# MAGIC of permission changes, add the account-level system tables (`system.information_schema`, `system.access.audit`).
