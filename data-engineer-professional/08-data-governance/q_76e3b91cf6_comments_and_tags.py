# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Searchable by concept: comments and tags in Unity Catalog
# MAGIC
# MAGIC Data Engineer Professional, section 8 of the exam guide, "Data Governance".
# MAGIC Objective: improve data discoverability by adding descriptions, comments, and tags to enterprise data assets.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the notebook builds three tiny tables per option (a bronze, a silver
# MAGIC and a gold table, eight rows each, derived from `range()` so every run is identical) and documents them the
# MAGIC way each option says. Everything lives in `dev_catalog.de_pro_qnotes`; nothing else is touched. The one step
# MAGIC Free Edition cannot run, publishing a Marketplace listing, appears as the annotated API call plus a local
# MAGIC stand-in.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data governance lead wants business users to find trustworthy tables by searching for business concepts
# MAGIC (e.g., 'PII', 'gold', 'customer 360') and to see human-readable descriptions of tables and columns directly in
# MAGIC the catalog. The solution must be queryable and enforceable through Unity Catalog metadata rather than an
# MAGIC external spreadsheet. Which combination of Unity Catalog features best supports discoverability at scale?
# MAGIC
# MAGIC * **A** Add `COMMENT` descriptions to catalogs, schemas, tables, and columns, and apply Unity Catalog key-value tags such as `pii=true` or `layer=gold` to tables and columns.
# MAGIC * **B** Record descriptions and classifications as `TBLPROPERTIES` key-value pairs on each table, and have business users find them with `SHOW TBLPROPERTIES` queries.
# MAGIC * **C** Keep descriptions and tags in a Delta `catalog_metadata` mapping table keyed by table and column name, and join it to `information_schema.tables` for search.
# MAGIC * **D** Encode concepts in table and column names, such as a `pii_` prefix on sensitive columns, and treat `COMMENT` text as display notes that search does not index.
# MAGIC * **E** Publish descriptions and tags as Databricks Marketplace listings for each internal table, and have business users search a private exchange by concept.
# MAGIC
# MAGIC The correct option (A) is built and run first; then B, C, D and E are each tried on the same three tables and
# MAGIC asked the same three search questions: where is the PII, which tables are gold, what is the customer 360 table.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "76e3b91cf6",
    "track": "de_pro",
    "section": "Data Governance",
    "stem": "A data governance lead wants business users to find trustworthy tables by searching for business concepts (e.g., 'PII', 'gold', 'customer 360') and to see human-readable descriptions of tables and columns directly in the catalog. The solution must be queryable and enforceable through Unity Catalog metadata rather than an external spreadsheet. Which combination of Unity Catalog features best supports discoverability at scale?",
    "correct": "A",
    "options": {
        "A": "Add `COMMENT` descriptions to catalogs, schemas, tables, and columns, and apply Unity Catalog key-value tags such as `pii=true` or `layer=gold` to tables and columns.",
        "B": "Record descriptions and classifications as `TBLPROPERTIES` key-value pairs on each table, and have business users find them with `SHOW TBLPROPERTIES` queries.",
        "C": "Keep descriptions and tags in a Delta `catalog_metadata` mapping table keyed by table and column name, and join it to `information_schema.tables` for search.",
        "D": "Encode concepts in table and column names, such as a `pii_` prefix on sensitive columns, and treat `COMMENT` text as display notes that search does not index.",
        "E": "Publish descriptions and tags as Databricks Marketplace listings for each internal table, and have business users search a private exchange by concept."
    },
    "objective": "Improve data discoverability by adding descriptions, comments, and tags to enterprise data assets.",
    "source": "testlogichub exams/fixes/qfix/76e3b91cf6.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and a helper that shows real failures
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

RESULTS = {}   # what each option could answer; filled in as we go, compared in the last section
print("working schema:", FQ)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'q76e_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: the same three tables for every option
# MAGIC
# MAGIC `build(prefix)` creates a medallion trio with a CTAS, which carries **no** comments and **no** tags:
# MAGIC
# MAGIC | table | layer | sensitive columns |
# MAGIC |---|---|---|
# MAGIC | `q76e_<x>_web_events` | bronze | `ip_address` |
# MAGIC | `q76e_<x>_orders` | silver | none |
# MAGIC | `q76e_<x>_customer_360` | gold | `email`, `phone` |
# MAGIC
# MAGIC Each option documents its own copy (`a`, `b`, `c`, `d`), so one option's metadata never leaks into another's
# MAGIC search results. The data is derived from `range(1, 9)` and `hash()`, so every run produces the same rows.

# COMMAND ----------

# DBTITLE 1,build(prefix): three undocumented tables
def build(prefix: str, email_col: str = "email", phone_col: str = "phone", gold_name: str = "customer_360") -> None:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FQ}.q76e_{prefix}_web_events AS
        SELECT id AS event_id,
               concat('10.0.', id % 4, '.', abs(hash(id)) % 250) AS ip_address,
               to_json(named_struct('page', concat('/p/', id % 3), 'ms', abs(hash(id, 'ms')) % 900)) AS payload
        FROM range(1, 9)""")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FQ}.q76e_{prefix}_orders AS
        SELECT id AS order_id, (id % 4) + 1 AS customer_id,
               round((abs(hash(id, 'amt')) % 10000) / 100.0, 2) AS amount
        FROM range(1, 9)""")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FQ}.q76e_{prefix}_{gold_name} AS
        SELECT id AS customer_id,
               concat('customer', id, '@example.com') AS {email_col},
               concat('+1-555-01', lpad(cast(id AS STRING), 2, '0')) AS {phone_col},
               round((abs(hash(id, 'ltv')) % 500000) / 100.0, 2) AS lifetime_value
        FROM range(1, 9)""")

build("a")
display(spark.table(f"{FQ}.q76e_a_customer_360"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option A, run first: `COMMENT` on every level plus key-value tags on tables and columns
# MAGIC
# MAGIC Two kinds of Unity Catalog metadata, each with its own job:
# MAGIC
# MAGIC * **Comments** are the human-readable description. Every securable level takes one: catalog, schema, table,
# MAGIC   column (and views, volumes, functions, models). Catalog Explorer shows them next to the object.
# MAGIC * **Tags** are key-value pairs (`pii=true`, `layer=gold`) that classify an object by business concept. They go
# MAGIC   on catalogs, schemas, tables, views, volumes and individual **columns**.
# MAGIC
# MAGIC The catalog-level comment is the statement below; it is shown, not run, because `dev_catalog` is shared and
# MAGIC this lesson changes nothing outside its own schema:
# MAGIC
# MAGIC ```sql
# MAGIC COMMENT ON CATALOG dev_catalog IS 'Development lakehouse for the data platform team';
# MAGIC ALTER CATALOG dev_catalog SET TAGS ('environment' = 'dev');
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Option A: comments on the schema, the tables and the columns
A_COMMENTS = [
    f"COMMENT ON SCHEMA {FQ} IS 'Sandbox for exam-question notebooks; one table set per lesson'",
    f"COMMENT ON TABLE {FQ}.q76e_a_web_events IS 'Raw clickstream events as received from the web tier. Not deduplicated.'",
    f"COMMENT ON TABLE {FQ}.q76e_a_orders IS 'Cleaned orders, one row per order, amounts in USD.'",
    f"COMMENT ON TABLE {FQ}.q76e_a_customer_360 IS 'Customer 360: one trusted row per customer with contact details and lifetime value. Certified for reporting.'",
    f"ALTER TABLE {FQ}.q76e_a_web_events ALTER COLUMN ip_address COMMENT 'Client IP address of the request'",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 ALTER COLUMN email COMMENT 'Primary email address of the customer'",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 ALTER COLUMN phone COMMENT 'Primary phone number, E.164 style'",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 ALTER COLUMN lifetime_value COMMENT 'Sum of all completed order amounts in USD'",
]
for stmt in A_COMMENTS:
    run(stmt)
print(f"{len(A_COMMENTS)} comment statements run")

# COMMAND ----------

# DBTITLE 1,Option A: key-value tags on the schema, the tables and the sensitive columns
A_TAGS = [
    f"ALTER SCHEMA {FQ} SET TAGS ('owner_team' = 'data_platform')",
    f"ALTER TABLE {FQ}.q76e_a_web_events SET TAGS ('layer' = 'bronze')",
    f"ALTER TABLE {FQ}.q76e_a_orders SET TAGS ('layer' = 'silver')",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 SET TAGS ('layer' = 'gold', 'domain' = 'customer_360', 'certified' = 'true')",
    f"ALTER TABLE {FQ}.q76e_a_web_events ALTER COLUMN ip_address SET TAGS ('pii' = 'true')",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 ALTER COLUMN email SET TAGS ('pii' = 'true')",
    f"ALTER TABLE {FQ}.q76e_a_customer_360 ALTER COLUMN phone SET TAGS ('pii' = 'true')",
]
for stmt in A_TAGS:
    run(stmt)
print(f"{len(A_TAGS)} tag statements run")

# COMMAND ----------

# DBTITLE 1,Option A: the three business searches, each one SQL query over information_schema
pii = run(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.column_tags
    WHERE schema_name = '{SCHEMA}' AND table_name LIKE 'q76e_a_%'
      AND tag_name = 'pii' AND tag_value = 'true'
    ORDER BY table_name, column_name""")
gold = run(f"""
    SELECT table_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.table_tags
    WHERE schema_name = '{SCHEMA}' AND table_name LIKE 'q76e_a_%'
      AND tag_name = 'layer' AND tag_value = 'gold'""")
c360 = run(f"""
    SELECT table_name, comment
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'q76e_a_%'
      AND lower(comment) LIKE '%customer 360%'""")
for label, df in [("PII columns", pii), ("gold tables", gold), ("'customer 360' in a description", c360)]:
    print(f"--- {label}")
    if df is not None:
        df.show(truncate=False)
RESULTS["A"] = {
    "pii columns found": pii.count() if pii is not None else 0,
    "gold tables found": gold.count() if gold is not None else 0,
    "customer 360 found": c360.count() if c360 is not None else 0,
}

# COMMAND ----------

# DBTITLE 1,Option A: the descriptions sit on the objects themselves
display(run(f"""
    SELECT table_name, column_name, data_type, comment
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{SCHEMA}' AND table_name = 'q76e_a_customer_360'
    ORDER BY ordinal_position"""))
display(run(f"""
    SELECT schema_name, comment FROM {CATALOG}.information_schema.schemata WHERE schema_name = '{SCHEMA}'"""))

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * Every search is **one query** over Unity Catalog's own metadata: the three PII columns across two tables come
# MAGIC   from `information_schema.column_tags`, the gold table from `information_schema.table_tags`, and the customer
# MAGIC   360 table from the `comment` column of `information_schema.tables`. Swap `{catalog}.information_schema` for
# MAGIC   `system.information_schema` and the same query sweeps every catalog in the metastore.
# MAGIC * The descriptions are attached to the schema, the table and each column, so a business user opening the table
# MAGIC   in Catalog Explorer reads them in place, and the workspace search box matches on names, comments and tags
# MAGIC   (search for `pii` or `customer 360`). The catalog level takes the same `COMMENT ON CATALOG` shown above.
# MAGIC * Tags work at the **column** level, which is where PII actually lives. Because they are Unity Catalog metadata
# MAGIC   they are also what governance builds on: governed tags restrict who may assign a key and which values are
# MAGIC   allowed, and attribute-based access control policies can match on a tag such as `pii=true` to mask the
# MAGIC   column. That is the "queryable and enforceable" part of the stem.
# MAGIC
# MAGIC Comments for humans, tags for classification, both native to Unity Catalog: that is option A, and it scales
# MAGIC because adding a table means two statements, not a spreadsheet edit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option B: `TBLPROPERTIES` and `SHOW TBLPROPERTIES`
# MAGIC
# MAGIC The misconception: that table properties are Unity Catalog's metadata layer for discovery (and that UC has no
# MAGIC tags). Same three tables, documented only with `TBLPROPERTIES`.

# COMMAND ----------

# DBTITLE 1,Option B: the table-level properties work
build("b")
run(f"ALTER TABLE {FQ}.q76e_b_web_events SET TBLPROPERTIES ('description' = 'Raw clickstream events', 'classification' = 'pii', 'layer' = 'bronze')")
run(f"ALTER TABLE {FQ}.q76e_b_orders SET TBLPROPERTIES ('description' = 'Cleaned orders', 'layer' = 'silver')")
run(f"ALTER TABLE {FQ}.q76e_b_customer_360 SET TBLPROPERTIES ('description' = 'Customer 360, one row per customer', 'classification' = 'pii', 'layer' = 'gold')")
display(spark.sql(f"SHOW TBLPROPERTIES {FQ}.q76e_b_customer_360"))

# COMMAND ----------

# DBTITLE 1,Option B: a column cannot carry a table property
run(f"ALTER TABLE {FQ}.q76e_b_customer_360 ALTER COLUMN email SET TBLPROPERTIES ('pii' = 'true')")

# COMMAND ----------

# DBTITLE 1,Option B: searching means one SHOW TBLPROPERTIES per table, and the catalog sees nothing
hits = []
tables_b = [r.tableName for r in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'q76e_b_*'").collect()]
for t in tables_b:   # one round trip per table: fine for 3, not for 30,000
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {FQ}.{t}").collect()}
    if props.get("classification") == "pii":
        hits.append((t, "whole table", {k: v for k, v in props.items() if not k.startswith("delta.")}))
print(f"SHOW TBLPROPERTIES queries run: {len(tables_b)}")
for h in hits:
    print("pii:", h)

seen = spark.sql(f"""
    SELECT (SELECT count(*) FROM {CATALOG}.information_schema.table_tags
            WHERE schema_name = '{SCHEMA}' AND table_name LIKE 'q76e_b_%') AS table_tags,
           (SELECT count(*) FROM {CATALOG}.information_schema.tables
            WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'q76e_b_%' AND comment IS NOT NULL) AS tables_with_comment""")
seen.show()
RESULTS["B"] = {"pii columns found": 0, "gold tables found": "per-table loop",
                "customer 360 found": "per-table loop"}

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * Table properties are **table-level configuration**. They sit next to Delta settings such as
# MAGIC   `delta.minReaderVersion` in the same key-value bag, and the column-level attempt fails with a parse error: a
# MAGIC   property cannot describe or classify an individual column, so the best B can say is "this whole table has PII".
# MAGIC * They are not what catalog search uses: `information_schema` shows zero tags and zero comments for the `b`
# MAGIC   tables, and finding PII took one `SHOW TBLPROPERTIES` per table, a loop a business user will not write.
# MAGIC * The premise is false: tags **are** supported on Unity Catalog securables (section 2 set them), and limiting
# MAGIC   metadata to `TBLPROPERTIES` throws away both comments and column-level tagging.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: a Delta `catalog_metadata` mapping table joined to `information_schema.tables`
# MAGIC
# MAGIC The misconception: that a governance table you maintain yourself counts as Unity Catalog metadata because it
# MAGIC happens to be a Delta table.

# COMMAND ----------

# DBTITLE 1,Option C: the mapping table and the join it promises
build("c")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.q76e_c_catalog_metadata AS
    SELECT * FROM VALUES
      ('q76e_c_web_events',   NULL,         'Raw clickstream events',              'layer', 'bronze'),
      ('q76e_c_web_events',   'ip_address', 'Client IP address',                   'pii',   'true'),
      ('q76e_c_orders',       NULL,         'Cleaned orders, amounts in USD',      'layer', 'silver'),
      ('q76e_c_customer_360', NULL,         'Customer 360, one row per customer',  'layer', 'gold'),
      ('q76e_c_customer_360', 'email',      'Primary email address',               'pii',   'true'),
      ('q76e_c_customer_360', 'phone',      'Primary phone number',                'pii',   'true')
    AS m(table_name, column_name, description, tag_key, tag_value)""")
SEARCH_C = f"""
    SELECT t.table_name, m.column_name, m.description, m.tag_key, m.tag_value
    FROM {CATALOG}.information_schema.tables t
    LEFT JOIN {FQ}.q76e_c_catalog_metadata m ON m.table_name = t.table_name
    WHERE t.table_schema = '{SCHEMA}' AND t.table_name LIKE 'q76e_c_%' AND t.table_name <> 'q76e_c_catalog_metadata'
    ORDER BY t.table_name, m.column_name"""
display(spark.sql(SEARCH_C))

# COMMAND ----------

# DBTITLE 1,Option C: a month later, someone renames a table and adds a new one
run(f"ALTER TABLE {FQ}.q76e_c_orders RENAME TO {FQ}.q76e_c_orders_v2")
run(f"CREATE OR REPLACE TABLE {FQ}.q76e_c_customer_360_eu AS SELECT * FROM {FQ}.q76e_c_customer_360")   # nobody registered it
display(spark.sql(SEARCH_C))
print("orphaned mapping rows (point at tables that no longer exist):")
display(spark.sql(f"""
    SELECT m.* FROM {FQ}.q76e_c_catalog_metadata m
    LEFT ANTI JOIN {CATALOG}.information_schema.tables t
      ON t.table_schema = '{SCHEMA}' AND t.table_name = m.table_name"""))
print("what Catalog Explorer shows for the gold table: comment =",
      spark.sql(f"SELECT comment FROM {CATALOG}.information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'q76e_c_customer_360'").first().comment)
RESULTS["C"] = {"pii columns found": "only if the mapping is current", "gold tables found": "only if the mapping is current",
                "customer 360 found": "misses the new table"}

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output:
# MAGIC
# MAGIC * It is the **external-spreadsheet pattern the stem rules out**, just stored in Delta. Nothing ties a mapping
# MAGIC   row to the object: after one rename and one new table, the search returns a gold table with no metadata
# MAGIC   (`q76e_c_customer_360_eu`), a renamed table with no metadata (`q76e_c_orders_v2`), and orphaned rows for a
# MAGIC   table that no longer exists.
# MAGIC * Catalog Explorer still shows `comment = None` for the gold table: the descriptions never reach the catalog,
# MAGIC   so users only see them if they know about the side table and write the join.
# MAGIC * Unity Catalog can store descriptions and tags **natively** (section 2), so the hand-maintained mapping is
# MAGIC   unnecessary, cannot be enforced by any policy, and is not the recommended pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: concepts in the names, comments treated as display-only
# MAGIC
# MAGIC The misconception: that comments are not searchable, so naming conventions are the only way to make a concept
# MAGIC findable.

# COMMAND ----------

# DBTITLE 1,Option D: pii_ prefixes and a gold_ table name, then a search on names
build("d", email_col="pii_email", phone_col="pii_phone", gold_name="gold_customer_360")
display(spark.sql(f"""
    SELECT table_name, column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'q76e_d_%' AND column_name LIKE 'pii\\_%'
    ORDER BY table_name, column_name"""))

# COMMAND ----------

# DBTITLE 1,Option D: the false premise, comments ARE queryable metadata
display(spark.sql(f"""
    SELECT table_name, column_name, comment
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{SCHEMA}' AND lower(comment) LIKE '%email%'"""))

# COMMAND ----------

# DBTITLE 1,Option D: renaming a column to encode a concept breaks downstream code
# A dashboard query written against the original column name:
run(f"SELECT customer_id, email FROM {FQ}.q76e_d_gold_customer_360")
# And the name prefix missed the bronze IP address column entirely, because nobody renamed it:
print("ip_address flagged by the naming rule:",
      spark.sql(f"SELECT count(*) AS n FROM {CATALOG}.information_schema.columns WHERE table_schema = '{SCHEMA}' AND table_name = 'q76e_d_web_events' AND column_name LIKE 'pii%'").first().n > 0)
RESULTS["D"] = {"pii columns found": 2, "gold tables found": "name match only", "customer 360 found": "name match only"}

# COMMAND ----------

# MAGIC %md
# MAGIC Why D is wrong, from the output:
# MAGIC
# MAGIC * Comments and tags are **queryable metadata, not display notes**: the comment search returns the `email`
# MAGIC   column documented in section 2. So names are not the only searchable option.
# MAGIC * Names are far less flexible: one name holds one concept, there is no key-value structure (`layer=gold` vs
# MAGIC   `layer=silver`), no description, and the bronze `ip_address` column was simply never flagged.
# MAGIC * Renaming columns to encode concepts **breaks downstream code**: the dashboard query fails with an unresolved
# MAGIC   column error because `email` is now `pii_email`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: Marketplace listings for internal tables
# MAGIC
# MAGIC The misconception: that a Databricks Marketplace or private exchange listing is where internal tables get
# MAGIC documented. Creating a listing is a provider-console or REST operation that Free Edition cannot run, so the
# MAGIC exact call is annotated below and a dict stands in for its response.

# COMMAND ----------

# DBTITLE 1,Option E: the listing API call (not run) and a local stand-in for what it creates
# Not run: Marketplace provider API. A listing advertises a Delta Sharing SHARE to consumers in OTHER
# organizations or metastores (public Marketplace) or to the members of a private exchange.
#
#   POST /api/2.0/marketplace-provider/listing
#   {"listing": {"summary": {"name": "Customer 360", "listingType": "STANDARD", "share": {"name": "customer_share"},
#                            "exchange_ids": ["<private-exchange-id>"]},
#                "detail": {"description": "One row per customer"}}}
LISTING_STANDIN = {
    "listing": {
        "summary": {"name": "Customer 360", "share": {"name": "customer_share"}, "exchange_ids": ["private-exchange-1"]},
        "detail": {"description": "One row per customer"},
    },
    "visible_to": "consumers of the exchange, after they install it into their own catalog",
}
print("what the listing points at :", LISTING_STANDIN["listing"]["summary"]["share"])
print("who searches it            :", LISTING_STANDIN["visible_to"])
e_comment = spark.sql(f"SELECT comment FROM {CATALOG}.information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = 'q76e_a_customer_360'").first().comment
print("what an internal user sees on the table itself comes from its UC comment, set in section 2:\n  ", e_comment)
RESULTS["E"] = {"pii columns found": "not applicable", "gold tables found": "not applicable", "customer 360 found": "not applicable"}

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong:
# MAGIC
# MAGIC * Databricks Marketplace and private exchanges **share data products with other organizations or workspaces**
# MAGIC   through Delta Sharing. A listing describes a share for its consumers; it is not how internal tables are
# MAGIC   documented, and it carries no column-level tags.
# MAGIC * Discoverability metadata (comments, tags) applies to **internal** Unity Catalog objects and is not exclusive to
# MAGIC   Marketplace: the description an internal user sees on the table comes from its `COMMENT`, as the last line of
# MAGIC   the cell shows. Publishing one listing per internal table would also expose them as shares, a governance
# MAGIC   problem rather than a discovery feature.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The five options side by side
# MAGIC
# MAGIC The same three business searches, answered by what each option actually produced above.

# COMMAND ----------

# DBTITLE 1,Who answered the three searches, and how
cols = ["pii columns found", "gold tables found", "customer 360 found"]
print(f"{'option':7s}" + "".join(f"{c:>34s}" for c in cols))
for letter in "ABCDE":
    print(f"{letter:7s}" + "".join(f"{str(RESULTS[letter][c]):>34s}" for c in cols))
print("\nOnly A answers all three with one native Unity Catalog query each, at column level, with descriptions in place.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC Discoverability in Unity Catalog is two native features: `COMMENT` for the human-readable description (on
# MAGIC catalogs, schemas, tables and columns) and key-value tags such as `pii=true` or `layer=gold` for classification
# MAGIC (on tables and columns too). Both live in Unity Catalog, show in Catalog Explorer, are matched by search and can
# MAGIC be queried through `information_schema` (`column_tags`, `table_tags`, the `comment` columns) and system tables,
# MAGIC which is what makes them work at scale. `TBLPROPERTIES` is table-level configuration, a mapping table is a
# MAGIC spreadsheet in Delta, name prefixes are brittle and break code, and Marketplace is for sharing data outside,
# MAGIC not for documenting it inside.
