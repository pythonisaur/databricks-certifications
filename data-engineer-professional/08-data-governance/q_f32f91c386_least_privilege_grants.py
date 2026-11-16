# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Least-privilege grants across medallion schemas
# MAGIC
# MAGIC Data Engineer Professional, section 8 of the exam guide, "Data Governance".
# MAGIC Objective: apply the principle of least privilege when granting access across Unity Catalog groups.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works. The notebook builds a miniature `sales` catalog as five tiny tables
# MAGIC (one per schema: `bronze`, `silver`, `gold` and two "other" schemas, eight deterministic rows each) inside
# MAGIC `dev_catalog.de_pro_qnotes`; nothing else is created or changed. Service principals and account groups are
# MAGIC account-level identities that Free Edition cannot create, so every option's exact `GRANT` / `REVOKE` / `OWNER TO`
# MAGIC SQL is shown and then executed against a small local model of the Unity Catalog privilege rules (inheritance,
# MAGIC traversal, ownership, group membership). Reads that the model allows return the real rows of the sandbox tables;
# MAGIC reads it refuses print the error Unity Catalog raises. The only live platform calls are read-only
# MAGIC (`SHOW GRANTS`, `DESCRIBE SCHEMA EXTENDED`).
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A data engineer must let a BI service principal read only the Gold-layer schema `sales.gold`, while a separate
# MAGIC data science group needs read access to `sales.gold` and `sales.silver` but must never see the raw `sales.bronze`
# MAGIC schema. The catalog `sales` also contains other schemas that neither principal should access. Following least
# MAGIC privilege, which grant strategy is correct?
# MAGIC
# MAGIC * **A** Grant `SELECT` on `sales.gold` to the BI service principal and `SELECT` on `sales.gold` and `sales.silver` to the data science group, with no `USE CATALOG` or `USE SCHEMA` grant on the catalog or schemas.
# MAGIC * **B** Grant `USE CATALOG` and `SELECT` on the `sales` catalog to both principals; then revoke `SELECT` on `sales.bronze` from the data science group and on `sales.silver` from the BI principal.
# MAGIC * **C** Grant `USE CATALOG` on `sales` to both principals; make the BI service principal the owner of `sales.gold` and the data science group the owner of `sales.silver` to give them read access.
# MAGIC * **D** Grant `USE CATALOG` on `sales` to both principals; grant `USE SCHEMA` and `SELECT` on `sales.gold` to the BI service principal, and on `sales.gold` and `sales.silver` to the data science group.
# MAGIC * **E** Add both principals to one new group; grant that group `USE CATALOG` on `sales`, and `USE SCHEMA` and `SELECT` on `sales.gold` and `sales.silver`, with no grants to either principal directly.
# MAGIC
# MAGIC The correct option (D) is built and run first; then A, B, C and E are each applied to a fresh copy of the same
# MAGIC catalog and audited with the same two requirements: the BI principal reads `gold` only, the data science group
# MAGIC reads `gold` and `silver` only.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "f32f91c386",
    "track": "de_pro",
    "section": "Data Governance",
    "stem": "A data engineer must let a BI service principal read only the Gold-layer schema `sales.gold`, while a separate data science group needs read access to `sales.gold` and `sales.silver` but must never see the raw `sales.bronze` schema. The catalog `sales` also contains other schemas that neither principal should access. Following least privilege, which grant strategy is correct?",
    "correct": "D",
    "options": {
        "A": "Grant `SELECT` on `sales.gold` to the BI service principal and `SELECT` on `sales.gold` and `sales.silver` to the data science group, with no `USE CATALOG` or `USE SCHEMA` grant on the catalog or schemas.",
        "B": "Grant `USE CATALOG` and `SELECT` on the `sales` catalog to both principals; then revoke `SELECT` on `sales.bronze` from the data science group and on `sales.silver` from the BI principal.",
        "C": "Grant `USE CATALOG` on `sales` to both principals; make the BI service principal the owner of `sales.gold` and the data science group the owner of `sales.silver` to give them read access.",
        "D": "Grant `USE CATALOG` on `sales` to both principals; grant `USE SCHEMA` and `SELECT` on `sales.gold` to the BI service principal, and on `sales.gold` and `sales.silver` to the data science group.",
        "E": "Add both principals to one new group; grant that group `USE CATALOG` on `sales`, and `USE SCHEMA` and `SELECT` on `sales.gold` and `sales.silver`, with no grants to either principal directly."
    },
    "objective": "Apply the principle of least privilege when granting access across Unity Catalog groups.",
    "source": "testlogichub exams/fixes/qfix/f32f91c386.py"
}

# COMMAND ----------

# DBTITLE 1,Widgets, the sandbox schema and a helper that shows real failures
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("bi_principal", "bi-reporting-sp")   # in a real workspace: the service principal's application ID
dbutils.widgets.text("ds_group", "data-science")
CATALOG = dbutils.widgets.get("catalog")
BI = dbutils.widgets.get("bi_principal")
DS = dbutils.widgets.get("ds_group")
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

RESULTS = {}   # one audit per option, compared in the last section
print("working schema:", FQ, "| BI principal:", BI, "| data science group:", DS)

# COMMAND ----------

# DBTITLE 1,Reset: drop only this lesson's own tables so the notebook can be re-run
for t in spark.sql(f"SHOW TABLES IN {FQ} LIKE 'qf32_*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t.tableName}")
print("clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: the `sales` catalog in miniature
# MAGIC
# MAGIC | securable in the question | sandbox table standing for it | who may read it |
# MAGIC |---|---|---|
# MAGIC | `sales.bronze.raw_orders` | `qf32_bronze_raw_orders` (raw, with customer e-mail) | nobody |
# MAGIC | `sales.silver.orders` | `qf32_silver_orders` | data science group |
# MAGIC | `sales.gold.daily_revenue` | `qf32_gold_daily_revenue` | BI principal and data science group |
# MAGIC | `sales.finance.ledger` | `qf32_finance_ledger` | nobody (an "other schema") |
# MAGIC | `sales.hr_staging.salaries` | `qf32_hr_staging_salaries` | nobody (an "other schema") |
# MAGIC
# MAGIC Rows come from `range(1, 9)` and `hash()`, so every run is identical.

# COMMAND ----------

# DBTITLE 1,Build the five sandbox tables
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qf32_bronze_raw_orders AS
    SELECT id AS ingest_id, concat('customer', id % 4 + 1, '@example.com') AS customer_email,
           to_json(named_struct('order_id', id, 'amount_cents', abs(hash(id, 'amt')) % 10000)) AS raw_payload
    FROM range(1, 9)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qf32_silver_orders AS
    SELECT id AS order_id, (id % 4) + 1 AS customer_id,
           date_add(DATE'2026-11-01', CAST(id % 3 AS INT)) AS order_date,
           round((abs(hash(id, 'amt')) % 10000) / 100.0, 2) AS amount
    FROM range(1, 9)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qf32_gold_daily_revenue AS
    SELECT order_date, count(*) AS orders, round(sum(amount), 2) AS revenue
    FROM {FQ}.qf32_silver_orders GROUP BY order_date""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qf32_finance_ledger AS
    SELECT id AS entry_id, concat('4', lpad(CAST(id % 5 AS STRING), 3, '0')) AS account,
           round((abs(hash(id, 'gl')) % 90000) / 100.0, 2) AS amount
    FROM range(1, 9)""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.qf32_hr_staging_salaries AS
    SELECT id AS employee_id, 50000 + abs(hash(id, 'sal')) % 70000 AS salary
    FROM range(1, 9)""")

TABLES = {   # securable in the question -> sandbox table
    "sales.bronze.raw_orders": f"{FQ}.qf32_bronze_raw_orders",
    "sales.silver.orders": f"{FQ}.qf32_silver_orders",
    "sales.gold.daily_revenue": f"{FQ}.qf32_gold_daily_revenue",
    "sales.finance.ledger": f"{FQ}.qf32_finance_ledger",
    "sales.hr_staging.salaries": f"{FQ}.qf32_hr_staging_salaries",
}
SCHEMAS = [t.rsplit(".", 1)[0] for t in TABLES]
for name, table in TABLES.items():
    print(f"{name:28s} -> {table:45s} rows={spark.table(table).count()}")
spark.table(TABLES["sales.gold.daily_revenue"]).orderBy("order_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### The live, read-only part: how you inspect grants on a real schema
# MAGIC
# MAGIC In your own workspace these are the statements you would run against `sales.gold` to check any option below.
# MAGIC Here they run against the sandbox schema, which is safe because they only read. `SHOW GRANTS` lists the direct
# MAGIC grants on the object; `DESCRIBE SCHEMA EXTENDED` shows its owner.

# COMMAND ----------

# DBTITLE 1,Live: SHOW GRANTS and the owner of the sandbox schema
print("current user:", spark.sql("SELECT current_user() AS u").first().u)
for stmt in [f"SHOW GRANTS ON SCHEMA {FQ}", f"DESCRIBE SCHEMA EXTENDED {FQ}"]:
    print(">>>", stmt)
    df = run(stmt)
    if df is not None:
        df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The stand-in: the Unity Catalog privilege rules as a small model
# MAGIC
# MAGIC `UCModel` executes the same `GRANT`, `REVOKE` and `ALTER ... OWNER TO` statements you would run, and applies
# MAGIC the rules the exam tests:
# MAGIC
# MAGIC 1. **Traversal.** Reading `sales.gold.daily_revenue` needs `USE CATALOG` on `sales`, `USE SCHEMA` on
# MAGIC    `sales.gold` **and** `SELECT` on the table. Missing any one of them fails the query.
# MAGIC 2. **Inheritance.** A privilege granted on a catalog or schema applies to everything below it, including
# MAGIC    schemas and tables created later.
# MAGIC 3. **No deny.** Unity Catalog has no deny rule. A `REVOKE` removes only a grant made on that exact object to
# MAGIC    that exact principal; it cannot carve an exception out of a privilege inherited from the parent.
# MAGIC 4. **Ownership.** The owner of an object holds every privilege on it and on its children and can grant, alter
# MAGIC    and remove them; non-owners without `MANAGE` cannot grant anything.
# MAGIC 5. **Groups.** A principal holds every privilege granted to any group it belongs to.
# MAGIC
# MAGIC Allowed reads return the real sandbox rows; refused reads print the error text Unity Catalog uses.

# COMMAND ----------

# DBTITLE 1,UCModel: the privilege rules, executed
import re

GRANT_RE = re.compile(r"^(GRANT|REVOKE) (.+?) ON (CATALOG|SCHEMA|TABLE) (\S+) (?:TO|FROM) `([^`]+)`$", re.I)
OWNER_RE = re.compile(r"^ALTER (CATALOG|SCHEMA|TABLE) (\S+) OWNER TO `([^`]+)`$", re.I)
KIND = {1: "Catalog", 2: "Schema", 3: "Table"}

def ancestors(obj: str):
    parts = obj.split(".")
    return [".".join(parts[:i]) for i in range(len(parts), 0, -1)]   # table, schema, catalog

class UCModel:
    def __init__(self, groups=None):
        self.grants = set()                                   # (principal, privilege, securable)
        self.owners = {o: "admins" for t in TABLES for o in ancestors(t)}
        self.groups = {g: set(m) for g, m in (groups or {}).items()}

    def identities(self, p):
        return {p} | {g for g, members in self.groups.items() if p in members}

    def owns(self, p, obj):
        return any(self.owners.get(n) in self.identities(p) for n in ancestors(obj))

    def execute(self, sql: str, actor: str = "admins"):
        sql = " ".join(sql.split())
        m = OWNER_RE.match(sql)
        if m:
            _, obj, new_owner = m.groups()
            if not self.owns(actor, obj):
                print(f"  DENIED  {sql}\n          [INSUFFICIENT_PERMISSIONS] {actor} is not the owner of '{obj}'.")
                return False
            self.owners[obj] = new_owner
            print(f"  ok      {sql}")
            return True
        m = GRANT_RE.match(sql)
        if not m:
            raise ValueError(f"UCModel does not parse: {sql}")
        verb, privs, _, obj, who = m.groups()
        if not self.owns(actor, obj):
            print(f"  DENIED  {sql}\n          [INSUFFICIENT_PERMISSIONS] {actor} does not have MANAGE on '{obj}'.")
            return False
        removed = 0
        for priv in [p.strip().upper() for p in privs.split(",")]:
            if verb.upper() == "GRANT":
                self.grants.add((who, priv, obj))
            elif (who, priv, obj) in self.grants:
                self.grants.discard((who, priv, obj))
                removed += 1
        note = f"   -- removed {removed} direct grant(s)" if verb.upper() == "REVOKE" else ""
        print(f"  ok      {sql}{note}")
        return True

    def source(self, p, priv, obj):
        """Why p holds priv on obj (the grant or ownership it comes from), or None."""
        ids = self.identities(p)
        for node in ancestors(obj):
            if self.owners.get(node) in ids:
                return f"owner of {node}"
            for i in sorted(ids):
                if (i, priv, node) in self.grants:
                    return f"{priv} on {node}" + ("" if i == p else f" via group {i}")
        return None

    def read(self, p, table: str):
        catalog, schema = ancestors(table)[2], ancestors(table)[1]
        for priv, node in [("USE CATALOG", catalog), ("USE SCHEMA", schema), ("SELECT", table)]:
            if not self.source(p, priv, node):
                kind = KIND[len(node.split("."))]
                return None, (f"[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have "
                              f"{priv} on {kind} '{node}'.")
        return spark.table(TABLES[table]), "ok"

print("model ready; securables:", sorted({a for t in TABLES for a in ancestors(t)}))

# COMMAND ----------

# DBTITLE 1,audit(): check one option against the two requirements
WANT = {BI: {"sales.gold"}, DS: {"sales.gold", "sales.silver"}}

def audit(label: str, model: UCModel, show_rows: bool = False):
    rows, verdict = [], {"missing": [], "over_read": [], "latent_select": [], "owns": []}
    for p, wanted in WANT.items():
        for table in TABLES:
            schema = ancestors(table)[1]
            df, msg = model.read(p, table)
            ok = df is not None
            sel = model.source(p, "SELECT", table)
            rows.append((p, schema, "READS" if ok else "denied", sel or "-", "wanted" if schema in wanted else "forbidden"))
            if schema in wanted and not ok:
                verdict["missing"].append(f"{p}:{schema}")
            if schema not in wanted and ok:
                verdict["over_read"].append(f"{p}:{schema}")
            if schema not in wanted and sel and not ok:
                verdict["latent_select"].append(f"{p}:{schema}")
            if model.owners.get(schema) in model.identities(p):
                verdict["owns"].append(f"{p}:{schema}")
            if show_rows and schema in wanted:
                print(f"--- {p} reads {table}:", "" if ok else msg)
                if ok:
                    df.limit(3).show(truncate=False)
    print(f"\n{'principal':18s} {'schema':18s} {'result':7s} {'SELECT comes from':34s} requirement")
    for r in rows:
        print(f"{r[0]:18s} {r[1]:18s} {r[2]:7s} {r[3]:34s} {r[4]}")
    verdict["meets requirements"] = not verdict["missing"] and not verdict["over_read"]
    verdict["least privilege"] = verdict["meets requirements"] and not verdict["latent_select"] and not verdict["owns"]
    print("\nverdict:", {k: v for k, v in verdict.items()})
    RESULTS[label] = verdict
    return verdict

print("requirements:", WANT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option D, run first: `USE CATALOG` on `sales`, then `USE SCHEMA` + `SELECT` per schema, per principal
# MAGIC
# MAGIC Least privilege is two layers: the **traversal** privileges that let a principal reach an object
# MAGIC (`USE CATALOG`, `USE SCHEMA`) and the **data** privilege (`SELECT`) scoped to exactly the schemas it needs.
# MAGIC Nothing is granted at catalog level except `USE CATALOG`, which by itself reads no data.

# COMMAND ----------

# DBTITLE 1,Option D: the five statements, executed
D_SQL = [
    f"GRANT USE CATALOG ON CATALOG sales TO `{BI}`",
    f"GRANT USE CATALOG ON CATALOG sales TO `{DS}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO `{BI}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO `{DS}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.silver TO `{DS}`",
]
model_d = UCModel()
for stmt in D_SQL:
    model_d.execute(stmt)
audit("D", model_d, show_rows=True)

# COMMAND ----------

# DBTITLE 1,Option D: what each principal gets when it tries the forbidden schemas, and when it tries to re-grant
for p, table in [(BI, "sales.silver.orders"), (BI, "sales.bronze.raw_orders"), (DS, "sales.bronze.raw_orders"), (DS, "sales.finance.ledger")]:
    _, msg = model_d.read(p, table)
    print(f"{p:16s} SELECT * FROM {table:26s} -> {msg}")
print()
model_d.execute(f"GRANT SELECT ON SCHEMA sales.gold TO `account users`", actor=BI)

# COMMAND ----------

# MAGIC %md
# MAGIC What the output shows:
# MAGIC
# MAGIC * The BI principal reads `sales.gold` and is refused everywhere else; the data science group reads `sales.gold`
# MAGIC   and `sales.silver` and is refused on `sales.bronze`, `sales.finance` and `sales.hr_staging`. The refusals
# MAGIC   name the missing `USE SCHEMA`: the `bronze` and other schemas are simply unreachable.
# MAGIC * The "SELECT comes from" column shows every `SELECT` is anchored on the schema it was meant for. Because
# MAGIC   `SELECT` was **never granted at the catalog level**, nothing is inherited by `bronze` or by any schema added
# MAGIC   to `sales` next month.
# MAGIC * Neither principal owns anything, so the BI principal cannot pass its access on: its `GRANT` to
# MAGIC   `account users` is denied.
# MAGIC * The traversal grants are part of least privilege, not an over-grant: `USE CATALOG` and `USE SCHEMA` read no
# MAGIC   data on their own, and without them the `SELECT` cannot be used (section 3 proves it).
# MAGIC
# MAGIC In a real workspace the BI service principal is referenced by its application ID in the `TO` clause; the
# MAGIC widget holds a readable stand-in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: `SELECT` on the schemas, no `USE CATALOG` or `USE SCHEMA`
# MAGIC
# MAGIC The misconception: that data privileges alone are enough, and that traversal privileges are an over-grant to
# MAGIC be avoided.

# COMMAND ----------

# DBTITLE 1,Option A: exactly the SELECT grants, nothing else
A_SQL = [
    f"GRANT SELECT ON SCHEMA sales.gold TO `{BI}`",
    f"GRANT SELECT ON SCHEMA sales.gold TO `{DS}`",
    f"GRANT SELECT ON SCHEMA sales.silver TO `{DS}`",
]
model_a = UCModel()
for stmt in A_SQL:
    model_a.execute(stmt)
audit("A", model_a, show_rows=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Why A is wrong, from the output:
# MAGIC
# MAGIC * Every read fails, including the wanted ones, with `User does not have USE CATALOG on Catalog 'sales'`. The
# MAGIC   `SELECT` grants exist (the "SELECT comes from" column shows them) but **cannot be exercised** without the
# MAGIC   traversal privileges above them.
# MAGIC * So A fails to grant working access at all. Traversal privileges are part of least privilege, not an
# MAGIC   over-grant: option D adds exactly `USE CATALOG` on `sales` and `USE SCHEMA` on the needed schemas and nothing
# MAGIC   more.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: `USE CATALOG` and `SELECT` on the whole catalog, then targeted revokes
# MAGIC
# MAGIC The misconception: that you can grant broadly at the catalog and claw back with `REVOKE` on the schemas that
# MAGIC should stay closed.

# COMMAND ----------

# DBTITLE 1,Option B as written: catalog-wide SELECT, then the two revokes
B_SQL = [
    f"GRANT USE CATALOG, SELECT ON CATALOG sales TO `{BI}`",
    f"GRANT USE CATALOG, SELECT ON CATALOG sales TO `{DS}`",
    f"REVOKE SELECT ON SCHEMA sales.bronze FROM `{DS}`",
    f"REVOKE SELECT ON SCHEMA sales.silver FROM `{BI}`",
]
model_b = UCModel()
for stmt in B_SQL:
    model_b.execute(stmt)
print("\nAfter the revokes, where does the data science group's SELECT on bronze come from?",
      model_b.source(DS, "SELECT", "sales.bronze.raw_orders"))
print("And the BI principal's SELECT on silver?", model_b.source(BI, "SELECT", "sales.silver.orders"))
audit("B as written", model_b)

# COMMAND ----------

# DBTITLE 1,Option B as teams finish it: add USE SCHEMA at catalog level so the reads work
model_b.execute(f"GRANT USE SCHEMA ON CATALOG sales TO `{BI}`")
model_b.execute(f"GRANT USE SCHEMA ON CATALOG sales TO `{DS}`")
audit("B", model_b)
df, msg = model_b.read(DS, "sales.bronze.raw_orders")
print(f"\n{DS} reads the raw bronze table it must never see:")
if df is not None:
    df.limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why B is wrong, from the output:
# MAGIC
# MAGIC * A catalog-wide `SELECT` **inherits down to all schemas**: `bronze`, `silver`, `gold`, `finance` and
# MAGIC   `hr_staging` (and any schema created later). The "SELECT comes from" column reads `SELECT on sales` for every
# MAGIC   schema.
# MAGIC * Both revokes are accepted and **remove 0 grants**: there was no direct grant on `sales.bronze` or
# MAGIC   `sales.silver` to remove. A revoke at the schema level does not remove a privilege inherited from the catalog
# MAGIC   grant, because Unity Catalog has no deny.
# MAGIC * As written, B even forgets `USE SCHEMA`, so nothing is readable yet, while the forbidden `SELECT` sits there
# MAGIC   latent. The moment `USE SCHEMA` is added the usual way, at the catalog, the data science group reads the raw
# MAGIC   `bronze` customer e-mails and the BI principal reads `silver`, `finance` and `hr_staging`.
# MAGIC * Relying on targeted revokes to claw back access is error-prone and violates least privilege; grant narrowly
# MAGIC   instead (option D).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option C: make each principal the owner of "its" schema
# MAGIC
# MAGIC The misconception: that ownership is a way to give read access.

# COMMAND ----------

# DBTITLE 1,Option C: USE CATALOG plus ownership of gold and silver
C_SQL = [
    f"GRANT USE CATALOG ON CATALOG sales TO `{BI}`",
    f"GRANT USE CATALOG ON CATALOG sales TO `{DS}`",
    f"ALTER SCHEMA sales.gold OWNER TO `{BI}`",
    f"ALTER SCHEMA sales.silver OWNER TO `{DS}`",
]
model_c = UCModel()
for stmt in C_SQL:
    model_c.execute(stmt)
audit("C", model_c, show_rows=True)

# COMMAND ----------

# DBTITLE 1,Option C: what an owner can do besides reading
print(f"As {BI}, owner of sales.gold:")
model_c.execute(f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO `account users`", actor=BI)
model_c.execute(f"ALTER SCHEMA sales.gold OWNER TO `someone-else`", actor=BI)
print(f"\nAs {DS}, owner of sales.silver only:")
model_c.execute(f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO `{DS}`", actor=DS)
print("\nwho owns what now:", {k: v for k, v in model_c.owners.items() if k.count(".") == 1})

# COMMAND ----------

# MAGIC %md
# MAGIC Why C is wrong, from the output:
# MAGIC
# MAGIC * Ownership confers **full control and management rights** over the object, not just read access: the BI
# MAGIC   service principal, as owner of `sales.gold`, could open it to `account users` and even hand ownership away.
# MAGIC   An owner can also alter the schema and remove its tables. That is far more than least-privilege read.
# MAGIC * It also leaves the data science group **without access to `sales.gold`**: the audit shows the wanted read
# MAGIC   denied, and the group cannot fix it itself because it does not own `gold` (its `GRANT` is denied).
# MAGIC * Ownership belongs to the team that manages the object, usually a group of data engineers; read access is
# MAGIC   granted, as in option D.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Option E: one shared group for both principals
# MAGIC
# MAGIC The misconception: that "grant to groups, not individuals" means putting principals with different needs into
# MAGIC the same group.

# COMMAND ----------

# DBTITLE 1,Option E: one group, gold and silver for everyone in it
GROUP_E = "bi-and-ds-readers"
model_e = UCModel(groups={GROUP_E: {BI, DS}})
E_SQL = [
    f"GRANT USE CATALOG ON CATALOG sales TO `{GROUP_E}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO `{GROUP_E}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA sales.silver TO `{GROUP_E}`",
]
for stmt in E_SQL:
    model_e.execute(stmt)
audit("E", model_e)
df, _ = model_e.read(BI, "sales.silver.orders")
print(f"\n{BI} reads silver through the group:")
if df is not None:
    df.limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Why E is wrong, from the output:
# MAGIC
# MAGIC * The BI principal must not read `silver`, yet it does, via `group bi-and-ds-readers`. Putting both principals
# MAGIC   in one group with `silver` and `gold` **over-grants `silver` to the BI principal**, violating least privilege.
# MAGIC * Granting to groups is good practice, but the group must match one access profile. Two profiles (gold only,
# MAGIC   gold plus silver) need two sets of grants, which is what option D does; the data science group already is a
# MAGIC   group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The five options side by side

# COMMAND ----------

# DBTITLE 1,Which option meets both requirements with least privilege
print(f"{'option':14s} {'meets reqs':11s} {'least priv':11s} {'missing reads':42s} {'over-reads / latent SELECT / owns'}")
for label in ["D", "A", "B as written", "B", "C", "E"]:
    v = RESULTS[label]
    extra = v["over_read"] + v["latent_select"] + v["owns"]
    print(f"{label:14s} {str(v['meets requirements']):11s} {str(v['least privilege']):11s} "
          f"{len(v['missing']):<42d} {len(extra)} {extra[:3]}{' ...' if len(extra) > 3 else ''}")
print("\nOnly D gives each principal exactly its schemas, through traversal plus schema-scoped SELECT.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The rule to memorise
# MAGIC
# MAGIC Reading a table in Unity Catalog takes three privileges: `USE CATALOG` on the catalog, `USE SCHEMA` on the
# MAGIC schema and `SELECT` on the table or a parent; the first two are traversal and are part of least privilege, not
# MAGIC an over-grant. Grant `SELECT` at the narrowest level that covers exactly what each principal needs, here the
# MAGIC schema, and never at the catalog, because a catalog grant inherits into every schema, including ones created
# MAGIC later. Unity Catalog has no deny, so a `REVOKE` on a child cannot claw back a privilege inherited from the parent.
# MAGIC Ownership is management, not read access, and a group should hold one access profile, so principals with
# MAGIC different needs get different grants.
