# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — The data agent (plan → look → write → run → check)
# MAGIC
# MAGIC One loop, four tools, read-only SQL against `food_delivery`. The model
# MAGIC decides which tool to call; the notebook executes it and feeds the
# MAGIC result back until the model answers in plain text.
# MAGIC
# MAGIC ```
# MAGIC question ─▶ PLAN ─▶ LOOK (list_tables / describe_table)
# MAGIC                      ─▶ WRITE SQL ─▶ RUN (execute_sql, SELECT only, LIMIT 200)
# MAGIC                      ─▶ CHECK (does the result make sense? retry if not) ─▶ answer
# MAGIC ```
# MAGIC
# MAGIC Two switches drive the whole demo:
# MAGIC
# MAGIC | widget | effect |
# MAGIC |---|---|
# MAGIC | `use_context = false` | raw tables only — the agent guesses what "revenue" means |
# MAGIC | `use_context = true`  | the system prompt carries `business_definitions` and the agent gets a `get_business_definitions` tool |

# COMMAND ----------

dbutils.widgets.text("catalog", "food_delivery")
dbutils.widgets.text("schema", "main")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5")
dbutils.widgets.text("today", "2026-10-27")
dbutils.widgets.dropdown("use_context", "false", ["false", "true"])
dbutils.widgets.text("question", "Why did our order volume in Chicago drop last week?")

CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
ENDPOINT, TODAY = dbutils.widgets.get("llm_endpoint"), dbutils.widgets.get("today")
USE_CONTEXT = dbutils.widgets.get("use_context") == "true"
QUESTION = dbutils.widgets.get("question")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

import json, re, time
from databricks.sdk import WorkspaceClient
client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The tools
# MAGIC Each tool returns **text** (a markdown-ish table), because that is what
# MAGIC the model reads. `execute_sql` is the only one that touches data and it
# MAGIC refuses anything that is not a single `SELECT`/`WITH` statement.

# COMMAND ----------

def _as_text(df, limit=200):
    rows = df.limit(limit).collect()
    if not rows:
        return "(0 rows)"
    cols = df.columns
    out = [" | ".join(cols), " | ".join("---" for _ in cols)]
    out += [" | ".join("" if v is None else str(v) for v in r) for r in rows]
    if len(rows) == limit:
        out.append(f"... (truncated at {limit} rows)")
    return "\n".join(out)


def list_tables():
    df = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").select("tableName")
    names = [r.tableName for r in df.collect() if r.tableName != "business_definitions" or USE_CONTEXT]
    lines = []
    for n in names:
        c = spark.sql(f"DESCRIBE TABLE EXTENDED {n}").filter("col_name = 'Comment'").collect()
        lines.append(f"{n:22s} {c[0].data_type if c else ''}")
    return "\n".join(lines)


def describe_table(table: str):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        return "error: bad table name"
    return _as_text(spark.sql(f"DESCRIBE TABLE {table}").select("col_name", "data_type"), 60)


FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|alter|create|truncate|grant|revoke|copy|vacuum|optimize)\b", re.I)

def execute_sql(sql: str):
    s = sql.strip().rstrip(";")
    if ";" in s or FORBIDDEN.search(s) or not re.match(r"^\s*(select|with)\b", s, re.I):
        return "error: read-only. One SELECT statement, no DML/DDL."
    t0 = time.time()
    try:
        df = spark.sql(s)
        body = _as_text(df)
    except Exception as e:                                # the model gets the error text and retries
        return f"error: {str(e).splitlines()[0][:400]}"
    return f"{body}\n\n(ran in {time.time() - t0:.1f} s)"


def get_business_definitions():
    return _as_text(spark.table("business_definitions"))


TOOL_FUNCS = {"list_tables": list_tables, "describe_table": describe_table, "execute_sql": execute_sql}
if USE_CONTEXT:
    TOOL_FUNCS["get_business_definitions"] = get_business_definitions

TOOLS = [
    {"type": "function", "function": {"name": "list_tables", "description": "List the tables in the warehouse with their comments.",
                                      "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "describe_table", "description": "Columns and types of one table.",
                                      "parameters": {"type": "object", "properties": {"table": {"type": "string"}}, "required": ["table"]}}},
    {"type": "function", "function": {"name": "execute_sql", "description": "Run ONE read-only SELECT (Databricks SQL). Max 200 rows returned.",
                                      "parameters": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]}}},
]
if USE_CONTEXT:
    TOOLS.append({"type": "function", "function": {"name": "get_business_definitions",
                                                   "description": "How the business defines revenue, order volume, last week, etc. Call this before writing SQL for any metric.",
                                                   "parameters": {"type": "object", "properties": {}}}})

# COMMAND ----------

# MAGIC %md
# MAGIC ## The instruction manual (system prompt)

# COMMAND ----------

SYSTEM = f"""You are a data analyst at a US food-delivery company. Today is {TODAY}.
You answer questions ONLY from the warehouse {CATALOG}.{SCHEMA} using the tools. Never answer from memory.

Work in a loop:
1. PLAN  - restate the question as a measurable thing.
2. LOOK  - list_tables, then describe_table for the tables you need. Join on the *_id columns.
3. WRITE - one Databricks SQL SELECT. Dates: placed_at is a timestamp; use date_trunc('week', ...) for weeks (Monday start).
4. RUN   - execute_sql. If it errors, fix the SQL and run again.
5. CHECK - does the number make sense? Compare with the previous period. If the question is "why", look for a
           second table that explains it (cancel_reason, weather_daily, refunds). Only then answer.

Final answer: 2-4 short sentences with the key numbers, then one line "Evidence:" naming the tables you used.
"""
if USE_CONTEXT:
    SYSTEM += "\nBUSINESS DEFINITIONS (authoritative, use them exactly):\n" + get_business_definitions() + "\n"
else:
    SYSTEM += "\nYou have no business glossary. State any assumption you make about a metric's definition.\n"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The loop

# COMMAND ----------

def box(kind, text, width=100):
    colour = {"PLAN": "33", "LOOK": "36", "WRITE": "35", "RUN": "32", "CHECK": "31", "ANSWER": "1"}[kind]
    print(f"\033[{colour}m┌─ {kind} " + "─" * (width - len(kind) - 4) + "\033[0m")
    for line in str(text).splitlines()[:40]:
        print(f"\033[{colour}m│\033[0m {line[:width]}")
    print(f"\033[{colour}m└" + "─" * width + "\033[0m")


def run_agent(question, max_steps=12, verbose=True):
    messages = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": question}]
    trace = []
    for step in range(max_steps):
        resp = client.chat.completions.create(model=ENDPOINT, messages=messages, tools=TOOLS, max_tokens=1200)
        msg = resp.choices[0].message
        if not msg.tool_calls:                               # plain text = final answer
            if verbose:
                box("ANSWER", msg.content)
            return msg.content, trace
        messages.append({"role": "assistant", "content": msg.content or "", "tool_calls": [tc.model_dump() for tc in msg.tool_calls]})
        if msg.content and verbose:
            box("PLAN", msg.content)
        for tc in msg.tool_calls:
            name, args = tc.function.name, json.loads(tc.function.arguments or "{}")
            kind = {"list_tables": "LOOK", "describe_table": "LOOK", "get_business_definitions": "LOOK", "execute_sql": "RUN"}[name]
            if name == "execute_sql" and verbose:
                box("WRITE", args["sql"])
            result = TOOL_FUNCS[name](**args)
            if verbose:
                box("CHECK" if result.startswith("error") else kind, result)
            trace.append({"step": step, "tool": name, "args": args, "result": result[:2000]})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
    return "(stopped: too many steps)", trace


print(f"Data agent · {'business context ON' if USE_CONTEXT else 'raw tables only'} · today = {TODAY}\n")
answer, trace = run_agent(QUESTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to notice
# MAGIC
# MAGIC | | Chatbot (01) | Agent (02) |
# MAGIC |---|---|---|
# MAGIC | tables looked at | 0 | 3–5 |
# MAGIC | SQL run | none | 2–6 statements, errors retried |
# MAGIC | Chicago question | "maybe the weather" | **heavy snow → a third of orders cancelled (no driver available), delivered orders down 46 %** |
# MAGIC | revenue question | — | **right only with context** (notebook 03) |
# MAGIC
# MAGIC > **Exam framing (section 03, agents & tools):** the model never touches
# MAGIC > data; it *asks* for a tool call, your code runs it and returns text.
# MAGIC > Read-only is enforced in code and in Unity Catalog grants, not in the
# MAGIC > prompt. The retry on an SQL error is the "reflection" step the exam
# MAGIC > likes to name.
