# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Right and wrong: the same agent, with and without context
# MAGIC
# MAGIC Runs the agent loop from notebook 02 on a fixed set of questions, twice:
# MAGIC raw tables only, then with the business glossary. The ground truth is
# MAGIC computed in SQL here so every answer gets a ✅ / ❌.
# MAGIC
# MAGIC This is the demo's punchline **and** an exam topic: an eval set with
# MAGIC known answers, run before and after a change (section 06).

# COMMAND ----------

CALLED_FROM_03 = True      # %run shares the interpreter: 02 sees this, defines the tools, skips its own run + exit

# COMMAND ----------

# MAGIC %run ./02_data_agent $exit_when_done="false"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ground truth

# COMMAND ----------

truth = {}
truth["chicago_last_week_orders"] = spark.sql("""
  SELECT COUNT(*) FROM orders o JOIN cities c USING (city_id)
  WHERE c.city = 'Chicago' AND placed_at >= '2026-10-19' AND placed_at < '2026-10-26'""").first()[0]
truth["chicago_prev_week_orders"] = spark.sql("""
  SELECT COUNT(*) FROM orders o JOIN cities c USING (city_id)
  WHERE c.city = 'Chicago' AND placed_at >= '2026-10-12' AND placed_at < '2026-10-19'""").first()[0]
truth["chicago_no_driver_share"] = spark.sql("""
  SELECT ROUND(AVG(CASE WHEN cancel_reason = 'no_driver_available' THEN 1.0 ELSE 0 END), 3)
  FROM orders o JOIN cities c USING (city_id)
  WHERE c.city = 'Chicago' AND placed_at >= '2026-10-19' AND placed_at < '2026-10-26'""").first()[0]
truth["revenue_sep_naive"] = spark.sql("""
  SELECT ROUND(SUM(total), 2) FROM orders WHERE placed_at >= '2026-09-01' AND placed_at < '2026-10-01'""").first()[0]
truth["revenue_sep_correct"] = spark.sql("""
  SELECT ROUND(SUM(CASE WHEN status = 'delivered' THEN subtotal ELSE 0 END)
       - (SELECT COALESCE(SUM(r.amount), 0) FROM refunds r JOIN orders o2 USING (order_id)
          WHERE o2.placed_at >= '2026-09-01' AND o2.placed_at < '2026-10-01'), 2)
  FROM orders WHERE placed_at >= '2026-09-01' AND placed_at < '2026-10-01'""").first()[0]
truth["delivered_sep"] = spark.sql("""
  SELECT COUNT(*) FROM orders WHERE status = 'delivered' AND placed_at >= '2026-09-01' AND placed_at < '2026-10-01'""").first()[0]
for k, v in truth.items():
    print(f"{k:28s} {v:,}" if isinstance(v, (int, float)) else f"{k:28s} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The questions and how we grade them
# MAGIC Grading is deliberately simple: the answer text must contain the right
# MAGIC number (±1 %) or the right cause. Good enough for a demo; the exam's
# MAGIC version of this is `mlflow.evaluate` with judges (notebook
# MAGIC `06-evaluation-and-monitoring/01`).

# COMMAND ----------

import re

def has_number(text, value, tol=0.01):
    raw = [n.lstrip("$").replace(",", "").rstrip(".") for n in re.findall(r"\$?\d[\d,]*\.?\d*", text)]
    nums = [float(n) for n in raw if n.replace(".", "", 1).isdigit()]
    return any(abs(n - value) <= tol * value for n in nums)

QUESTIONS = [
    ("Why did our order volume in Chicago drop last week?",
     lambda a: ("snow" in a.lower()) or ("driver" in a.lower())),   # either real cause counts; runs vary in which table they reach
    ("What was revenue last month?",
     lambda a: has_number(a, float(truth["revenue_sep_correct"]))),
    ("How many orders were delivered in September?",
     lambda a: has_number(a, truth["delivered_sep"], 0.005)),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run both configurations

# COMMAND ----------

results = []
for ctx in (False, True):
    USE_CONTEXT = ctx                                   # flips the globals the agent reads
    TOOL_FUNCS = {"list_tables": list_tables, "describe_table": describe_table, "execute_sql": execute_sql}
    TOOLS = [t for t in TOOLS if t["function"]["name"] != "get_business_definitions"]
    SYSTEM = SYSTEM.split("\nBUSINESS DEFINITIONS")[0].split("\nYou have no business glossary")[0]
    if ctx:
        TOOL_FUNCS["get_business_definitions"] = get_business_definitions
        TOOLS.append({"type": "function", "function": {"name": "get_business_definitions",
                      "description": "How the business defines revenue, order volume, last week, etc. Call this before writing SQL for any metric.",
                      "parameters": {"type": "object", "properties": {}}}})
        SYSTEM += "\nBUSINESS DEFINITIONS (authoritative, use them exactly):\n" + get_business_definitions() + "\n"
    else:
        SYSTEM += "\nYou have no business glossary. State any assumption you make about a metric's definition.\n"
    for q, grade in QUESTIONS:
        print("\n" + "=" * 100 + f"\n{'CONTEXT ON ' if ctx else 'RAW TABLES '}· {q}\n" + "=" * 100)
        answer, trace = run_agent(q, verbose=True)
        results.append({"context": ctx, "question": q, "correct": bool(grade(answer)),
                        "sql_runs": sum(t["tool"] == "execute_sql" for t in trace),
                        "retries": sum(t["result"].startswith("error") for t in trace), "answer": answer})

# COMMAND ----------

import pandas as pd
scorecard = pd.DataFrame(results)
scorecard["verdict"] = scorecard.correct.map({True: "✅ right", False: "❌ wrong"})
display(spark.createDataFrame(scorecard[["context", "question", "verdict", "sql_runs", "retries", "answer"]]))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected picture
# MAGIC
# MAGIC | question | raw tables | with context |
# MAGIC |---|---|---|
# MAGIC | Chicago drop | ✅ heavy snow, no drivers (the data explains itself) | ✅ |
# MAGIC | revenue last month | ❌ `SUM(total)` of every row, cancelled orders and tax included | ✅ delivered subtotal − refunds |
# MAGIC | delivered in September | ✅ | ✅ |
# MAGIC
# MAGIC The agent is not "smart" or "dumb": it is exactly as right as the
# MAGIC definitions you give it. **Semantic layer > bigger model** — the
# MAGIC cheapest-layer-first rule from the README.
