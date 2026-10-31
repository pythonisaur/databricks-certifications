# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The plain chatbot (answers from memory)
# MAGIC
# MAGIC Same question we will later give the agent, sent to the model with **no
# MAGIC tools and no data**. It has never seen our warehouse, so whatever comes
# MAGIC back is a reasonable-sounding guess. Keep this cell's output — it is the
# MAGIC "before" picture.
# MAGIC
# MAGIC Uses a Databricks Foundation Model API chat endpoint (OpenAI wire
# MAGIC format) through the SDK's API client, so nothing extra to install; any
# MAGIC tool-calling chat endpoint in **Serving** works (set the widget).

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "databricks-gpt-oss-120b")
dbutils.widgets.text("question", "Why did our order volume in Chicago drop last week?")
ENDPOINT = dbutils.widgets.get("llm_endpoint")
QUESTION = dbutils.widgets.get("question")

from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()

def chat(messages, tools=None, max_tokens=800):
    """Call a Foundation Model API chat endpoint (OpenAI wire format) without the openai package."""
    body = {"messages": messages, "max_tokens": max_tokens}
    if tools:
        body["tools"] = tools
    return _w.api_client.do("POST", f"/serving-endpoints/{ENDPOINT}/invocations", body=body)["choices"][0]["message"]

# COMMAND ----------

msg = chat([
    {"role": "system", "content": "You are a helpful analyst at a US food-delivery company."},
    {"role": "user", "content": QUESTION},
], max_tokens=400)
answer = msg["content"]
print(f"Q: {QUESTION}\n")
import textwrap
print("\n".join(textwrap.fill(p, 110) if p.strip() else "" for p in answer.splitlines()))   # wrapped for the screen

# COMMAND ----------

# MAGIC %md
# MAGIC ## What just happened
# MAGIC
# MAGIC | | Chatbot |
# MAGIC |---|---|
# MAGIC | tables it looked at | **0** |
# MAGIC | SQL it ran | none |
# MAGIC | the answer | weather / competitors / promotions — plausible, unverified |
# MAGIC
# MAGIC > **Exam framing:** this is *generation without grounding*. Section 03
# MAGIC > calls the fix "give the model tools or retrieved context"; section 06
# MAGIC > calls the failure "hallucination" and asks you to measure it with
# MAGIC > groundedness judges. Notebook `02_data_agent` is the fix.

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"question": QUESTION, "answer": answer, "tables_looked_at": 0}))
