# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The plain chatbot (answers from memory)
# MAGIC
# MAGIC Same question we will later give the agent, sent to the model with **no
# MAGIC tools and no data**. It has never seen our warehouse, so whatever comes
# MAGIC back is a reasonable-sounding guess. Keep this cell's output — it is the
# MAGIC "before" picture.
# MAGIC
# MAGIC Uses a Databricks Foundation Model API chat endpoint through the
# MAGIC OpenAI-compatible client (any chat endpoint in **Serving** works; set
# MAGIC the widget).

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-5")
dbutils.widgets.text("question", "Why did our order volume in Chicago drop last week?")
ENDPOINT = dbutils.widgets.get("llm_endpoint")
QUESTION = dbutils.widgets.get("question")

from databricks.sdk import WorkspaceClient
client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# COMMAND ----------

resp = client.chat.completions.create(
    model=ENDPOINT,
    messages=[
        {"role": "system", "content": "You are a helpful analyst at a US food-delivery company."},
        {"role": "user", "content": QUESTION},
    ],
    max_tokens=400,
)
answer = resp.choices[0].message.content
print(f"Q: {QUESTION}\n")
print(answer)

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
