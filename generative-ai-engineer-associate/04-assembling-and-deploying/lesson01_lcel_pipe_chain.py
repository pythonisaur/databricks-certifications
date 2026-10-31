# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lesson 01: the LCEL pipe (prompt | llm | StrOutputParser)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute is enough. Outputs shown in the video were captured on 10/21/2026.

# COMMAND ----------

# MAGIC %pip install -q -U langchain-core databricks-langchain

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Endpoint widget
dbutils.widgets.text("ENDPOINT", "databricks-llama-4-maverick")
ENDPOINT = dbutils.widgets.get("ENDPOINT")

# COMMAND ----------

# DBTITLE 1,Four options
# MAGIC %md
# MAGIC ## Four ways to combine them — only one is LCEL
# MAGIC
# MAGIC * **A — `prompt + llm + parser`**: fails, because `+` is addition and Runnables do not implement it.
# MAGIC * **B — `prompt | llm | parser`**: the LCEL pipe, each component's output feeds the next.
# MAGIC * **C — `LLMChain`**: works, but it is the pre-1.0 legacy API — the deprecation warning in its output is expected, not a bug.
# MAGIC * **D — `StrOutputParser(prompt, llm)`**: fails, because a parser transforms a model's output; it never wraps other components.

# COMMAND ----------

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from databricks_langchain import ChatDatabricks

# Hosted by Databricks, nothing to deploy. Free Edition lists these in Playground > Select an endpoint
# (shown there as system.ai.llama-4-maverick; the serving name is the "databricks-" form).
# A plain chat model: reasoning models such as GPT OSS return reasoning blocks that StrOutputParser cannot flatten.
prompt = PromptTemplate.from_template("Answer in one sentence: {question}")
llm    = ChatDatabricks(endpoint=ENDPOINT, temperature=0)
parser = StrOutputParser()

# COMMAND ----------

# Option A: the pipe
chain = prompt | llm | parser
chain.invoke({"question": "What does the pipe operator do in LangChain Expression Language (LCEL)?"})

# COMMAND ----------

# Only the LCEL chain gives you stream and batch for free
chain = prompt | llm | parser
for chunk in chain.stream({"question": "Name one benefit of LangChain Expression Language (LCEL)."}):
    print(chunk, end="")
print()

chain.batch([{"question": "What is Delta Lake?"}, {"question": "What is Unity Catalog?"}])

# COMMAND ----------

# Option B: the plus sign
try:
    chain = prompt + llm + parser
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# Option C: the legacy class (works, but it is not LCEL)
try:
    from langchain.chains import LLMChain          # langchain < 1.0
except ImportError:
    from langchain_classic.chains import LLMChain  # langchain >= 1.0

legacy = LLMChain(prompt=prompt, llm=llm)
legacy.invoke({"question": "What is Delta Lake?"})

# COMMAND ----------

# Option D: a parser that wraps the others
try:
    chain = StrOutputParser(prompt, llm)
except Exception as e:
    print(type(e).__name__, "->", e)