# Databricks notebook source
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

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from databricks_langchain import ChatDatabricks

prompt = PromptTemplate.from_template("Answer in one sentence: {question}")
llm    = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct", temperature=0)
parser = StrOutputParser()

# COMMAND ----------

# Option A: the plus sign
try:
    chain = prompt + llm + parser
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# Option B: the pipe
chain = prompt | llm | parser
chain.invoke({"question": "What does the pipe operator do in LCEL?"})

# COMMAND ----------

# What flows through the pipe: dict -> PromptValue -> AIMessage -> str
q = {"question": "What is Delta Lake?"}
print(type(prompt.invoke(q)).__name__)
print(type((prompt | llm).invoke(q)).__name__)
print(type((prompt | llm | parser).invoke(q)).__name__)

# COMMAND ----------

# Option C: the legacy class (works, but it is not LCEL)
try:
    from langchain.chains import LLMChain          # langchain < 1.0
except ImportError:
    from langchain_classic.chains import LLMChain  # langchain >= 1.0

legacy = LLMChain(prompt=prompt, llm=llm)
legacy.invoke({"question": "What is Delta Lake? "})

# COMMAND ----------

# Option D: a parser that wraps the others
try:
    chain = StrOutputParser(prompt, llm)
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# Only the LCEL chain gives you stream and batch for free
chain = prompt | llm | parser
for chunk in chain.stream({"question": "Name one benefit of LCEL."}):
    print(chunk, end="")