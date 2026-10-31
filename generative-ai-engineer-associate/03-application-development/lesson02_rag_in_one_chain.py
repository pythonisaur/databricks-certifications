# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lesson 02: RAG in one chain (retriever, passthrough, parallel)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 3 of the exam guide,
# MAGIC "Augment a prompt with additional context from a user's input".
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom.
# MAGIC Serverless compute is enough and Free Edition works: the chat model and the embedding model are
# MAGIC pay-per-token Foundation Model endpoints, nothing to deploy. Outputs shown in the video were captured
# MAGIC on 10/31/2026 (langchain-core 1.7.x).
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A generative AI engineer is building a RAG chain with LCEL. The chain receives the user's question
# MAGIC as a string, retrieves the three most relevant chunks, inserts both the chunks and the original
# MAGIC question into a prompt template with the variables `{context}` and `{question}`, calls the LLM and
# MAGIC returns a string. Which snippet correctly represents this chain?
# MAGIC
# MAGIC * **A** `retriever | prompt | llm | StrOutputParser()`
# MAGIC * **B** `{"context": retriever, "question": RunnablePassthrough()} | prompt | llm | StrOutputParser()`
# MAGIC * **C** `{"context": retriever} | prompt | llm | StrOutputParser()`
# MAGIC * **D** `prompt | retriever | llm | StrOutputParser()`
# MAGIC
# MAGIC The correct chain (B) is built and explained first. Then the three wrong options are run, each to its
# MAGIC real error, with one line on why.

# COMMAND ----------

# MAGIC %pip install -q -U langchain-core databricks-langchain grandalf

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Endpoint widgets
# Both are Databricks-hosted pay-per-token endpoints. Free Edition lists them in Playground > Select an endpoint.
dbutils.widgets.text("ENDPOINT", "databricks-llama-4-maverick")        # a plain chat model (no reasoning blocks)
dbutils.widgets.text("EMBED_ENDPOINT", "databricks-gte-large-en")      # the embedding model behind the retriever
ENDPOINT = dbutils.widgets.get("ENDPOINT")
EMBED_ENDPOINT = dbutils.widgets.get("EMBED_ENDPOINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a small corpus, a retriever, a prompt with two slots
# MAGIC
# MAGIC Six lines of a made-up support handbook become Documents, an embedding endpoint turns them into vectors
# MAGIC in an in-memory store, and the store gives us a retriever. In production the store is a Vector Search
# MAGIC index (`DatabricksVectorSearch(...).as_retriever()`); the chain below does not change.

# COMMAND ----------

# DBTITLE 1,Documents, embeddings, retriever
from langchain_core.documents import Document
from langchain_core.vectorstores import InMemoryVectorStore
from databricks_langchain import DatabricksEmbeddings

HANDBOOK = [
    "Refund policy: a cold-food complaint is refunded in full if it is filed within 14 days of delivery.",
    "Driver tips are capped at 25 dollars per order and are paid out to the driver every Friday.",
    "Orders over 60 dollars get free delivery; otherwise the delivery fee is 3.99.",
    "Restaurants can pause their menu for up to 48 hours from the partner app without a penalty.",
    "A chat is escalated to a human agent when the customer writes the word manager or after three unresolved bot replies.",
    "Promo codes cannot be combined; the newest code applied wins.",
]
docs = [Document(page_content=t, metadata={"source": f"handbook#{i+1}"}) for i, t in enumerate(HANDBOOK)]

vectorstore = InMemoryVectorStore.from_documents(docs, embedding=DatabricksEmbeddings(endpoint=EMBED_ENDPOINT))
retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

# COMMAND ----------

# DBTITLE 1,A prompt with two slots, the model and the parser from lesson 01
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from databricks_langchain import ChatDatabricks

prompt = ChatPromptTemplate.from_template(
    "Answer the question using only the context below. "
    "If the answer is not in the context, say you do not know.\n\n"
    "Context:\n{context}\n\n"
    "Question: {question}"
)
llm = ChatDatabricks(endpoint=ENDPOINT, temperature=0)
parser = StrOutputParser()

print(prompt.input_variables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The chain (option B): a dict with both keys, then the pipe from lesson 01
# MAGIC
# MAGIC The prompt needs `{"context": ..., "question": ...}` but the chain receives one string. The first step
# MAGIC is a dict of runnables: **every value receives the same input**, the retriever fills `context` with the
# MAGIC closest documents, and `RunnablePassthrough()` fills `question` with the input itself, unchanged.

# COMMAND ----------

# DBTITLE 1,The correct chain, run first
from langchain_core.runnables import RunnablePassthrough

question = "How long does a customer have to file a cold-food refund?"

chain = {"context": retriever, "question": RunnablePassthrough()} | prompt | llm | parser
chain.invoke(question)

# COMMAND ----------

# DBTITLE 1,What the first step produces: the dict the prompt receives
from langchain_core.runnables import RunnableParallel

flowing = RunnableParallel(context=retriever, question=RunnablePassthrough()).invoke(question)
print(type(flowing).__name__, list(flowing))
print("question ->", flowing["question"])
print("context  ->", type(flowing["context"]).__name__, "of", len(flowing["context"]), "Documents")
print("first step of the chain ->", type(chain.first).__name__)

# COMMAND ----------

# DBTITLE 1,The retriever alone: a string in, a list of Documents out
for d in retriever.invoke(question):
    print(" -", d.metadata["source"], "|", d.page_content)

# COMMAND ----------

# DBTITLE 1,The chain drawn: the two branches before the prompt
chain.get_graph().print_ascii()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Does the context change the answer?
# MAGIC
# MAGIC Same model, same question, prompt without the handbook.

# COMMAND ----------

bare_prompt = ChatPromptTemplate.from_template(
    "Answer the question. If you do not know, say you do not know.\n\nQuestion: {question}"
)
bare_chain = bare_prompt | llm | parser

print("without context:", bare_chain.invoke({"question": question}))
print("with context:   ", chain.invoke(question))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The three wrong options, each run to its real error
# MAGIC
# MAGIC | Option | What the prompt receives | Result |
# MAGIC |---|---|---|
# MAGIC | A `retriever \| prompt \| ...` | a list of Documents, question lost | `TypeError`: prompt expects a mapping |
# MAGIC | C `{"context": retriever} \| prompt \| ...` | `{"context": [...]}` only | `KeyError`: missing `question` |
# MAGIC | D `prompt \| retriever \| ...` | `{"question": "..."}` before any retrieval | `KeyError`: missing `context` |

# COMMAND ----------

# DBTITLE 1,Option A: the retriever piped straight into the prompt
chain_a = retriever | prompt | llm | parser
try:
    chain_a.invoke(question)
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# DBTITLE 1,Option C: the dict without the passthrough
chain_c = {"context": retriever} | prompt | llm | parser
try:
    chain_c.invoke(question)
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# DBTITLE 1,Option D: the prompt before the retriever (invoked with the dict it expects)
chain_d = prompt | retriever | llm | parser
try:
    chain_d.invoke({"question": question})
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Three refinements you will see in exam snippets
# MAGIC
# MAGIC **Format the documents.** The prompt above received the list of Documents and stringified it. A small
# MAGIC function between the retriever and the prompt turns the list into clean text; a plain function in a pipe
# MAGIC is coerced to a `RunnableLambda`.
# MAGIC
# MAGIC **Pick keys with `itemgetter`.** When the chain's input is a dict instead of a bare string (a question
# MAGIC plus a language, a user id, chat history), each branch chooses the key it needs and `RunnablePassthrough()`
# MAGIC becomes `itemgetter("question")`.
# MAGIC
# MAGIC **`RunnablePassthrough.assign`** keeps the whole input dict and adds `context` to it.

# COMMAND ----------

# DBTITLE 1,format_docs between the retriever and the prompt
def format_docs(found: list[Document]) -> str:
    return "\n".join(f"[{d.metadata['source']}] {d.page_content}" for d in found)

rag_chain = {"context": retriever | format_docs, "question": RunnablePassthrough()} | prompt | llm | parser
rag_chain.invoke("What is the tip cap for a driver?")

# COMMAND ----------

# DBTITLE 1,itemgetter when the input is a dict
from operator import itemgetter

prompt_lang = ChatPromptTemplate.from_template(
    "Answer the question using only the context below, in {language}.\n\n"
    "Context:\n{context}\n\nQuestion: {question}"
)
chain_dict_in = (
    {
        "context":  itemgetter("question") | retriever | format_docs,
        "question": itemgetter("question"),
        "language": itemgetter("language"),
    }
    | prompt_lang | llm | parser
)
chain_dict_in.invoke({"question": "When is free delivery applied?", "language": "Spanish"})

# COMMAND ----------

# DBTITLE 1,RunnablePassthrough.assign keeps the whole input and adds a key
chain_assign = (
    RunnablePassthrough.assign(context=itemgetter("question") | retriever | format_docs)
    | prompt | llm | parser
)
chain_assign.invoke({"question": "Can two promo codes be combined?"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The rule to memorise
# MAGIC
# MAGIC The step before the prompt must output a dict with every variable the prompt declares. A dict of
# MAGIC runnables (`RunnableParallel`) builds it; `RunnablePassthrough()` is how the user's input survives into
# MAGIC one of its keys. Next: lesson 03, chunking (how many pieces, how big). The retriever used here goes to a
# MAGIC Vector Search index in lesson 06.
