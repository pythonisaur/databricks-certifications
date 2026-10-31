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
# MAGIC Every option is run below. Three of them fail with the real error; the one that works is the one
# MAGIC whose first step produces a **dict with both keys the prompt asks for**.

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
# MAGIC ## 1. The four pieces, one at a time
# MAGIC
# MAGIC A retriever, a prompt, a chat model and a parser. The whole lesson is about **what type each one
# MAGIC hands to the next**, so every piece is invoked alone first.

# COMMAND ----------

# DBTITLE 1,A tiny corpus and a retriever
from langchain_core.documents import Document
from langchain_core.vectorstores import InMemoryVectorStore
from databricks_langchain import DatabricksEmbeddings

# Six lines from a made-up food-delivery support handbook. The model has never seen them,
# so a correct answer can only come from the context we put in the prompt.
HANDBOOK = [
    "Refund policy: a cold-food complaint is refunded in full if it is filed within 14 days of delivery.",
    "Driver tips are capped at 25 dollars per order and are paid out to the driver every Friday.",
    "Orders over 60 dollars get free delivery; otherwise the delivery fee is 3.99.",
    "Restaurants can pause their menu for up to 48 hours from the partner app without a penalty.",
    "A chat is escalated to a human agent when the customer writes the word manager or after three unresolved bot replies.",
    "Promo codes cannot be combined; the newest code applied wins.",
]
docs = [Document(page_content=t, metadata={"source": f"handbook#{i+1}"}) for i, t in enumerate(HANDBOOK)]

# In production the store is a Vector Search index: DatabricksVectorSearch(index_name=...).as_retriever()
# The chain below does not change; only this line does.
vectorstore = InMemoryVectorStore.from_documents(docs, embedding=DatabricksEmbeddings(endpoint=EMBED_ENDPOINT))
retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

# COMMAND ----------

# DBTITLE 1,A retriever takes a string and returns a list of Documents
question = "How long does a customer have to file a cold-food refund?"

hits = retriever.invoke(question)
print(type(hits).__name__, "of", len(hits), type(hits[0]).__name__)
for d in hits:
    print(" -", d.metadata["source"], "|", d.page_content)

# COMMAND ----------

# DBTITLE 1,A prompt takes a dict and wants exactly these keys
from langchain_core.prompts import ChatPromptTemplate

prompt = ChatPromptTemplate.from_template(
    "Answer the question using only the context below. "
    "If the answer is not in the context, say you do not know.\n\n"
    "Context:\n{context}\n\n"
    "Question: {question}"
)
print(prompt.input_variables)

# COMMAND ----------

# DBTITLE 1,The model and the parser, same as lesson 01
from langchain_core.output_parsers import StrOutputParser
from databricks_langchain import ChatDatabricks

llm = ChatDatabricks(endpoint=ENDPOINT, temperature=0)
parser = StrOutputParser()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option A: `retriever | prompt | llm | parser`
# MAGIC
# MAGIC The pipe hands **the retriever's output** to the prompt. That output is a list of Documents,
# MAGIC and the question string is gone: nothing carried it past the retriever.

# COMMAND ----------

chain_a = retriever | prompt | llm | parser
try:
    chain_a.invoke(question)
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The fix is a dict: RunnableParallel + RunnablePassthrough
# MAGIC
# MAGIC The prompt needs `{"context": ..., "question": ...}`. A dict of runnables builds exactly that:
# MAGIC **every value receives the same input** (the question string), runs in parallel, and the results
# MAGIC land under their keys. `RunnablePassthrough()` is the value that returns its input unchanged,
# MAGIC so the question survives into the prompt.

# COMMAND ----------

# DBTITLE 1,The dict that flows through
from langchain_core.runnables import RunnableParallel, RunnablePassthrough

augment = RunnableParallel(context=retriever, question=RunnablePassthrough())

flowing = augment.invoke(question)
print(type(flowing).__name__, list(flowing))
print("question ->", flowing["question"])
print("context  ->", type(flowing["context"]).__name__, "of", len(flowing["context"]), "Documents")

# COMMAND ----------

# DBTITLE 1,Option B: the dict, then the pipe from lesson 01
chain_b = {"context": retriever, "question": RunnablePassthrough()} | prompt | llm | parser
chain_b.invoke(question)

# COMMAND ----------

# DBTITLE 1,Inside a chain the plain dict became a RunnableParallel
print(type(chain_b.first).__name__)
chain_b.get_graph().print_ascii()

# COMMAND ----------

# DBTITLE 1,Three spellings, one object
from langchain_core.runnables import RunnableLambda

same_1 = {"context": retriever, "question": RunnablePassthrough()} | prompt
same_2 = RunnableParallel({"context": retriever, "question": RunnablePassthrough()}) | prompt
same_3 = RunnableParallel(context=retriever, question=RunnablePassthrough()) | prompt

print(type(same_1.first).__name__, type(same_2.first).__name__, type(same_3.first).__name__)
print(same_1.first.steps__.keys() == same_2.first.steps__.keys() == same_3.first.steps__.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option C: `{"context": retriever} | prompt | llm | parser`
# MAGIC
# MAGIC Half the fix. The dict is built, but without a passthrough the question never reaches the prompt.

# COMMAND ----------

chain_c = {"context": retriever} | prompt | llm | parser
try:
    chain_c.invoke(question)
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option D: `prompt | retriever | llm | parser`
# MAGIC
# MAGIC The order is backwards. The prompt runs first, and nothing has retrieved anything yet, so `context`
# MAGIC is missing. (Invoked with the dict the prompt expects, to show the missing key rather than the
# MAGIC mapping error from option A.)

# COMMAND ----------

chain_d = prompt | retriever | llm | parser
try:
    chain_d.invoke({"question": question})
except Exception as e:
    print(type(e).__name__, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Does the context actually change the answer?
# MAGIC
# MAGIC Same model, same question, prompt without the handbook.

# COMMAND ----------

bare_prompt = ChatPromptTemplate.from_template(
    "Answer the question. If you do not know, say you do not know.\n\nQuestion: {question}"
)
bare_chain = bare_prompt | llm | parser

print("without context:", bare_chain.invoke({"question": question}))
print("with context:   ", chain_b.invoke(question))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Two refinements you will see in exam snippets
# MAGIC
# MAGIC **Format the documents.** The prompt above received the list of Documents and stringified it,
# MAGIC `page_content='...' metadata={...}` and all. A small function between the retriever and the prompt
# MAGIC turns the list into clean text. A plain function in a pipe is coerced to a `RunnableLambda`.
# MAGIC
# MAGIC **Pick keys with `itemgetter`.** When the chain's input is a dict instead of a bare string
# MAGIC (a question plus a language, a user id, chat history), each branch chooses the key it needs
# MAGIC and `RunnablePassthrough()` is replaced by `itemgetter("question")`.

# COMMAND ----------

# DBTITLE 1,format_docs between the retriever and the prompt
def format_docs(found: list[Document]) -> str:
    return "\n".join(f"[{d.metadata['source']}] {d.page_content}" for d in found)

augment_clean = RunnableParallel(context=retriever | format_docs, question=RunnablePassthrough())
print(augment_clean.invoke(question)["context"])

rag_chain = augment_clean | prompt | llm | parser
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
# Same idea as the dict, written as "take what came in and add context to it".
# Input dict -> output dict with every original key plus "context".
chain_assign = (
    RunnablePassthrough.assign(context=itemgetter("question") | retriever | format_docs)
    | prompt | llm | parser
)
chain_assign.invoke({"question": "Can two promo codes be combined?"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Back to the question
# MAGIC
# MAGIC | Option | What the prompt received | Result |
# MAGIC |---|---|---|
# MAGIC | A `retriever \| prompt \| ...` | a list of Documents, question lost | `TypeError`: prompt expects a mapping |
# MAGIC | **B** `{"context": retriever, "question": RunnablePassthrough()} \| prompt \| ...` | `{"context": [...], "question": "..."}` | **the grounded answer** |
# MAGIC | C `{"context": retriever} \| prompt \| ...` | `{"context": [...]}` only | `KeyError`: missing `question` |
# MAGIC | D `prompt \| retriever \| ...` | `{"question": "..."}` before any retrieval | `KeyError`: missing `context` |
# MAGIC
# MAGIC **The rule to memorise:** the step before the prompt must output a dict with every variable the
# MAGIC prompt declares. A dict of runnables (`RunnableParallel`) builds it; `RunnablePassthrough()` is how
# MAGIC the user's input survives into one of its keys.
# MAGIC
# MAGIC Next: lesson 03, chunking (how many pieces, how big). The retriever used here goes to a Vector
# MAGIC Search index in lesson 06.
