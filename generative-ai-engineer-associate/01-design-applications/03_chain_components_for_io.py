# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Which chain components does the requirement need? (retriever, chat history, parser)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 1 of the exam guide, "Select chain components for
# MAGIC a desired model input and output". Lesson 01 (the pipe) and lesson 02 (RAG in one chain) built the pieces;
# MAGIC this lesson is about reading a requirement and picking exactly the pieces it needs.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the chat model and the embedding model are pay-per-token
# MAGIC Foundation Model endpoints, nothing to deploy. Outputs shown in the video were captured on 11/04/2026
# MAGIC (langchain-core 1.7.x).
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A generative AI engineer is building a customer support bot with LangChain on Databricks. The bot must
# MAGIC (1) answer only from the internal handbook stored in a vector index, (2) handle follow-up questions such as
# MAGIC "tell me more about the second one", and (3) return plain text. Which combination of components is required?
# MAGIC
# MAGIC * **A** `PromptTemplate` + chat model + `StrOutputParser`
# MAGIC * **B** `ChatPromptTemplate` with a `MessagesPlaceholder` for the history + a retriever + chat model + `StrOutputParser`
# MAGIC * **C** a retriever + `PromptTemplate` + chat model + `StrOutputParser`
# MAGIC * **D** `ChatPromptTemplate` with a `MessagesPlaceholder` for the history + chat model + `StrOutputParser`
# MAGIC
# MAGIC The correct chain (B) is built and run first, on two turns of a conversation. Then the three other chains
# MAGIC run on the same two turns, and each one fails the requirement it has no component for.

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
# MAGIC ## 1. Setup: the handbook, a retriever, two turns of one conversation
# MAGIC
# MAGIC The handbook has two remedies for cold food, so "the second one" means something. In production the store
# MAGIC is a Vector Search index (`DatabricksVectorSearch(...).as_retriever()`); the chains below do not change.

# COMMAND ----------

# DBTITLE 1,Documents, embeddings, retriever
from langchain_core.documents import Document
from langchain_core.vectorstores import InMemoryVectorStore
from databricks_langchain import DatabricksEmbeddings

HANDBOOK = [
    "Cold food: the customer may choose one of two remedies. Option one is a full refund of the food items, available if the complaint is filed within 14 days of delivery.",
    "Cold food, option two: a re-delivery credit of 10 dollars added to the account within 48 hours; the credit expires after 30 days and cannot be converted to cash.",
    "Driver tips are capped at 25 dollars per order and are paid out to the driver every Friday.",
    "Orders over 60 dollars get free delivery; otherwise the delivery fee is 3.99.",
    "Restaurants can pause their menu for up to 48 hours from the partner app without a penalty.",
    "A chat is escalated to a human agent when the customer writes the word manager or after three unresolved bot replies.",
    "Promo codes cannot be combined; the newest code applied wins.",
]
docs = [Document(page_content=t, metadata={"source": f"handbook#{i+1}"}) for i, t in enumerate(HANDBOOK)]

vectorstore = InMemoryVectorStore.from_documents(docs, embedding=DatabricksEmbeddings(endpoint=EMBED_ENDPOINT))
retriever = vectorstore.as_retriever(search_kwargs={"k": 3})

def format_docs(found: list[Document]) -> str:
    return "\n".join(f"[{d.metadata['source']}] {d.page_content}" for d in found)

# COMMAND ----------

# DBTITLE 1,The model, the parser, and the two turns every chain will face
from databricks_langchain import ChatDatabricks
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import HumanMessage, AIMessage

llm = ChatDatabricks(endpoint=ENDPOINT, temperature=0)
parser = StrOutputParser()

TURN_1 = "My food arrived cold. What are my options?"
TURN_2 = "Tell me more about the second one."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The chain with a component per requirement (option B)
# MAGIC
# MAGIC Read the requirement as a checklist. "Only from the handbook" needs a **retriever** feeding a context slot.
# MAGIC "Follow-up questions" need the earlier turns inside the prompt, which is what a **`MessagesPlaceholder`** in
# MAGIC a **`ChatPromptTemplate`** holds. "Plain text" is the **`StrOutputParser`**. The chain's input is therefore a
# MAGIC dict with the question and the history, and the first step builds the three things the prompt declares.
# MAGIC
# MAGIC One detail decides whether the follow-up works: the history has to reach **two** places. The prompt, so the
# MAGIC model knows what "the second one" refers to, and the **retriever's query**, because the words "the second
# MAGIC one" on their own retrieve nothing useful. The simplest history-aware retrieval is to search with the last
# MAGIC turn appended to the new question.

# COMMAND ----------

# DBTITLE 1,The correct chain, run first: context slot, history slot, question
from operator import itemgetter
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnableLambda

prompt_b = ChatPromptTemplate.from_messages([
    ("system", "You are a support assistant. Answer in plain text using only the context below and the conversation so far. "
               "If the answer is not in the context, say you do not know.\n\nContext:\n{context}"),
    MessagesPlaceholder("history"),
    ("human", "{question}"),
])
print(prompt_b.input_variables)

def retrieval_query(x: dict) -> str:
    """The new question plus the last exchange, so a follow-up retrieves the chunks it refers to."""
    recent = " ".join(m.content for m in x["history"][-2:])
    return f"{recent} {x['question']}".strip()

chain_b = (
    {
        "context":  RunnableLambda(retrieval_query) | retriever | format_docs,
        "history":  itemgetter("history"),
        "question": itemgetter("question"),
    }
    | prompt_b | llm | parser
)

# COMMAND ----------

# DBTITLE 1,Turn 1: grounded in the handbook
answer_1 = chain_b.invoke({"question": TURN_1, "history": []})
print(answer_1)

# COMMAND ----------

# DBTITLE 1,Turn 2: the follow-up resolves because the first turn is in the prompt
history = [HumanMessage(TURN_1), AIMessage(answer_1)]
answer_2 = chain_b.invoke({"question": TURN_2, "history": history})
print(answer_2)

# COMMAND ----------

# DBTITLE 1,Why the history must reach the retriever too: what each query finds for turn 2
print("question alone     ->", [d.metadata["source"] for d in retriever.invoke(TURN_2)])
print("with the last turn ->", [d.metadata["source"] for d in retriever.invoke(retrieval_query({"history": history, "question": TURN_2}))])

# COMMAND ----------

# DBTITLE 1,What the prompt received on turn 2: the messages, in order
for m in prompt_b.invoke({"context": "<handbook chunks>", "history": history, "question": TURN_2}).to_messages():
    print(f"[{m.type:6s}] {m.content[:90]}")

# COMMAND ----------

# DBTITLE 1,The chain drawn: three branches before the prompt, the history feeding two of them
chain_b.get_graph().print_ascii()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The three other options, each run on the same two turns
# MAGIC
# MAGIC | Option | Missing component | Requirement it fails |
# MAGIC |---|---|---|
# MAGIC | A `PromptTemplate` + model + parser | retriever and history | (1) not grounded, (2) no follow-ups |
# MAGIC | C retriever + `PromptTemplate` + model + parser | history | (2) "the second one" retrieves the wrong chunk and the model has nothing to refer to |
# MAGIC | D `ChatPromptTemplate` with history + model + parser | retriever | (1) the handbook is never in the prompt |

# COMMAND ----------

# DBTITLE 1,Option A: no retriever, no history
from langchain_core.prompts import PromptTemplate

prompt_a = PromptTemplate.from_template(
    "You are a support assistant for a food delivery company. Answer in plain text. "
    "If you do not know the company policy, say you do not know.\n\nQuestion: {question}")
chain_a = prompt_a | llm | parser

print("turn 1:", chain_a.invoke({"question": TURN_1}))
print("turn 2:", chain_a.invoke({"question": TURN_2}))

# COMMAND ----------

# DBTITLE 1,Option C: grounded, but each turn starts from nothing
from langchain_core.runnables import RunnablePassthrough

prompt_c = PromptTemplate.from_template(
    "You are a support assistant. Answer in plain text using only the context below. "
    "If the answer is not in the context, say you do not know.\n\nContext:\n{context}\n\nQuestion: {question}")
chain_c = {"context": retriever | format_docs, "question": RunnablePassthrough()} | prompt_c | llm | parser

print("turn 1:", chain_c.invoke(TURN_1))
print("turn 2:", chain_c.invoke(TURN_2))

# COMMAND ----------

# DBTITLE 1,Option D: remembers the conversation, never reads the handbook
prompt_d = ChatPromptTemplate.from_messages([
    ("system", "You are a support assistant for a food delivery company. Answer in plain text. "
               "If you do not know the company policy, say you do not know."),
    MessagesPlaceholder("history"),
    ("human", "{question}"),
])
chain_d = prompt_d | llm | parser

answer_d1 = chain_d.invoke({"question": TURN_1, "history": []})
print("turn 1:", answer_d1)
print("turn 2:", chain_d.invoke({"question": TURN_2, "history": [HumanMessage(TURN_1), AIMessage(answer_d1)]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Requirement words and the component each one buys
# MAGIC
# MAGIC | The requirement says | Component | Where it sits |
# MAGIC |---|---|---|
# MAGIC | "only from our documents", "grounded", "cite" | a retriever (Vector Search index) | fills the `{context}` slot before the prompt |
# MAGIC | "follow-up questions", "multi-turn", "remembers" | `MessagesPlaceholder` for the history in a `ChatPromptTemplate` | between the system message and the new question |
# MAGIC | "plain text", "a string for the UI" | `StrOutputParser` | last |
# MAGIC | "a dict / JSON the app reads" | `JsonOutputParser` or structured output (notebook 01) | last |
# MAGIC | "the question plus other fields" | `itemgetter` branches, `RunnablePassthrough.assign` | the dict before the prompt |
# MAGIC | "a chat model" vs "a completion model" | `ChatPromptTemplate` (messages) vs `PromptTemplate` (one string) | the prompt |
# MAGIC
# MAGIC Two notes for later lessons: appending the last turn to the retrieval query is the smallest history-aware
# MAGIC retriever; production chains usually ask the model to rewrite the follow-up into a standalone question first.
# MAGIC And the history list is kept by the caller here, where a real app wraps the chain in
# MAGIC `RunnableWithMessageHistory` or a Lakebase-backed memory store.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The rule to memorise
# MAGIC
# MAGIC Turn the requirement into a checklist of inputs and outputs, then buy one component per line: grounding is a
# MAGIC retriever, follow-ups are a history slot in a chat prompt (and the history in the retrieval query), the
# MAGIC output type is the parser. A requirement with no component behind it is a requirement the chain will fail,
# MAGIC and the exam's wrong options are exactly the chains with one line unbought.
