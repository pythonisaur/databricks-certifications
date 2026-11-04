# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lesson 03: Chunking decides the record count (chunk size, overlap, and what the embedding model does not change)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 2 of the exam guide, "Apply a chunking strategy for a
# MAGIC given document structure and model constraints". Official sample question 1 is this exact shape.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute
# MAGIC is enough and Free Edition works: the splitter runs locally and the embedding models are pay-per-token
# MAGIC Foundation Model endpoints, nothing to deploy. Outputs shown in the video were captured on 11/04/2026.
# MAGIC
# MAGIC **The exam question this lesson answers** (official sample question 1)
# MAGIC
# MAGIC A Generative AI Engineer is loading 150 million embeddings into a vector database that takes a maximum of
# MAGIC 100 million. Which TWO actions can they take to reduce the record count?
# MAGIC
# MAGIC * **A** Increase the document chunk size
# MAGIC * **B** Decrease the overlap between chunks
# MAGIC * **C** Decrease the document chunk size
# MAGIC * **D** Increase the overlap between chunks
# MAGIC * **E** Use a smaller embedding model
# MAGIC
# MAGIC The two correct actions (A and B) run first on a real document, with the count projected to the question's
# MAGIC scale. The three distractors then run together in one table, because they are common and quick to see through.

# COMMAND ----------

# MAGIC %pip install -q -U langchain-text-splitters databricks-langchain

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Endpoint widgets
# Two Databricks-hosted pay-per-token embedding endpoints. Free Edition lists them in Playground > Select an endpoint.
dbutils.widgets.text("EMBED_ENDPOINT", "databricks-gte-large-en")
dbutils.widgets.text("SMALL_EMBED_ENDPOINT", "databricks-bge-large-en")
EMBED_ENDPOINT = dbutils.widgets.get("EMBED_ENDPOINT")
SMALL_EMBED_ENDPOINT = dbutils.widgets.get("SMALL_EMBED_ENDPOINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: one real document and the splitter
# MAGIC
# MAGIC One record in a vector database is one chunk. The splitter decides how many chunks a document becomes, so
# MAGIC the splitter decides the record count. Everything in this lesson is that one sentence, measured.
# MAGIC
# MAGIC The question talks about chunk size and overlap as a window sliding over the text, so the lesson uses exactly
# MAGIC that splitter: a fixed-size window with a fixed overlap. The structure-aware splitter you use in production
# MAGIC comes back in section 4, because document structure changes the count too.

# COMMAND ----------

# DBTITLE 1,The document: an operations handbook, about 750 words
DOC = """
Delivery Partner Operations Handbook

1. Accepting orders
A delivery partner may accept any order offered within their active zone. Once accepted, the order must be picked up within the pickup window shown in the app. Declining three offers in a row pauses new offers for ten minutes. Partners who accept an order and then cancel it after pickup receive a cancellation strike; three strikes in thirty days trigger a review.

2. Pickup at the restaurant
On arrival, tap Arrived so the restaurant sees you in the queue. Check the order number on the bag against the app before leaving. Sealed bags must not be opened by the partner. If the restaurant cannot find the order, use Report a problem and wait for the dispatcher instead of leaving; waiting time above twelve minutes is paid at the waiting rate.

3. Delivery and handoff
Follow the drop-off instructions the customer left in the app. If the instructions say Leave at door, take a photo of the bag at the door and mark the order delivered. If the customer must receive the order in person, call once, wait five minutes, call again, and then contact the dispatcher. Never leave alcohol orders unattended; alcohol requires an ID check and a signature, and the recipient must be over twenty-one.

4. Cold food and remakes
If a customer reports cold food at the door, do not return to the restaurant on your own. Mark the complaint in the app; the support team decides whether the restaurant remakes the order. A remake is a new delivery and is paid as a new delivery. Partners are not charged for cold-food complaints unless the delivery took more than twenty minutes longer than the estimate.

5. Tips and payouts
Tips are shown after the delivery is completed and are paid out in full. Tips are capped at twenty-five dollars per order; amounts above the cap are returned to the customer. Payouts run every Friday for the week ending Sunday. Instant payout is available once per day for a one dollar fee.

6. Vehicle and equipment
Partners must carry an insulated bag on every delivery. Bikes and scooters are allowed in zones marked as bike zones; cars are required in all other zones. A partner switching vehicle type must update the profile before going online; orders offered for the wrong vehicle type are not paid at the car rate.

7. Ratings
Customers rate deliveries from one to five stars. Ratings below three are reviewed by support before they count, and a rating is removed when the delay was caused by the restaurant. The rolling rating is calculated over the last one hundred deliveries. Partners below four point two for two consecutive weeks are offered a coaching call before any other action is taken.

8. Promo codes and pricing
Partners never apply promo codes; the customer's app does. If a customer asks for a discount at the door, direct them to support. Delivery fees, service fees and small-order fees are set per city and are not negotiable at the door. A customer who refuses to pay the cash balance on a cash order should be reported through the app immediately.

9. Safety
Partners may refuse a delivery that feels unsafe and will not receive a strike for doing so. Report unsafe locations through the app so the zone map can be updated. In an accident, call emergency services first and the dispatcher second. Partners are covered by the occupational accident policy while online with an active order.

10. Support and escalation
In-app chat answers most questions within three minutes. A chat is escalated to a human agent when the partner writes the word manager or after three unresolved bot replies. Payment disputes are resolved within five business days; the decision and the amount appear in the earnings tab. Appeals against strikes are filed from the strike notification itself.

11. Restaurant partners
Restaurants can pause their menu for up to forty-eight hours from the partner app without a penalty. Pauses longer than that require a note to the account manager. Restaurants are rated separately on preparation time; a restaurant that keeps partners waiting more than fifteen minutes on average loses priority in the offer queue.

12. Account standing
Accounts are reviewed monthly on acceptance rate, completion rate and rating. No single metric deactivates an account on its own. A partner who disagrees with a review outcome may request a second review by a different team within fourteen days.
""".strip()

print(f"{len(DOC):,} characters, {len(DOC.split()):,} words, {DOC.count(chr(10)+chr(10))} sections")

# COMMAND ----------

# DBTITLE 1,The splitter and one helper that counts records
from langchain_text_splitters import CharacterTextSplitter

def chunk(chunk_size: int, chunk_overlap: int) -> list[str]:
    """A fixed-size window sliding over the words, with a fixed overlap: the splitter the question assumes."""
    splitter = CharacterTextSplitter(separator=" ", chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    return splitter.split_text(DOC)

baseline = chunk(chunk_size=500, chunk_overlap=100)
print(len(baseline), "chunks at chunk_size=500, chunk_overlap=100")
print()
for i, c in enumerate(baseline[:2]):
    print(f"--- chunk {i + 1} ({len(c)} chars)\n{c}\n")

# COMMAND ----------

# DBTITLE 1,The question's scale: how many documents like this one make 150 million records
BASE_RECORDS = 150_000_000
LIMIT = 100_000_000
DOCS = BASE_RECORDS // len(baseline)          # documents in the corpus if every document splits like this one

def records(chunk_size: int, chunk_overlap: int) -> int:
    return len(chunk(chunk_size, chunk_overlap)) * DOCS

print(f"{DOCS:,} documents x {len(baseline)} chunks = {records(500, 100):,} records (the question's starting point)")
print(f"store limit: {LIMIT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The two actions that lower the count (options A and B), run first
# MAGIC
# MAGIC A chunk is a window sliding over the text. A bigger window covers the document in fewer steps (option A).
# MAGIC Overlap makes every step shorter than the window, so less overlap also means fewer steps (option B). Both
# MAGIC change how many chunks come out of the same text, and the number of chunks is the number of records.

# COMMAND ----------

# DBTITLE 1,Option A: increase the chunk size
for size in (500, 650, 800, 1000):
    n = len(chunk(size, 100))
    print(f"chunk_size={size:5d} overlap=100 -> {n:3d} chunks per document -> {records(size, 100):>13,} records  {'fits' if records(size, 100) <= LIMIT else 'too many'}")

# COMMAND ----------

# DBTITLE 1,Option B: decrease the overlap
for overlap in (100, 50, 0):
    n = len(chunk(500, overlap))
    print(f"chunk_size=500 overlap={overlap:3d} -> {n:3d} chunks per document -> {records(500, overlap):>13,} records  {'fits' if records(500, overlap) <= LIMIT else 'too many'}")

# COMMAND ----------

# DBTITLE 1,A and B together, and the arithmetic behind the table
both = records(800, 0)
print(f"chunk_size=800 overlap=0 -> {len(chunk(800, 0))} chunks per document -> {both:,} records")
print()
print("rule of thumb: chunks per document ~ characters / (chunk_size - chunk_overlap)")
for size, overlap in [(500, 100), (800, 100), (500, 0), (800, 0)]:
    print(f"  {len(DOC):,} / ({size} - {overlap}) = {len(DOC) / (size - overlap):5.1f}   measured {len(chunk(size, overlap))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The common distractors, in one table
# MAGIC
# MAGIC Two of them move the count the wrong way. The third does not touch the count at all: an embedding model
# MAGIC changes what each vector is, never how many vectors there are, and the number of vectors is the number of chunks.

# COMMAND ----------

# DBTITLE 1,Options C, D and E next to the starting point
from databricks_langchain import DatabricksEmbeddings

rows = [
    ("start",                     500, 100, EMBED_ENDPOINT),
    ("C  smaller chunks",         300, 100, EMBED_ENDPOINT),
    ("D  more overlap",           500, 200, EMBED_ENDPOINT),
    ("E  smaller embedding model", 500, 100, SMALL_EMBED_ENDPOINT),
]
print(f"{'option':28s} {'size':>5s} {'overlap':>8s} {'chunks':>7s} {'records':>14s}   {'vector length':>13s}  endpoint")
for label, size, overlap, endpoint in rows:
    chunks = chunk(size, overlap)
    dims = len(DatabricksEmbeddings(endpoint=endpoint).embed_documents(chunks[:1])[0])
    print(f"{label:28s} {size:5d} {overlap:8d} {len(chunks):7d} {len(chunks) * DOCS:14,}   {dims:13d}  {endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Two things the question's arithmetic leaves out
# MAGIC
# MAGIC **Document structure.** The production splitter, `RecursiveCharacterTextSplitter`, cuts at paragraph breaks
# MAGIC first and only slides a window inside a paragraph that is longer than the chunk size. On this handbook, whose
# MAGIC sections are shorter than 500 characters, that means one chunk per section and the overlap setting barely
# MAGIC moves the count. Chunk size still does.

# COMMAND ----------

# DBTITLE 1,The structure-aware splitter on the same document
from langchain_text_splitters import RecursiveCharacterTextSplitter

for size, overlap in [(500, 100), (500, 0), (800, 100), (800, 0)]:
    n = len(RecursiveCharacterTextSplitter(chunk_size=size, chunk_overlap=overlap).split_text(DOC))
    print(f"recursive chunk_size={size} overlap={overlap:3d} -> {n:3d} chunks per document   (sliding window: {len(chunk(size, overlap))})")

# COMMAND ----------

# MAGIC %md
# MAGIC **The embedding model's context length**, the cap on option A.
# MAGIC
# MAGIC Bigger chunks lower the count, but a chunk longer than the embedding model's context length gets truncated,
# MAGIC and the truncated tail is never searchable. The question says "model constraints" for this reason. GTE large
# MAGIC takes 512 tokens, roughly 2,000 characters of English, so the chunk sizes above are safe; a 4,000-character
# MAGIC chunk would not be.

# COMMAND ----------

# DBTITLE 1,Longest chunk per setting against a 512-token window (about 4 characters per token)
TOKEN_WINDOW = 512
for size in (500, 800, 1000, 2500, 4000):
    longest = max(len(c) for c in chunk(size, 0))
    approx_tokens = longest // 4
    print(f"chunk_size={size:5d} -> longest chunk {longest:5d} chars ~ {approx_tokens:4d} tokens  {'fits the window' if approx_tokens <= TOKEN_WINDOW else 'TRUNCATED by the embedding model'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The rule to memorise
# MAGIC
# MAGIC Records equal chunks. To lower the count, make the window bigger or the overlap smaller, and stop before a
# MAGIC chunk outgrows the embedding model's context length. Smaller chunks and more overlap raise the count; a
# MAGIC smaller embedding model shortens each vector and leaves the count exactly where it was.
