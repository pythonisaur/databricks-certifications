# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Extraction and chunking
# MAGIC
# MAGIC RAG quality is mostly decided here. This notebook runs a real
# MAGIC **token-aware chunker with overlap** and a structure-aware one, side by
# MAGIC side, on the same corpus — so the trade-offs are visible in the output,
# MAGIC not just described.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "genai_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — extraction: get *text worth embedding*
# MAGIC
# MAGIC | Source | Tool | Watch for |
# MAGIC |---|---|---|
# MAGIC | PDFs / images / scans in a volume | **`ai_parse_document`** (SQL, handles layout + OCR) | tables and multi-column layouts — verify, don't trust |
# MAGIC | HTML | strip boilerplate (nav, footers, cookie banners) | boilerplate that survives gets retrieved forever |
# MAGIC | wikis/markdown | keep the **heading structure** — it powers chunking | |
# MAGIC | spreadsheets/tables | serialize rows to sentences or keep as structured data | naive text-dumps of tables embed terribly |
# MAGIC
# MAGIC ```sql
# MAGIC SELECT path, ai_parse_document(content) AS parsed
# MAGIC FROM READ_FILES('/Volumes/dev_catalog/genai_assoc/raw_docs', format => 'binaryFile')
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Filter **before** chunking: dedupe near-identical docs, drop obsolete
# MAGIC >   versions, scrub PII (section 05 — *before embedding*, not after).
# MAGIC > - Keep `source`, `section`, `updated_at` as **metadata columns** — they
# MAGIC >   become retrieval filters and citations later. Discarding them during
# MAGIC >   extraction is unrecoverable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — chunking, run for real
# MAGIC Tokens ≈ words × 1.3 (English). Both chunkers below are pure Python —
# MAGIC deterministic and runnable anywhere.

# COMMAND ----------

DOC = """# Setup
Install the agent with pip. Configure OAuth credentials in the settings page.
Restart after configuration. Verify the connection with the test button.
# Usage
Ask questions in plain language. The agent cites its sources.
Review answers before forwarding to customers. Flag bad answers with thumbs down.
# Limits
Do not paste customer PII into prompts. Rate limits apply per user.
Long documents are truncated at fifty pages. Contact support for quota increases."""

def fixed_size_chunks(text, max_words=28, overlap_words=7):
    words = text.replace("\n", " ").split()
    step = max_words - overlap_words
    return [" ".join(words[i:i + max_words]) for i in range(0, len(words), step)
            if words[i:i + max_words]]

def structure_chunks(text):
    parts = [p.strip() for p in text.split("# ") if p.strip()]
    return [f"# {p}" for p in parts]

fixed = fixed_size_chunks(DOC)
structured = structure_chunks(DOC)

print(f"fixed-size w/ overlap: {len(fixed)} chunks")
print(f"  e.g. …{fixed[0][-40:]!r} / {fixed[1][:40]!r}…  <- overlap preserves the seam")
print(f"structure-aware:       {len(structured)} chunks (one per section)")
for c in structured:
    print(f"  {c.splitlines()[0]} ({len(c.split())} words)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the trade-off you just ran
# MAGIC
# MAGIC | | Fixed + overlap | Structure-aware |
# MAGIC |---|---|---|
# MAGIC | topic purity | can straddle sections | ✅ one topic per chunk |
# MAGIC | size uniformity | ✅ predictable token budget | varies with the doc |
# MAGIC | needs | nothing | real structure in the source |
# MAGIC | overlap | protects sentences cut at borders | n/a (borders are semantic) |
# MAGIC
# MAGIC And the two failure directions to diagnose from symptoms:
# MAGIC **too big** → retrieval imprecise (chunk matches on one topic, drags in
# MAGIC three others), fewer chunks fit the context budget;
# MAGIC **too small** → fragments with no context ("it must be restarted" —
# MAGIC what must?), answers that need info split across chunks fail.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Chunk size must respect the **embedding model's input limit**
# MAGIC >   (GTE-large ≈ 512 tokens) — oversized chunks get silently truncated
# MAGIC >   at embedding time, and the *unembedded tail is unretrievable*.
# MAGIC > - **Parent-document / small-to-big**: embed small chunks for precise
# MAGIC >   matching, hand the LLM the surrounding parent section — the named
# MAGIC >   fix when precision and context fight.
# MAGIC > - Changing chunking = **re-embed + re-index everything**. It's a
# MAGIC >   pipeline rerun, not a setting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — land it in Delta, CDF on

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

rows = [(f"doc1-{i}", "agent-guide.md", c.splitlines()[0].lstrip("# "), c)
        for i, c in enumerate(structured)]
chunks_df = spark.createDataFrame(  # noqa: F821
    rows, "chunk_id STRING, source STRING, section STRING, chunk_text STRING"
).withColumn("updated_at", F.current_timestamp())

(chunks_df.write.mode("overwrite")
 .option("delta.enableChangeDataFeed", "true")
 .saveAsTable("doc_chunks"))
display(spark.table("doc_chunks"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC **Change Data Feed is required** for a Delta Sync vector index (section
# MAGIC 04) — it's how the index learns which chunks changed instead of
# MAGIC re-embedding the world. The schema shape to remember:
# MAGIC `primary key + text column + metadata columns` — nothing exotic.