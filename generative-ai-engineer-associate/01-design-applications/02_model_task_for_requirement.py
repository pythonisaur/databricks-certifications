# Databricks notebook source
# MAGIC %md
# MAGIC # Which model task fits the requirement? (summarize, classify, extract, generate)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 1 of the exam guide, "Select model tasks to
# MAGIC accomplish a given business requirement". Official sample question 5 is this exact shape.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: every cell is SQL, and the AI Functions (`ai_summarize`,
# MAGIC `ai_classify`, `ai_extract`, `ai_gen`) run on Databricks-hosted models with nothing to deploy.
# MAGIC Outputs shown in the video were captured on 11/04/2026.
# MAGIC
# MAGIC **The exam question this lesson answers** (official sample question 5)
# MAGIC
# MAGIC A Generative AI Engineer would like to build an application that can update a memo field that is about a
# MAGIC paragraph long to just a single sentence gist that shows the intent of the memo field, but fits into their
# MAGIC application front end. With which Natural Language Processing task category should they evaluate potential
# MAGIC LLMs for this application?
# MAGIC
# MAGIC * **A** text2text Generation
# MAGIC * **B** Sentencizer
# MAGIC * **C** Text Classification
# MAGIC * **D** Summarization
# MAGIC
# MAGIC The correct task (D) runs first on four real memos. Then the three wrong tasks run on the same memos, each
# MAGIC to the output it really produces, with one line on why that output is not what the requirement asked for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: four memos, each about a paragraph long
# MAGIC
# MAGIC The requirement has a shape: **a paragraph in, one sentence out, and the sentence must keep the intent**.
# MAGIC Keep that shape in mind; it is what decides the task.

# COMMAND ----------

# DBTITLE 1,The memos
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW memos AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Team, the office kitchen fridge will be emptied every Friday at 5 pm starting next week because of repeated complaints about spoiled food. Label anything you want to keep with your name and the date; unlabelled items will be discarded. Facilities will also replace the broken ice maker during the same visit. Questions go to the facilities inbox, not to HR.'),
# MAGIC   (2, 'Reminder that the Q3 expense reports are due on the 15th. Finance has moved to the new portal, so the old email submissions will bounce. Receipts above 25 dollars still need to be attached as images. Late reports will be paid in the following month, no exceptions this quarter because of the audit.'),
# MAGIC   (3, 'We are migrating the customer analytics workspace to Unity Catalog over the weekend of the 20th. Notebooks keep working, but any job that writes to the legacy hive_metastore schema must be repointed to the new catalog before Friday. The data platform team will be on call Saturday morning. A checklist is pinned in the channel.'),
# MAGIC   (4, 'Following the engagement survey, the people team is piloting a four day week for the support organisation in November. Shifts will be rebalanced so that coverage stays the same. Managers should collect preferences by the end of the month. Nothing changes for other departments yet; a decision on a wider rollout comes in January.')
# MAGIC AS t(memo_id, memo);
# MAGIC
# MAGIC SELECT memo_id, length(memo) AS chars, memo FROM memos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Summarization (option D): the task whose input and output match the requirement
# MAGIC
# MAGIC Summarization takes a longer text and returns a shorter text that keeps the meaning. `ai_summarize` is
# MAGIC that task as a SQL function: one call per row, a word budget as the second argument. Evaluating a
# MAGIC candidate model against this task means asking "does the short text keep the intent of the long one", and
# MAGIC that is exactly what the memo field needs.

# COMMAND ----------

# DBTITLE 1,The correct task, run first: a paragraph in, a gist out
# MAGIC %sql
# MAGIC SELECT memo_id,
# MAGIC        ai_summarize(memo, 15) AS gist
# MAGIC FROM memos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The three wrong tasks, each run to its real output
# MAGIC
# MAGIC | Option | What the task returns | Why it misses the requirement |
# MAGIC |---|---|---|
# MAGIC | C Text classification | one label from a list you provide | a label is a category, not a sentence with the memo's intent |
# MAGIC | B Sentencizer | the text split into its sentences | the first sentence is whatever the writer typed first, not the gist |
# MAGIC | A text2text generation | new text answering the prompt | open-ended: it answers or expands the memo unless you build the condensing yourself |

# COMMAND ----------

# DBTITLE 1,Option C: classification returns a label, not a gist
# MAGIC %sql
# MAGIC SELECT memo_id,
# MAGIC        ai_classify(memo, array('facilities', 'finance', 'it', 'hr')) AS label
# MAGIC FROM memos;

# COMMAND ----------

# DBTITLE 1,Option B: a sentencizer splits; the first sentence is not the intent
# MAGIC %sql
# MAGIC SELECT memo_id,
# MAGIC        size(split(memo, '(?<=[.!?])\\s+')) AS sentences,
# MAGIC        split(memo, '(?<=[.!?])\\s+')[0]    AS first_sentence
# MAGIC FROM memos;

# COMMAND ----------

# DBTITLE 1,Option A: generation answers the memo instead of condensing it
# MAGIC %sql
# MAGIC SELECT memo_id,
# MAGIC        length(memo)        AS memo_chars,
# MAGIC        length(ai_gen(memo)) AS generated_chars,
# MAGIC        ai_gen(memo)        AS generated
# MAGIC FROM memos
# MAGIC WHERE memo_id = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Generation is the family every chat model belongs to, so of course a prompt like "rewrite this in one
# MAGIC sentence" would work. The question asks which **task category to evaluate candidate models against**, and
# MAGIC the answer is the category whose inputs, outputs and metrics match the requirement. For "paragraph in, gist
# MAGIC out" that is summarization: its benchmarks and judges measure whether the short text keeps the meaning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The other requirement shapes the exam uses, each run live
# MAGIC
# MAGIC The same move works for every "which task" question: write down what goes in and what must come out.
# MAGIC
# MAGIC | Requirement in the question | In | Out | Task | SQL door |
# MAGIC |---|---|---|---|---|
# MAGIC | route 50,000 complaints a day into five queues | text | one of five labels | **text classification** | `ai_classify` |
# MAGIC | fill a case database from contract PDFs | text | named fields | **information extraction** | `ai_extract` |
# MAGIC | a memo field to a one-sentence gist | text | shorter text, same intent | **summarization** | `ai_summarize` |
# MAGIC | "is the review positive" | text | positive / negative / neutral / mixed | **sentiment analysis** | `ai_analyze_sentiment` |
# MAGIC | the same notice in Spanish | text | text in another language | **translation** | `ai_translate` |

# COMMAND ----------

# DBTITLE 1,Classification: five complaints into five queues
# MAGIC %sql
# MAGIC SELECT complaint,
# MAGIC        ai_classify(complaint, array('account_access', 'fraud', 'fees_and_charges', 'loan_services', 'general_inquiry')) AS queue
# MAGIC FROM VALUES
# MAGIC   ('I have been locked out of online banking since Tuesday and the reset link never arrives.'),
# MAGIC   ('There are three card payments in Lisbon on my statement and I have never been to Portugal.'),
# MAGIC   ('Why was I charged a 35 dollar maintenance fee when my balance never dropped below the minimum?'),
# MAGIC   ('Can I move my car loan payment date from the 1st to the 15th?'),
# MAGIC   ('What are the opening hours of the Lincoln Park branch on Saturdays?')
# MAGIC AS t(complaint);

# COMMAND ----------

# DBTITLE 1,Extraction: named fields out of a contract paragraph
# MAGIC %sql
# MAGIC SELECT fields.*
# MAGIC FROM (
# MAGIC   SELECT ai_extract(contract, array('party_names', 'effective_date', 'governing_law_jurisdiction', 'total_contract_value', 'payment_terms')) AS fields
# MAGIC   FROM VALUES
# MAGIC     ('This Services Agreement between Northwind Traders LLC and Contoso Ltd is effective 1 March 2026 and is governed by the laws of the State of Illinois. The total contract value is 48,000 dollars, payable net 30 against monthly invoices.'),
# MAGIC     ('Master Supply Agreement dated 12 January 2026 between Fabrikam Inc, Tailspin Toys and Wide World Importers, governed by English law, for an aggregate value of 1.2 million dollars with 50 percent due on signature and 50 percent on delivery.')
# MAGIC   AS t(contract)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Two more task names you will see: sentiment and translation
# MAGIC %sql
# MAGIC SELECT review,
# MAGIC        ai_analyze_sentiment(review)   AS sentiment,
# MAGIC        ai_translate(review, 'es')     AS spanish
# MAGIC FROM VALUES
# MAGIC   ('Great driver, terrible soup.'),
# MAGIC   ('Fast, hot and exactly what I ordered.')
# MAGIC AS t(review);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Why the task name matters after the exam: it picks the models and the metric
# MAGIC
# MAGIC | Task | Models you shortlist | How you judge them |
# MAGIC |---|---|---|
# MAGIC | summarization | any instruction-tuned chat model, or a summarization fine-tune | does the summary keep the meaning (ROUGE against references, or an LLM judge for faithfulness) |
# MAGIC | text classification | a chat model with the label list, or a small classifier | accuracy and F1 against a labelled set |
# MAGIC | information extraction | a chat model with structured output (lesson 01 of this section) | field-level exact match |
# MAGIC | text generation | a chat model | human or LLM-judge ratings of the generated text |
# MAGIC
# MAGIC The task-specific AI Functions are the SQL door to these tasks. When you need a custom prompt or a
# MAGIC specific endpoint, `ai_query` is the general one; in Python the same tasks are a prompt and a chat model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The rule to memorise
# MAGIC
# MAGIC Name the shape of the input and the output before you look at the options. Paragraph in, shorter text with
# MAGIC the same intent out: summarization. Text in, one of N labels out: classification. Text in, named fields out:
# MAGIC extraction. Open-ended text out: generation. The task category is what you evaluate candidate models against,
# MAGIC so it has to match the requirement's shape, not just produce something that looks right once.
