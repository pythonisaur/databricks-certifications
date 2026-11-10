# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SME review-app feedback: from thumbs-down to eval example
# MAGIC
# MAGIC Generative AI Engineer Associate, section 6 of the exam guide, "Evaluation and Monitoring".
# MAGIC Objective: incorporate SME feedback to improve agent performance.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the agent is a deterministic stand-in (a lookup table playing the
# MAGIC LLM), the review-app annotations are written by the notebook into a Delta table, and `mlflow.genai.evaluate`
# MAGIC runs with code-based scorers, so no serving endpoint and no LLM judge are needed. Everything lives in
# MAGIC `dev_catalog.gen_ai_ass_qnotes`; nothing else is touched.
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A team uses the Databricks Agent Evaluation review app to collect SME ratings on their deployed agent's
# MAGIC responses. Subject matter experts annotate responses with thumbs-up/thumbs-down ratings and optional
# MAGIC free-text corrections. Which Databricks-native workflow BEST describes how to use these annotations to
# MAGIC improve the agent in the next iteration?
# MAGIC
# MAGIC * **A** Export the SME annotations from the review app to a CSV file and send it to the model vendor to request a custom fine-tuned model
# MAGIC * **B** Use the thumbs-down annotations as negative examples to train an RLHF reward model, and retire the offline evaluation dataset
# MAGIC * **C** Drop the free-text corrections and use the thumbs-up/thumbs-down ratings to compute a preference score per agent version in MLflow
# MAGIC * **D** Add the thumbs-down annotations, with their corrections as expected responses, to the evaluation dataset and re-run mlflow.evaluate()
# MAGIC
# MAGIC The correct workflow (D) is built and run first: annotations become labeled examples, the evaluation dataset
# MAGIC grows, and the evaluation is re-run against the old and the fixed agent. Then A, B and C run on the same
# MAGIC annotations, each to the output (or the failure) it really produces.

# COMMAND ----------

# DBTITLE 1,Question metadata
QUESTION = {
    "key": "5860796433",
    "track": "gen_ai_ass",
    "section": "Evaluation and Monitoring",
    "stem": "A team uses the Databricks Agent Evaluation review app to collect SME ratings on their deployed agent's responses. Subject matter experts annotate responses with thumbs-up/thumbs-down ratings and optional free-text corrections. Which Databricks-native workflow BEST describes how to use these annotations to improve the agent in the next iteration?",
    "correct": "D",
    "options": {
        "A": "Export the SME annotations from the review app to a CSV file and send it to the model vendor to request a custom fine-tuned model",
        "B": "Use the thumbs-down annotations as negative examples to train an RLHF reward model, and retire the offline evaluation dataset",
        "C": "Drop the free-text corrections and use the thumbs-up/thumbs-down ratings to compute a preference score per agent version in MLflow",
        "D": "Add the thumbs-down annotations, with their corrections as expected responses, to the evaluation dataset and re-run mlflow.evaluate()"
    },
    "objective": "Incorporate SME feedback to improve agent performance",
    "source": "testlogichub exams/fixes/qfix/5860796433.py"
}

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow[databricks]>=3.1" scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Widgets and the sandbox schema
dbutils.widgets.text("catalog", "dev_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "gen_ai_ass_qnotes"
PREFIX = "q5860796433"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
ANNOTATIONS_TABLE = f"{CATALOG}.{SCHEMA}.{PREFIX}_review_annotations"
EVAL_TABLE = f"{CATALOG}.{SCHEMA}.{PREFIX}_eval_augmented"
SFT_TABLE = f"{CATALOG}.{SCHEMA}.{PREFIX}_sft_pairs"
print("tables:", ANNOTATIONS_TABLE, EVAL_TABLE, SFT_TABLE, sep="\n  ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: agent v1, its offline evaluation set, and what the SMEs said about it
# MAGIC
# MAGIC The agent is a support assistant for an online store. Version 1 is "deployed"; its offline evaluation set has
# MAGIC four questions and v1 passes all four. Then SMEs open the review app and rate six real responses. Three get a
# MAGIC thumbs-down, each with a free-text correction: the answer the expert says the agent should have given.
# MAGIC
# MAGIC The agent is a stand-in: a dictionary from question to answer plays the LLM, so every run of this notebook
# MAGIC prints the same numbers. Version 2 is v1 with its knowledge (in real life: the system prompt or the retrieved
# MAGIC policy documents) updated.

# COMMAND ----------

# DBTITLE 1,The agent stand-in: v1 (deployed) and v2 (next iteration)
KB_V1 = {
    # questions the offline eval set already covers: v1 is right
    "How do I track my order?": "Open Orders in your account and select the order to see its tracking link.",
    "How do I reset my password?": "Select Forgot password on the sign-in page and follow the emailed link.",
    "How long is the warranty on electronics?": "Electronics carry a 2 year manufacturer warranty.",
    "Can I pick up my order in store?": "Yes, choose Store pickup at checkout and collect it within 7 days.",
    # questions real users asked after deployment
    "How many days do I have to return an item?": "You can return items within 14 days of delivery.",
    "Is shipping free for Plus members?": "Shipping costs $4.99 for all orders.",
    "Do gift cards expire?": "Gift cards expire after 12 months.",
    "Can I change my delivery address after the order ships?": "No, once the order ships the address cannot be changed; contact the carrier to redirect it.",
    "How do I cancel my Plus subscription?": "Go to Account, then Plus, then select Cancel subscription.",
    "Which payment methods do you accept?": "We accept credit cards, debit cards, PayPal and gift cards.",
}
# The fix for the next iteration: corrected policy facts (prompt / retrieval change, no model training).
KB_V2 = {**KB_V1,
    "How many days do I have to return an item?": "You can return items within 30 days of delivery.",
    "Is shipping free for Plus members?": "Shipping is free for Plus members on every order.",
    "Do gift cards expire?": "Gift cards never expire.",
}

def agent_v1(question: str) -> str:
    return KB_V1.get(question, "I don't know.")

def agent_v2(question: str) -> str:
    return KB_V2.get(question, "I don't know.")

# COMMAND ----------

# DBTITLE 1,The offline evaluation set the team already had
import pandas as pd

offline_eval = pd.DataFrame([
    {"question": "How do I track my order?", "expected_response": "Open Orders in your account and select the order to see its tracking link.", "source": "offline"},
    {"question": "How do I reset my password?", "expected_response": "Select Forgot password on the sign-in page and follow the emailed link.", "source": "offline"},
    {"question": "How long is the warranty on electronics?", "expected_response": "Electronics carry a 2 year manufacturer warranty.", "source": "offline"},
    {"question": "Can I pick up my order in store?", "expected_response": "Yes, choose Store pickup at checkout and collect it within 7 days.", "source": "offline"},
])
offline_eval

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where the annotations come from (platform calls, shown, not run)
# MAGIC
# MAGIC Deploying an agent creates its review app, and a labeling session sends traces to named SMEs:
# MAGIC
# MAGIC ```python
# MAGIC from databricks import agents
# MAGIC agents.deploy(model_name="dev_catalog.gen_ai_ass_qnotes.support_agent", model_version=1)  # serving endpoint + review app
# MAGIC
# MAGIC import mlflow.genai.labeling as labeling
# MAGIC session = labeling.create_labeling_session(name="support_agent_v1_review",
# MAGIC                                            assigned_users=["sme1@example.com", "sme2@example.com"])
# MAGIC session.add_traces(mlflow.search_traces(max_results=50))
# MAGIC ```
# MAGIC
# MAGIC The review app integrates with Unity Catalog: every rating and free-text correction is persisted in a Delta
# MAGIC table (the assessments are also attached to the MLflow traces). Free Edition cannot deploy an agent endpoint,
# MAGIC so the next cell writes the same shape of table directly: question, agent response, rating, correction.

# COMMAND ----------

# DBTITLE 1,Stand-in for the review app's Delta table of SME annotations
annotations = pd.DataFrame([
    {"question": "How many days do I have to return an item?", "rating": "thumbs_down", "reviewer": "sme1@example.com",
     "correction": "You can return items within 30 days of delivery."},
    {"question": "Is shipping free for Plus members?", "rating": "thumbs_down", "reviewer": "sme2@example.com",
     "correction": "Shipping is free for Plus members on every order."},
    {"question": "Do gift cards expire?", "rating": "thumbs_down", "reviewer": "sme1@example.com",
     "correction": "Gift cards never expire."},
    {"question": "Can I change my delivery address after the order ships?", "rating": "thumbs_up", "reviewer": "sme2@example.com", "correction": None},
    {"question": "How do I cancel my Plus subscription?", "rating": "thumbs_up", "reviewer": "sme1@example.com", "correction": None},
    {"question": "Which payment methods do you accept?", "rating": "thumbs_up", "reviewer": "sme2@example.com", "correction": None},
])
annotations.insert(1, "agent_version", "v1")
annotations.insert(2, "response", annotations["question"].map(agent_v1))

spark.createDataFrame(annotations).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ANNOTATIONS_TABLE)
display(spark.table(ANNOTATIONS_TABLE))

# COMMAND ----------

# DBTITLE 1,One code-based scorer: does the response say what the expected response says?
import re
import mlflow
from mlflow.genai.scorers import scorer

STOP = {"the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "is", "are", "you", "your", "it", "can", "be", "all", "every"}

def _tokens(text):
    return [t for t in re.findall(r"[a-z0-9$.]+", str(text).lower().replace(".", " ")) if t not in STOP]

def matches(output, expected):
    """True when every number in the expected response appears and >= 80% of its content words do.
    None when there is no expected response to compare against."""
    if expected is None or (isinstance(expected, float) and pd.isna(expected)):
        return None
    exp, out = _tokens(expected), set(_tokens(output))
    numbers_ok = all(t in out for t in exp if any(c.isdigit() for c in t))
    recall = sum(t in out for t in exp) / max(len(exp), 1)
    return bool(numbers_ok and recall >= 0.8)

@scorer
def matches_expected(outputs, expectations):
    return matches(outputs, (expectations or {}).get("expected_response"))

print(matches(agent_v1("Do gift cards expire?"), "Gift cards never expire."),
      matches(agent_v2("Do gift cards expire?"), "Gift cards never expire."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Option D (correct): thumbs-down + correction = a new labeled evaluation example
# MAGIC
# MAGIC The recommended workflow, step by step:
# MAGIC
# MAGIC 1. Read the stored annotations. Each thumbs-down with its free-text correction becomes a
# MAGIC    `question` + `expected_response` pair.
# MAGIC 2. Append those pairs to the offline evaluation dataset (the one registered for MLflow Agent Evaluation).
# MAGIC 3. Re-run the evaluation with the augmented dataset against the updated agent, to measure whether the specific
# MAGIC    failure modes the SMEs found are resolved, without breaking what already worked.
# MAGIC 4. Optionally, curate the best pairs as supervised fine-tuning data for a future model version.

# COMMAND ----------

# DBTITLE 1,Steps 1 and 2: annotations become eval examples, the eval set grows
from pyspark.sql import functions as F

new_examples = (spark.table(ANNOTATIONS_TABLE)
    .where((F.col("rating") == "thumbs_down") & F.col("correction").isNotNull())
    .select("question", F.col("correction").alias("expected_response"), F.lit("sme_review").alias("source")))

augmented = spark.createDataFrame(offline_eval).unionByName(new_examples)
augmented.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(EVAL_TABLE)

eval_df = spark.table(EVAL_TABLE).toPandas()
print(f"offline examples: {len(offline_eval)} | from SME thumbs-downs: {new_examples.count()} | augmented: {len(eval_df)}")
eval_df

# COMMAND ----------

# DBTITLE 1,Register it as an MLflow evaluation dataset (Unity Catalog backed), when the workspace allows it
records = [{"inputs": {"question": r.question}, "expectations": {"expected_response": r.expected_response}}
           for r in eval_df.itertuples()]
try:
    from mlflow.genai import datasets
    ds = datasets.create_dataset(uc_table_name=f"{CATALOG}.{SCHEMA}.{PREFIX}_eval_dataset")
    ds.merge_records(records)
    print("registered evaluation dataset:", ds.name)
except Exception as e:  # already exists on a re-run, or not available in this workspace
    print("kept as the Delta table", EVAL_TABLE, "-", type(e).__name__, str(e)[:120])

# COMMAND ----------

# DBTITLE 1,Step 3: re-run the evaluation, old agent vs updated agent, on the augmented set
def evaluate(agent, data, label):
    res = mlflow.genai.evaluate(data=data, predict_fn=lambda question: agent(question), scorers=[matches_expected])
    score = {k: round(v, 3) for k, v in res.metrics.items() if k.startswith("matches_expected")}
    print(f"{label:28s} {score}")
    return res

res_v1 = evaluate(agent_v1, records, "v1 on the augmented set")
res_v2 = evaluate(agent_v2, records, "v2 on the augmented set")

# COMMAND ----------

# DBTITLE 1,Row by row: the SME-found failures, before and after
rows = eval_df.assign(
    v1_pass=[matches(agent_v1(q), e) for q, e in zip(eval_df.question, eval_df.expected_response)],
    v2_pass=[matches(agent_v2(q), e) for q, e in zip(eval_df.question, eval_df.expected_response)],
)
print(rows.groupby("source")[["v1_pass", "v2_pass"]].mean())
rows[["source", "question", "v1_pass", "v2_pass"]]

# COMMAND ----------

# MAGIC %md
# MAGIC Read the output. On the four offline rows v1 and v2 both pass: the old dataset alone could never have told
# MAGIC the two versions apart, which is exactly why SME review is worth collecting. On the three `sme_review` rows v1
# MAGIC fails all three (that is the failure the experts saw) and v2 passes all three. The overall
# MAGIC `matches_expected/mean` moves from 4/7 to 7/7, and the offline rows prove the fix broke nothing. That is
# MAGIC option **D**: the corrections become `expected_response` values, the evaluation dataset grows, and
# MAGIC re-running `mlflow.evaluate()` (`mlflow.genai.evaluate()` in MLflow 3) measures the next iteration on the
# MAGIC exact failure modes the SMEs identified. The improvement itself came from the cheapest lever, a prompt /
# MAGIC knowledge change; nothing was trained.

# COMMAND ----------

# DBTITLE 1,Step 4 (optional): curated pairs as supervised fine-tuning data, kept in Unity Catalog
sft = spark.table(EVAL_TABLE).where("source = 'sme_review'").select(
    F.array(
        F.struct(F.lit("user").alias("role"), F.col("question").alias("content")),
        F.struct(F.lit("assistant").alias("role"), F.col("expected_response").alias("content")),
    ).alias("messages"))
sft.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SFT_TABLE)
display(spark.table(SFT_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Option A: export to CSV and send it to the model vendor
# MAGIC
# MAGIC Misconception: "SME feedback is training data for someone else's model." Exporting to CSV and sharing with
# MAGIC an external vendor is operationally slow (weeks for a custom model, versus minutes for a prompt fix),
# MAGIC introduces data governance risk, and bypasses the Databricks-native annotation-to-evaluation pipeline the
# MAGIC review app is designed to support.

# COMMAND ----------

# DBTITLE 1,What leaves the workspace, and what the next iteration can measure
import os

os.makedirs("/tmp/q5860796433", exist_ok=True)
csv_path = "/tmp/q5860796433/sme_annotations_for_vendor.csv"
spark.table(ANNOTATIONS_TABLE).toPandas().to_csv(csv_path, index=False)
print("written:", csv_path, os.path.getsize(csv_path), "bytes")
print("columns that leave Unity Catalog (no grants, no lineage, no audit once emailed):", list(pd.read_csv(csv_path).columns))

# The evaluation dataset was not touched, so the evaluation the team re-runs is the old one.
offline_records = [{"inputs": {"question": q}, "expectations": {"expected_response": e}}
                   for q, e in zip(offline_eval.question, offline_eval.expected_response)]
evaluate(agent_v1, offline_records, "A: v1 on the old set")
print("v1 still scores 1.0: the three failures the SMEs found are invisible to the team's own evaluation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option B: thumbs-downs train an RLHF reward model, offline evaluation retired
# MAGIC
# MAGIC Misconception: "the most sophisticated technique is the best workflow." RLHF (Reinforcement Learning from
# MAGIC Human Feedback) is valid but complex: a reward model needs significant training infrastructure and
# MAGIC preference data. Using only thumbs-down annotations while abandoning offline evaluation and prompt engineering
# MAGIC ignores the simpler, faster levers the platform already gives you, and without the evaluation dataset there
# MAGIC is no way to measure whether the next iteration improved. Both halves run below.

# COMMAND ----------

# DBTITLE 1,A reward model from thumbs-down annotations only
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

negatives = annotations[annotations.rating == "thumbs_down"]
X = TfidfVectorizer().fit_transform(negatives.question + " " + negatives.response)
y = [0] * len(negatives)   # every example is "bad": the reward model has nothing to contrast it with
try:
    LogisticRegression().fit(X, y)
except ValueError as e:
    print("reward model training failed:", e)
print(f"{len(negatives)} examples, one class. A real RLHF reward model needs thousands of preference pairs and GPU training.")

# COMMAND ----------

# DBTITLE 1,Retire the offline evaluation dataset, then try to compare v1 and v2
eval_dataset = None   # option B "retires" it
if not eval_dataset:
    print("B: nothing to evaluate against. v1 vs v2 cannot be compared; 'did the next iteration improve?' has no answer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option C: drop the corrections, keep a preference score per version
# MAGIC
# MAGIC Misconception: "the rating is the feedback." Dropping the free-text corrections discards the most actionable
# MAGIC signal in the annotations: the expert's statement of what the correct response should have been. Binary
# MAGIC ratings alone give a quality signal, but not the expected response content needed to build evaluation
# MAGIC examples or fine-tuning pairs.

# COMMAND ----------

# DBTITLE 1,The preference score, and how many eval examples are left to build
ratings_only = spark.table(ANNOTATIONS_TABLE).drop("correction").toPandas()
pref = (ratings_only.assign(up=ratings_only.rating.eq("thumbs_up"))
        .groupby("agent_version").up.mean().reindex(["v1", "v2"]))
print("preference score per version:\n", pref.to_string())

usable = [matches_expected(outputs=r.response, expectations={}) for r in ratings_only.itertuples()]
print("scorer results without an expected response:", usable)
print("eval examples buildable from ratings only:", sum(u is not None for u in usable))

# COMMAND ----------

# MAGIC %md
# MAGIC v1 gets 0.5 and v2 gets `NaN`: a score says *that* half the answers were bad, not *what* the right answers
# MAGIC are, and it cannot score the new version until v2 is deployed and rated by people again. Every scorer call
# MAGIC returns `None` because there is no `expected_response` to compare with.
# MAGIC
# MAGIC | Option | What it produced here | Why it is not the best workflow |
# MAGIC |---|---|---|
# MAGIC | **D** corrections into the eval set, re-run evaluate | 4/7 → 7/7, offline rows still pass | correct: measures the exact SME failure modes |
# MAGIC | **A** CSV to the vendor | a file outside Unity Catalog; v1 still scores 1.0 | slow, governance risk, bypasses the native pipeline |
# MAGIC | **B** RLHF reward model, eval retired | one-class training error; nothing to compare | complex infrastructure; no way to measure improvement |
# MAGIC | **C** ratings only | 0.5 vs `NaN`; zero eval examples | throws away the expected responses |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The rule to memorise
# MAGIC
# MAGIC The review app persists SME annotations in a Unity Catalog Delta table so they can feed the evaluation loop.
# MAGIC A thumbs-down plus its free-text correction is a ready-made labeled example: `question` + `expected_response`.
# MAGIC Append those examples to the offline evaluation dataset and re-run `mlflow.evaluate()` against the updated
# MAGIC agent to prove the SME-found failures are fixed and nothing else broke. Ratings alone, exports to a vendor, or
# MAGIC RLHF without an evaluation set cannot measure the next iteration; curated pairs as fine-tuning data are an
# MAGIC optional later step, not a replacement.
