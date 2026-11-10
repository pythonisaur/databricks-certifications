# 06 — Evaluation and Monitoring (12%)

Two notebooks along the ship line. Pre-ship: eval sets (mined from real
traffic and Review-App thumbs-downs), deterministic scorers run live —
including the case they *can't* catch, which motivates the LLM judge — the
full `mlflow.genai.evaluate` harness with built-in and `@scorer` custom
scorers, groundedness as the anti-hallucination metric, and judge
validation. Post-ship: tracing with the which-stage-failed table, inference
tables as production's memory (scheduled scorers over sampled traffic,
eval-set mining, audit), and cost with real token arithmetic showing why
RAG spend is mostly input tokens.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_evaluation_and_monitoring.py`](01_evaluation_and_monitoring.py) | Eval-set sourcing, deterministic scorers live + their blind spot, the evaluate harness and scorer table, judge validation, the fixed-set iteration loop |
| [`02_tracing_monitoring_and_cost.py`](02_tracing_monitoring_and_cost.py) | `autolog`/`@mlflow.trace`, the trace-diagnosis table, inference-table workflows, quality vs serving-metrics layers, the cost function run live + lever table |
| [`q_5860796433_sme_feedback_eval.py`](q_5860796433_sme_feedback_eval.py) | Exam question: using review-app SME annotations. Thumbs-downs + corrections become `expected_response` rows in the eval set and `mlflow.genai.evaluate` re-runs v1 vs v2 (4/7 → 7/7); then the CSV export, the one-class RLHF reward model and the ratings-only preference score run to what they really produce. Self-contained in `dev_catalog.gen_ai_ass_qnotes` |

## How to study

1. Match metric to evidence: expected facts → Correctness; retrieved
   context → Groundedness; a rubric → Guidelines; codeable → `@scorer`.
   And always validate the judge on a labeled sample first.
2. Route symptoms to layers: bad answers → traces + scorers; slow/erroring
   endpoint → serving metrics; rising spend → Gateway usage + the token
   arithmetic.
3. Then drill this section on [TestHub](https://testlogichub.com) — 12% ≈ 5
   of 45 questions.
