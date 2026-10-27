# Databricks Generative AI Engineer Associate

Study notes organized by the official exam sections (the **March 2026
guide**), one folder per section, with notebook depth proportional to
section weight. 45 scored questions, 90 minutes.

| # | Section | Weight | Notebooks |
|---|---------|--------|-----------|
| 01 | [Design Applications](01-design-applications/) | 14% | 2 |
| 02 | [Data Preparation](02-data-preparation/) | 14% | 2 |
| 03 | [Application Development](03-application-development/) | 30% | 3 |
| 04 | [Assembling and Deploying Applications](04-assembling-and-deploying/) | 22% | 2 |
| 05 | [Governance](05-governance/) | 8% | 1 |
| 06 | [Evaluation and Monitoring](06-evaluation-and-monitoring/) | 12% | 2 |

Sections 03 + 04 are **52% of the exam**. LLM apps need endpoints to run
fully, so the notebooks split their content deliberately: the **mechanics
run live** (prompt builders, chunkers, retrieval metrics, the agent
tool-loop with a stubbed LLM, guardrail filters, PII scrubs, cost
arithmetic) while the **platform calls** (FM APIs, Vector Search,
`agents.deploy`) appear as exact, annotated API shapes you'll recognize on
the exam and can paste into a workspace that has the services.

## The mental model the whole exam hangs on

```
raw docs -> parse -> chunk -> embed -> VECTOR SEARCH INDEX      (sections 02, 04)
user query -> retrieve -> augment prompt -> LLM -> guardrails    (sections 01, 03)
        logged as a chain/agent -> UC -> Model Serving endpoint  (section 04)
        traced, evaluated (LLM-as-judge), monitored, governed    (sections 05, 06)
```

Three cross-cutting disciplines recur in every section — internalize them
once: **fix the cheapest layer first** (prompt → retrieval → architecture →
model size → fine-tuning), **measure against a fixed eval set before and
after every change**, and **controls sit at the stage where they can't be
bypassed** (PII at ingestion, guardrails at the Gateway, permissions in UC).

## Projects

- [projects/data-agent/](projects/data-agent/) — build a read-only SQL agent
  over a fake US food-delivery warehouse (sections 03, 05, 06); four
  notebooks, also the demo behind the Emma on Data data-agent video.

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 📖 [Official exam page](https://www.databricks.com/learn/certification/genai-engineer-associate)
