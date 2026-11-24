# 🎓 databricks-certifications code base

Runnable Databricks notebooks for the seven Databricks certification exams: one folder per exam, one subfolder per section of the official exam guide.

**For** anyone preparing for a Databricks certification who would rather run the code than reread slides. Each notebook builds its own small practice data, so you can predict a cell's output, run it, and check yourself.

**Runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)** (no cost, serverless compute). Three exceptions, so you are not surprised: the Data Engineer Professional lesson setups clone tables from a deployed [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter); the Machine Learning notebooks prefer an ML Runtime cluster and skip the cells that need it elsewhere; and a few Spark Developer cells call the RDD API, which serverless compute does not support.

Sister repo to [**lakehouse-starter**](https://github.com/byambaa1982/lakehouse-starter), a deployable Asset Bundle where the concepts from these exams (medallion architecture, Lakeflow Spark Declarative Pipelines (formerly DLT), CDC → SCD Type 2, Unity Catalog, bundles CI/CD) run for real. Read here, practice there.

## Start in three steps

1. **Import one exam folder** into your workspace: Workspace > Import for a single file, or clone this repo as a Git folder.
2. **Run the section's `00_setup` notebook** if the folder has one. It builds the practice data in `dev_catalog.<track schema>`; the [lab environments map](resources/lab-environments.md) lists them all. Question notebooks (`q_*.py`) set up their own data.
3. **Predict each cell's output before you run it.** The exams show code and ask what happens, so this is the skill that gets tested.

## Certification tracks

| Folder | Certification (official exam page and guide) | Notebooks | Status |
|---|---|---|---|
| [`data-engineer-associate/`](data-engineer-associate/) | [Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate) | 18 | ✅ Earned — [credential](https://credentials.databricks.com/056070f5-1581-4e24-8996-82b1f1e23b20#acc.3C6pjHkb) |
| [`data-engineer-professional/`](data-engineer-professional/) | [Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional) | 43 | ✅ Earned — [credential](https://credentials.databricks.com/d9114e66-f556-4da1-a383-fc65cb512027#acc.8lQgipFn) |
| [`data-analyst-associate/`](data-analyst-associate/) | [Data Analyst Associate](https://www.databricks.com/learn/certification/data-analyst-associate) | 13 | 📚 Studying |
| [`machine-learning-associate/`](machine-learning-associate/) | [Machine Learning Associate](https://www.databricks.com/learn/certification/machine-learning-associate) | 9 | 📚 Studying |
| [`machine-learning-professional/`](machine-learning-professional/) | [Machine Learning Professional](https://www.databricks.com/learn/certification/machine-learning-professional) | 7 | 📚 Studying |
| [`generative-ai-engineer-associate/`](generative-ai-engineer-associate/) | [Generative AI Engineer Associate](https://www.databricks.com/learn/certification/genai-engineer-associate) | 25 | 📚 Studying |
| [`apache-spark-developer-associate/`](apache-spark-developer-associate/) | [Associate Developer for Apache Spark](https://www.databricks.com/learn/certification/apache-spark-developer-associate) | 7 | 📚 Studying |
| [`videos/`](videos/) | Index of the companion notebooks for the [Emma on Data](https://www.youtube.com/@emmaondata) videos; GenAI lesson notebooks sit inside their track's section folders | 4 | 🎬 Growing |
| [`resources/`](resources/) | Cross-exam material: the [cheat sheet](resources/cross-exam-cheat-sheet.md), [lab environments map](resources/lab-environments.md), [exam-day notes](resources/exam-day-notes.md) | — | — |

Notebook counts are the `.py` files in each folder, setup notebooks included. A new question notebook lands most days, so the folders may be slightly ahead of this table.

Every certification folder follows the same shape:

```
<certification>/
├── README.md                      # the exam's sections + weights, mapped to folders
└── NN-<section-name>/             # one folder per OFFICIAL exam-guide section
    ├── README.md                  # run order, setup, how to study, the section's traps
    ├── 00_setup.py                # (where needed) builds deterministic practice data
    └── NN_<topic>.py              # runnable Databricks notebooks with exam-trap callouts
```

Notebooks are stored as `.py` source (`# Databricks notebook source`) —
import a folder into your workspace and run them. Practice data is
hash-deterministic with blemishes planted where lessons need them; the
[lab environments map](resources/lab-environments.md) shows what each track
builds and how to reset it.

## How I study

1. **Read the official exam guide first** — every exam page above links one. The section folders mirror its headings and weights, so coverage gaps are visible.
2. **Do it, don't just read it** — every concept that can run, runs: predict each cell's output before executing it, because that's exactly the skill the exams test. Labs lean on a [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) dev deployment (serverless — a run costs cents).
3. **Drill with timed practice tests** — I built [TestHub](https://testlogichub.com) for exactly this: timed practice exams with explanations, starting with Data Engineer Associate.

## Related

- 🏗️ [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) — production-grade Asset Bundle template (the lab environment for this repo)
- 🎯 [TestHub](https://testlogichub.com) — timed practice exams with explanations
- 📚 [Official Databricks certification overview](https://www.databricks.com/learn/certification)

## About

Maintained by **[Byamba Enkhbat](https://github.com/byambaa1982)** — Databricks Certified Data Engineer (Associate · Professional), building Lakehouse pipelines in production since 2018.

Need help with Databricks, data pipelines, or AI automation? **[Work with me →](https://datalogichub.com/work-with-me)**

---

⭐ If these notes help you pass, a star helps others find them.
