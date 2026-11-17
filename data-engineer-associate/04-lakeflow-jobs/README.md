# 04 — Working with Lakeflow Jobs (16%)

The orchestration section — third-heaviest on the exam, and new as a
standalone section in the May 2026 guide. Two notebooks: wiring the DAG
(task types, run-if conditions, retries, repair, task values, for-each) and
firing it (the five trigger types and the time-based vs data-driven
decision). Jobs are defined in the UI or bundles, so these are
reference-first with the runnable bits (task values, system-table probes)
live.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_tasks_dags_and_control_flow.py`](01_tasks_dags_and_control_flow.py) | Task types incl. if/else and for-each, `depends_on` + Run-if conditions (the "always run cleanup" answer), per-task retries vs repair runs, `dbutils.jobs.taskValues` live, job parameters vs task values, job compute choices |
| [`02_scheduling_and_triggers.py`](02_scheduling_and_triggers.py) | The five triggers with their sweet spots, why clock-chaining dependent jobs is the canonical wrong answer, pause/resume, reading a job's bundle YAML, `system.lakeflow.job_run_timeline` probe |
| [`q_e58476c849_asset_bundle_structure.py`](q_e58476c849_asset_bundle_structure.py) | Exam question on Asset Bundle structure: `databricks.yml` as the one required root file (name, targets, resources), five project trees put through stand-ins for `bundle validate` / `deploy` / `run`, and why workspace-path code, `requirements.txt`/`Makefile`, manifests and JSON specs are wrong |

## Setup

None — self-contained. To *feel* the concepts, wrap this repo's section 02
notebooks in a real two-task job with a file-arrival trigger on the
`de_assoc` landing volume (the YAML sketch in notebook 02 is exactly that).

## How to study

1. Memorize two small tables: **Run-if** conditions (All done = the cleanup
   answer) and the **trigger** table (file arrival vs continuous vs table
   update). Together they cover most of this section's questions.
2. Keep the two value channels straight: job **parameters** flow down into
   tasks; **task values** flow between tasks; **tables** carry the data.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 16% weight ≈ 7 of 45 questions.
