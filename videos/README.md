# Video companion notebooks

The notebooks behind the **Emma on Data** YouTube videos, one file per video, exported from the exact cells the
video shows. Import a file into any Databricks workspace (Workspace > Import, or clone this repo as a Git folder)
and run it top to bottom; serverless compute is enough and Free Edition works.

Each notebook maps to a section of a certification track in this repo, so after the video you can go deeper in the
matching folder.

| # | Video | Notebook | Track / section |
|---|---|---|---|
| 01 | Delta time travel (coming) | [`data-engineer-associate/01_delta_time_travel.py`](data-engineer-associate/01_delta_time_travel.py) | [DE Associate 03 — Data Transformation and Modeling](../data-engineer-associate/03-data-transformation-and-modeling/) |
| 02 | Exam question: CREATE TABLE IF NOT EXISTS vs CREATE OR REPLACE TABLE (DE Associate Q60) | [`data-engineer-associate/02_create_table_if_not_exists_vs_create_or_replace.py`](data-engineer-associate/02_create_table_if_not_exists_vs_create_or_replace.py) | [DE Associate 03 — Data Transformation and Modeling](../data-engineer-associate/03-data-transformation-and-modeling/) |
| 03 | [Liquid Clustering vs Z-ORDER: boxes, SQL and an exam question (DE Associate Q46)](https://www.youtube.com/watch?v=6CESMQ9tl0I) | [`data-engineer-associate/03_liquid_clustering_vs_zorder.py`](data-engineer-associate/03_liquid_clustering_vs_zorder.py) | [DE Associate 01 — Databricks Intelligence Platform](../data-engineer-associate/01-databricks-intelligence-platform/) |
| 04 | Lesson 01: the LCEL pipe, `prompt \| llm \| StrOutputParser()` (GenAI Engineer Associate, Section 4) (coming) | [`generative-ai-engineer-associate/04-assembling-and-deploying/lesson01_lcel_pipe_chain.py`](../generative-ai-engineer-associate/04-assembling-and-deploying/lesson01_lcel_pipe_chain.py) (moved into the track; the old path keeps a pointer) | [GenAI Associate 04 — Assembling and Deploying](../generative-ai-engineer-associate/04-assembling-and-deploying/) |
| 05 | Lesson 02: RAG in one chain, retriever, `RunnablePassthrough()`, `RunnableParallel` (GenAI Engineer Associate, Section 3) (coming) | [`generative-ai-engineer-associate/03-application-development/lesson02_rag_in_one_chain.py`](../generative-ai-engineer-associate/03-application-development/lesson02_rag_in_one_chain.py) | [GenAI Associate 03 — Application Development](../generative-ai-engineer-associate/03-application-development/) |

Conventions:

- GenAI Engineer Associate lesson notebooks live **inside the track**, in the section folder they prove
  (`generative-ai-engineer-associate/NN-section/lessonNN_<slug>.py`); this table only points to them.

- Folders follow the track names used in this repo (`data-engineer-associate/`, `data-engineer-professional/`, ...).
- Files are numbered in release order and stored as Databricks notebook source (`# Databricks notebook source`).
- Every cell in a video notebook was run before publishing; the outputs you see in the video are real.
- Found a mistake? Open an issue with the video number and the cell.
