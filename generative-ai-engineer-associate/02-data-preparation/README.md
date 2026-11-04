# 02 — Data Preparation (14%)

Two notebooks that actually run the section instead of describing it.
First, extraction and chunking: `ai_parse_document` and per-source-type
extraction guidance, then a token-aware fixed-size chunker (with visible
overlap at the seams) and a structure-aware chunker executed on the same
document, the trade-off read off their real output, and the corpus landed
in Delta with CDF on. Second, retrieval evaluation as its own discipline:
a labeled eval set, recall@k and MRR computed live on two retrieval
configs — including the case where both have perfect recall and only MRR
tells them apart — plus re-ranking, hybrid search, and embedding-model
rules.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_chunking_and_retrieval_prep.py`](01_chunking_and_retrieval_prep.py) | Extraction per source type, both chunkers run side by side, the size trade-offs and the embedding-input-limit truncation trap, parent-document retrieval, Delta + CDF landing |
| [`02_retrieval_evaluation.py`](02_retrieval_evaluation.py) | Building an eval set, recall@k / precision@k / MRR / NDCG computed live, retrieval-vs-generation triage, re-ranker reasoning, hybrid search, embedding-model consistency |
| [`lesson03_chunking_record_count.py`](lesson03_chunking_record_count.py) | Emma on Data lesson 03 (official sample question 1): chunk size and overlap run on a real document and projected to 150 million records; the two correct actions first, the three distractors in one table, the structure-aware splitter and the embedding context length as the limits |

## How to study

1. Diagnose from symptoms: imprecise retrieval → chunks too big; context-
   free fragments → too small; right chunk ranked low → re-rank; exact
   terms missed → hybrid. Each symptom is one question.
2. Internalize the triage order: score retrieval alone before touching
   prompts or models — "high recall, bad answers" and "low recall" lead to
   different halves of the exam.
3. Then drill this section on [TestHub](https://testlogichub.com) — 14% ≈ 6
   of 45 questions.
