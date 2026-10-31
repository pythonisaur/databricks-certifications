# 04 — Assembling and Deploying Applications (22%)

Two notebooks, split where the exam splits: the index and the ship-path.
Vector Search in working detail — endpoint vs index, Delta Sync vs Direct
Access, managed vs self-managed embeddings, triggered vs continuous sync,
all four query variants (semantic/hybrid/filtered/vector) and a
troubleshooting table. Then the deployment pipeline: models-from-code with
`resources` as the auth answer, `ChatAgent` interfaces, alias →
`agents.deploy` with the Review App, the Prompt Registry, `ai_query` for
batch inference, and MCP.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_vector_search_and_deployment.py`](01_vector_search_and_deployment.py) | The endpoint/index object model, the three index decisions, the full query API surface, k as a budget, the symptom → cause troubleshooting table |
| [`lesson01_lcel_pipe_chain.py`](lesson01_lcel_pipe_chain.py) | **Video lesson 01** ([watch](https://www.youtube.com/watch?v=WqBvH1UEC2Y)): `prompt \| llm \| StrOutputParser()` on a Foundation Model endpoint, and why `+`, `LLMChain` and `StrOutputParser(prompt, llm)` are not LCEL |
| [`02_logging_registering_serving.py`](02_logging_registering_serving.py) | `chain.py` + `set_model` + `log_model(resources=[...])`, register/alias/`agents.deploy`, Review App and inference tables, Prompt Registry versioning, `ai_query` and the `ai_*` functions, managed MCP servers |

## How to study

1. The auth chain is the most-missed fact: notebook works (your identity) →
   endpoint 403s (served identity) → because `resources` weren't declared
   at logging time. Rehearse that causal chain.
2. Keep the sync modes and embedding ownership straight: nightly docs →
   TRIGGERED + `sync()` in the pipeline; managed embeddings pin the model
   and embed queries consistently for free.
3. Then drill this section on [TestHub](https://testlogichub.com) — 22% ≈ 10
   of 45 questions.
