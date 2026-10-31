# 03 — Application Development (30%)

The heaviest section, now three notebooks deep. The RAG chain is written
out as runnable code — stub retriever, real prompt contract where every
line prevents a named failure — then shown again in LangChain's LCEL shape
so you can read pipelines the exam displays. The agent loop **runs live**
with a stubbed LLM: tool schemas, `tool_calls`, `role: "tool"` messages,
iteration caps, UC functions as governed tools, and the multi-agent
patterns. The closer is triage discipline: the symptom → first-knob table
with the escalation order (prompt → retrieval → architecture → model size →
fine-tuning), plus four guardrail layers with a runnable input filter and
the failure-handling code every production chain wraps around model calls.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_building_rag_and_agents.py`](01_building_rag_and_agents.py) | The full chain runnable with a stub retriever, the prompt-contract table, the LCEL pipeline to recognize, multi-turn history policies and query rewriting |
| [`02_agents_and_tools.py`](02_agents_and_tools.py) | The function-calling protocol and agent loop executed deterministically, tool-description and `role:"tool"` traps, UC function tools with `UCFunctionToolkit`, supervisor/pipeline/agent-as-tool patterns |
| [`03_quality_knobs_and_guardrails.py`](03_quality_knobs_and_guardrails.py) | The triage table + escalation order, a runnable regex guard inside the four-layer model, indirect injection via retrieved docs, retries/validation/fallbacks/idempotent tools |
| [`lesson02_rag_in_one_chain.py`](lesson02_rag_in_one_chain.py) | **Video lesson 02** (Emma on Data): the RAG chain as one LCEL expression, `{"context": retriever, "question": RunnablePassthrough()} \| prompt \| llm \| parser`; the three wrong shapes run and fail with their real errors; `format_docs`, `itemgetter`, `RunnablePassthrough.assign` |

## How to study

1. Run the agent loop, then rewrite it from memory — the message roles and
   the who-executes-what fact carry several questions.
2. Recite the escalation order until reflexive; most quality questions are
   "which knob first", and queue-jumping options are the distractors.
3. Then drill this section on [TestHub](https://testlogichub.com) — 30% ≈ 13
   of 45 questions, the most of any section.
