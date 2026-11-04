# 01 — Design Applications (14%)

Four runnable notebooks, one exam question each. Every notebook opens with the question, builds and runs
the correct option first, then runs the wrong options to the output they really produce, and ends with the
one rule to memorise. They are the companion notebooks for the Emma on Data lessons on this section and run
top to bottom on serverless compute or Free Edition (pay-per-token Foundation Model endpoints and SQL AI
Functions, nothing to deploy).

## Run order

| Notebook | Exam objective | The question it answers | What runs |
|---|---|---|---|
| [`01_formatted_response.py`](01_formatted_response.py) | Design a prompt that elicits a specifically formatted response; translate a use case into inputs and outputs | an app must receive exactly three JSON keys on every call: which approach guarantees it | `with_structured_output` / `response_format` `json_schema` first; then prompt instructions, `json_object` and few-shot on the same eight inputs with a validator counting matches; few-shot for a human layout; a stakeholder sentence turned into a nested schema and run |
| [`02_model_task_for_requirement.py`](02_model_task_for_requirement.py) | Select model tasks to accomplish a given business requirement (official sample question 5) | a paragraph memo to a one-sentence gist: which NLP task category | `ai_summarize` first, then `ai_classify`, a sentence split and `ai_gen` on the same memos; classification, extraction, sentiment and translation on the other requirement shapes the exam uses |
| [`03_chain_components_for_io.py`](03_chain_components_for_io.py) | Select chain components for a desired model input and output | grounded, multi-turn, plain-text support bot: which components | retriever + `MessagesPlaceholder` + `StrOutputParser` on two turns first; then the three chains with one component missing, each failing its requirement |
| [`04_order_tools_for_reasoning.py`](04_order_tools_for_reasoning.py) | Define and order tools that gather knowledge or take actions for multi-stage reasoning | a room-booking agent with four tools: in which order | the agent's real tool-call trace in the correct order first; then the wrong orders executed by hand, each leaving a visible mess in the calendar; the pure data-dependency shape (price, high, percentage) |

The LCEL pipe itself (`prompt | llm | StrOutputParser()`) and RAG in one chain are the earlier lessons:
[`04-assembling-and-deploying/lesson01_lcel_pipe_chain.py`](../04-assembling-and-deploying/lesson01_lcel_pipe_chain.py)
and [`03-application-development/lesson02_rag_in_one_chain.py`](../03-application-development/lesson02_rag_in_one_chain.py).

## The sixth objective: Agent Bricks (no notebook, by design)

"Determine how and when to use Agent Bricks" is the largest single objective of this section in the practice
bank, and it is a product decision made in the UI, not code. The lesson for it is a screen recording. The
decision table the questions test:

| The question describes | Agent Brick | Tell-tale words |
|---|---|---|
| questions over **documents** (PDFs in a Volume), answers with citations | **Knowledge Assistant** | "analysts ask what the reports say", "cite its sources" |
| plain-English questions over **tables** | **AI/BI Genie** (used by the Supervisor as a data agent) | "query Delta tables", "sales by region", "non-technical users" |
| documents in, a **structured table** out, at volume | **Information Extraction** | "800 invoices a month", "populate fields", "Delta table" |
| **several** of the above behind one assistant, routed by question type | **Multi-Agent Supervisor** | "three kinds of questions", "one entry point", "Genie spaces and agents together" |
| a task none of the above fits, or custom logic between steps | write the agent (Agent Framework, section 3) | "custom tool order", "must call our API", "specific guardrail logic" |

Pick a Brick when the use case is one of the standard shapes and time to value matters; write code when the
logic between the steps is the product.

## How to study

1. Run each notebook and predict every cell's output before you run it; the wrong options are there so you
   see the real failure, not read about it.
2. For every scenario in the practice tests, name the shape before the option: what goes in, what must come
   out, who reads it (a program wants a schema, a person wants examples), and which component or task that
   buys.
3. Drill this section on [TestHub](https://testlogichub.com): 14% is about 6 of 45 questions, and the Agent
   Bricks objective alone is a quarter of them.
