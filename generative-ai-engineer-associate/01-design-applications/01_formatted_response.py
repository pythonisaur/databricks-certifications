# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # A response the application can parse: structured output vs instructions
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 1 of the exam guide, "Design a prompt that
# MAGIC elicits a specifically formatted response", plus "Translate business use case goals into a description of
# MAGIC the desired inputs and outputs" in section 6 of this notebook.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the chat model is a pay-per-token Foundation Model endpoint,
# MAGIC nothing to deploy. Outputs shown in the video were captured on 11/04/2026 (langchain-core 1.7.x).
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A generative AI engineer classifies customer feedback with a Foundation Model API endpoint. An application
# MAGIC reads the result, so every response must be a JSON object with exactly three keys: `category` (one of four
# MAGIC labels), `confidence` (a number from 0 to 1) and `reason` (a string). In testing, the model sometimes wraps
# MAGIC the JSON in markdown or adds a sentence before it. Which approach guarantees the structure on every call?
# MAGIC
# MAGIC * **A** Tell the model in the system prompt to reply as JSON with those three keys, and parse the text
# MAGIC * **B** Declare the schema and let the endpoint enforce it: `llm.with_structured_output(Feedback)`, which sends `response_format={"type": "json_schema", ...}`
# MAGIC * **C** Send `response_format={"type": "json_object"}` with the request
# MAGIC * **D** Add two few-shot examples of the JSON to the prompt
# MAGIC
# MAGIC The correct approach (B) is built and run first. Then the three others run on the same eight lines of
# MAGIC feedback, and a validator counts how many responses actually match the contract.

# COMMAND ----------

# MAGIC %pip install -q -U langchain-core databricks-langchain databricks-openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Endpoint widget
# A Databricks-hosted pay-per-token endpoint. Free Edition lists it in Playground > Select an endpoint.
dbutils.widgets.text("ENDPOINT", "databricks-llama-4-maverick")
ENDPOINT = dbutils.widgets.get("ENDPOINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: the contract as code, eight lines of feedback, one validator
# MAGIC
# MAGIC The requirement in the question is a contract: three keys, one of them from a closed list, one of them a
# MAGIC number in a range. Written as a Pydantic class it becomes something a program can check, and something an
# MAGIC endpoint can be told to obey.

# COMMAND ----------

# DBTITLE 1,The contract: three keys, a closed label set, a bounded number
from typing import Literal
from pydantic import BaseModel, Field

class Feedback(BaseModel):
    category: Literal["delivery", "food_quality", "app", "payment"] = Field(description="The team that should handle this feedback")
    confidence: float = Field(ge=0, le=1, description="How sure the model is, from 0 to 1")
    reason: str = Field(description="One sentence quoting the words that decided the category")

# COMMAND ----------

# DBTITLE 1,Eight lines of feedback, two of them awkward on purpose
FEEDBACK = [
    "The driver left my order at the wrong building and it took an hour to find it.",
    "Pad thai was cold and the spring rolls were soggy.",
    "The app crashes every time I open the order history page.",
    "I was charged twice for order 5531 and still have not been refunded.",
    "Food was fine, delivery was fine, but why does the app log me out every day?",
    "PLEASE JUST TELL ME IN PLAIN ENGLISH WHICH TEAM HANDLES THIS: my fries were missing.",
    'The receipt says "service fee" twice and nobody can explain the second one.',
    "Great driver, terrible soup. Second time the soup arrived half spilled.",
]
inputs = [{"feedback": f} for f in FEEDBACK]
# Pay-per-token endpoints have a per-workspace requests-per-second limit; this keeps every .batch() under it.
SEQ = {"max_concurrency": 1}

# COMMAND ----------

# DBTITLE 1,The prompt, the model, and a validator that checks the contract
import json
from databricks_langchain import ChatDatabricks
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

# "json" is spelled in lower case on purpose: the json_object option in section 3 requires that exact word in the messages.
SYSTEM = ("You classify customer feedback for a food delivery company. Reply as a json object with the keys category, confidence and reason. "
          "category is one of delivery, food_quality, app, payment; confidence is a number from 0 to 1; reason is one sentence.")
prompt = ChatPromptTemplate.from_messages([("system", SYSTEM), ("human", "{feedback}")])
llm = ChatDatabricks(endpoint=ENDPOINT, temperature=0)

def validate(raw):
    """'ok' when the response is a dict (or parses to one) that satisfies Feedback; otherwise the error."""
    try:
        Feedback.model_validate(raw if isinstance(raw, dict) else json.loads(raw))
        return "ok"
    except Exception as e:
        return f"{type(e).__name__}: {str(e).splitlines()[0][:70]}"

def report(label, outputs):
    checks = [validate(o if isinstance(o, (dict, str)) else o.model_dump()) for o in outputs]
    kind = "str" if isinstance(outputs[0], str) else type(outputs[0]).__name__
    print(f"{label}: {checks.count('ok')} of {len(outputs)} match the contract; each output is a {kind}")
    for o, c in zip(outputs, checks):
        shown = o if isinstance(o, str) else json.dumps(o if isinstance(o, dict) else o.model_dump())
        print(f"  {c:38s} | {shown[:78]!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Structured output (option B): the schema goes to the endpoint, not into a sentence
# MAGIC
# MAGIC `with_structured_output(Feedback)` does two things. It sends the JSON schema of the class with the request,
# MAGIC so the endpoint constrains what it generates to that schema; and it parses the reply back into a `Feedback`
# MAGIC object. Your code never sees text, so there is nothing to strip and nothing to hope for.

# COMMAND ----------

# DBTITLE 1,The correct approach, run first
structured_llm = llm.with_structured_output(Feedback, method="json_schema")
chain_b = prompt | structured_llm

one = chain_b.invoke(inputs[3])
print(type(one).__name__, "->", one)
print("category:", one.category, "| confidence:", one.confidence)

# COMMAND ----------

# DBTITLE 1,All eight: every reply is already a Feedback object
outputs_b = chain_b.batch(inputs, config=SEQ)
report("B structured output", outputs_b)

# COMMAND ----------

# DBTITLE 1,What was sent: the schema the endpoint enforces
print(json.dumps(Feedback.model_json_schema(), indent=1))

# COMMAND ----------

# DBTITLE 1,The same thing one level down: response_format on the OpenAI-compatible endpoint
from databricks_openai import DatabricksOpenAI

client = DatabricksOpenAI()   # the OpenAI client pointed at this workspace's serving endpoints
resp = client.chat.completions.create(
    model=ENDPOINT, temperature=0,
    messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": FEEDBACK[5]}],
    response_format={"type": "json_schema",
                     "json_schema": {"name": "feedback", "schema": Feedback.model_json_schema(), "strict": True}},
)
print(resp.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The three other options, each run on the same eight lines
# MAGIC
# MAGIC | Option | What comes back | What is guaranteed |
# MAGIC |---|---|---|
# MAGIC | A instructions in the prompt | a string | nothing: the model may add a sentence or a markdown fence |
# MAGIC | C `response_format` `json_object` | a string that is valid JSON | valid JSON, not your keys, labels or ranges |
# MAGIC | D few-shot examples | a string | nothing: examples demonstrate the format, they do not enforce it |

# COMMAND ----------

# DBTITLE 1,Option A: the instruction is in the prompt, the reply is text
chain_a = prompt | llm | StrOutputParser()
outputs_a = chain_a.batch(inputs, config=SEQ)
report("A instructions only", outputs_a)

# COMMAND ----------

# DBTITLE 1,Option C: json_object guarantees JSON, not your JSON
# json_object requires the lower-case word "json" somewhere in the messages (the request is rejected otherwise); the system prompt has it.
llm_json = ChatDatabricks(endpoint=ENDPOINT, temperature=0, extra_params={"response_format": {"type": "json_object"}})
chain_c = prompt | llm_json | StrOutputParser()
outputs_c = chain_c.batch(inputs, config=SEQ)
report("C json_object", outputs_c)

# COMMAND ----------

# DBTITLE 1,Option C with no keys named in the prompt: valid JSON, whatever keys the model likes
prompt_nokeys = ChatPromptTemplate.from_messages([
    ("system", "You classify customer feedback for a food delivery company. Reply as a json object."),
    ("human", "{feedback}")])
for raw in (prompt_nokeys | llm_json | StrOutputParser()).batch(inputs[:4], config=SEQ):
    print(sorted(json.loads(raw).keys()), "|", raw[:80])

# COMMAND ----------

# DBTITLE 1,Option D: few-shot examples show the format; the reply is still text
from langchain_core.prompts import FewShotChatMessagePromptTemplate

examples = [
    {"feedback": "My order arrived 50 minutes late and the driver never answered the phone.",
     "answer": '{"category": "delivery", "confidence": 0.95, "reason": "arrived 50 minutes late and the driver never answered"}'},
    {"feedback": "The checkout button does nothing on Android.",
     "answer": '{"category": "app", "confidence": 0.9, "reason": "the checkout button does nothing"}'},
]
few_shot = FewShotChatMessagePromptTemplate(
    example_prompt=ChatPromptTemplate.from_messages([("human", "{feedback}"), ("ai", "{answer}")]),
    examples=examples,
)
prompt_d = ChatPromptTemplate.from_messages([("system", SYSTEM), few_shot, ("human", "{feedback}")])
chain_d = prompt_d | llm | StrOutputParser()
outputs_d = chain_d.batch(inputs, config=SEQ)
report("D few-shot", outputs_d)

# COMMAND ----------

# MAGIC %md
# MAGIC Whatever the counts came out to on your run, notice what they are: a **measurement of how often the model
# MAGIC felt like complying**. A, C and D can score eight of eight on a good day and seven of eight on the next
# MAGIC deployment, and the application finds out in production. B cannot score below eight, because the
# MAGIC structure is enforced while the tokens are generated, and a reply that is not a `Feedback` raises before it
# MAGIC reaches your code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. When a person reads the output: show the format with examples
# MAGIC
# MAGIC The other half of this objective is formats a program does not parse: "exactly three bullets, no
# MAGIC introduction". There is no schema for prose layout, so the strongest lever is a few-shot example of the exact
# MAGIC layout plus a negative instruction. Same model, same ticket, with and without the examples.

# COMMAND ----------

# DBTITLE 1,Zero-shot layout instruction
TICKET = ("Customer called twice about order 7781. First call: the order showed as delivered but nothing arrived; agent reissued the order. "
          "Second call: the reissued order arrived without the drinks and the customer wants the drinks refunded, not re-sent. Customer is a Gold member since 2023.")
LAYOUT = "Summarize the support ticket as exactly three bullet points starting with '- '. No introduction, no conclusion."

prompt_zero = ChatPromptTemplate.from_messages([("system", LAYOUT), ("human", "{ticket}")])
print((prompt_zero | llm | StrOutputParser()).invoke({"ticket": TICKET}))

# COMMAND ----------

# DBTITLE 1,The same instruction with two examples of the layout
layout_examples = [
    {"ticket": "Order 1201 arrived cold. Customer asked for a refund. Refund of 18 dollars issued.",
     "answer": "- Order 1201 arrived cold\n- Customer asked for a refund\n- 18 dollar refund issued"},
    {"ticket": "App crashed at checkout twice for user 88. Cleared cache, still crashing. Escalated to mobile team.",
     "answer": "- Checkout crashed twice for user 88\n- Cache clear did not help\n- Escalated to the mobile team"},
]
layout_shots = FewShotChatMessagePromptTemplate(
    example_prompt=ChatPromptTemplate.from_messages([("human", "{ticket}"), ("ai", "{answer}")]),
    examples=layout_examples,
)
prompt_few = ChatPromptTemplate.from_messages([("system", LAYOUT), layout_shots, ("human", "{ticket}")])
print((prompt_few | llm | StrOutputParser()).invoke({"ticket": TICKET}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The levers the exam names, and what each one actually controls
# MAGIC
# MAGIC | Lever | Controls | Does not control |
# MAGIC |---|---|---|
# MAGIC | schema via `response_format` json_schema / `with_structured_output` | the structure of every reply: keys, types, enums | the correctness of the values |
# MAGIC | `response_format` json_object | that the reply parses as JSON | which keys appear |
# MAGIC | few-shot examples | layout and style a person reads | anything a program must rely on |
# MAGIC | instructions in the system prompt | the task and the rules | compliance on every call |
# MAGIC | `temperature` | variance between runs | structure (a deterministic wrong format is still wrong) |
# MAGIC | `max_tokens` | the length of the output | format; a cut-off JSON is a `max_tokens` symptom, not a prompting one |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. From a business sentence to inputs and outputs
# MAGIC
# MAGIC The exam also asks you to turn a stakeholder's sentence into a pipeline description. The sentence: "after
# MAGIC discharge, the physician dictates a free-form note; we need a structured document with the diagnosis, the
# MAGIC medications with dose and frequency, the follow-up timing and the warning signs." Read it as a spec:
# MAGIC
# MAGIC | | Description |
# MAGIC |---|---|
# MAGIC | input | one free-text clinical note (unstructured) |
# MAGIC | output | one structured record: `diagnosis`, a list of `medications` (name, dose, frequency), `follow_up_days`, a list of `red_flags` |
# MAGIC | task | information extraction with a fixed schema |
# MAGIC
# MAGIC The output column of that table is a Pydantic class, and the mechanism from section 2 runs it.

# COMMAND ----------

# DBTITLE 1,The spec as code: nested schema, one free-text note in
class Medication(BaseModel):
    name: str
    dose: str
    frequency: str

class DischargeSummary(BaseModel):
    diagnosis: str
    medications: list[Medication]
    follow_up_days: int | None = Field(description="Days until the follow-up visit, if the note states one")
    red_flags: list[str] = Field(description="Symptoms that should send the patient back to the hospital")

NOTE = ("Patient admitted with community acquired pneumonia, improved on antibiotics. Discharge on amoxicillin 500 mg three times daily "
        "for five more days, continue lisinopril 10 mg once daily. See the GP in one week. Return to the ER for fever above 39 or shortness of breath.")

summary = llm.with_structured_output(DischargeSummary, method="json_schema").invoke(f"Extract the discharge summary from this note:\n{NOTE}")
print(json.dumps(summary.model_dump(), indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rule to memorise
# MAGIC
# MAGIC If a program reads the answer, declare the schema and let the endpoint enforce it: `response_format` with a
# MAGIC `json_schema`, or `with_structured_output` in LangChain. `json_object` only promises JSON, examples only
# MAGIC demonstrate, instructions only ask. If a person reads the answer, show the layout with few-shot examples.
# MAGIC And when a stakeholder describes a feature, write the input and the output as a schema first; the task and
# MAGIC the mechanism follow from it.
