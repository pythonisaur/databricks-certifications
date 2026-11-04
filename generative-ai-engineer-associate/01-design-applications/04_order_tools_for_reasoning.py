# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # In which order should an agent call its tools? (gather, check, act, notify)
# MAGIC
# MAGIC Companion notebook for the Emma on Data video. Section 1 of the exam guide, "Define and order tools that
# MAGIC gather knowledge or take actions for multi-stage reasoning".
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless
# MAGIC compute is enough and Free Edition works: the chat model is a pay-per-token Foundation Model endpoint,
# MAGIC nothing to deploy. Outputs shown in the video were captured on 11/04/2026 (langchain-core 1.7.x).
# MAGIC
# MAGIC **The exam question this lesson answers**
# MAGIC
# MAGIC A generative AI engineer is designing an agent that books meeting rooms. It has four tools:
# MAGIC `check_room_availability(room_id, time_slot) -> bool`, `verify_user_permissions(user_id, room_id) -> bool`,
# MAGIC `create_booking(user_id, room_id, time_slot) -> booking_id` and `send_confirmation_email(user_id, booking_id)`.
# MAGIC A user asks for room R-202 at 10:00 with a confirmation by email. In which order should the tools run?
# MAGIC
# MAGIC * **A** `create_booking` -> `check_room_availability` -> `verify_user_permissions` -> `send_confirmation_email`
# MAGIC * **B** `check_room_availability` -> `verify_user_permissions` -> `create_booking` -> `send_confirmation_email`
# MAGIC * **C** `send_confirmation_email` -> `create_booking` -> `check_room_availability` -> `verify_user_permissions`
# MAGIC * **D** `verify_user_permissions` -> `create_booking` -> `check_room_availability` -> `send_confirmation_email`
# MAGIC
# MAGIC The correct order (B) is what a real agent does when the tools are described well: its tool-call trace is
# MAGIC shown first. Then the three wrong orders are executed by hand against the same calendar, and each one
# MAGIC leaves a visible mess.

# COMMAND ----------

# MAGIC %pip install -q -U langchain-core databricks-langchain

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Endpoint widget
# A Databricks-hosted pay-per-token endpoint that supports tool calling. Free Edition lists it in Playground.
# Llama 3.3 70B follows multi-step tool loops cleanly; Llama 4 Maverick tends to write its second tool call as
# text instead of a tool call, which ends the loop early. GPT OSS 120B also works.
dbutils.widgets.text("ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
ENDPOINT = dbutils.widgets.get("ENDPOINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: a calendar, a permission list, and four tools
# MAGIC
# MAGIC Two of the tools only **read** (availability, permission). One **writes** (the booking). One is a
# MAGIC **side effect that depends on the write** (the email needs a booking id). Those three kinds are the whole
# MAGIC lesson. Each tool's docstring says which kind it is and what must be true before it runs; the docstring is
# MAGIC the only thing the agent reads, so it is where the order is taught.

# COMMAND ----------

# DBTITLE 1,The state the tools touch
CALENDAR = {}
PERMISSIONS = {"u-42": {"R-101", "R-202"}, "u-7": {"R-101"}}
BOOKINGS, EMAILS = [], []

def reset():
    CALENDAR.clear(); CALENDAR[("R-101", "10:00")] = "u-7"     # R-101 at 10:00 is already taken
    BOOKINGS.clear(); EMAILS.clear()

def show_state():
    print("calendar:", {f"{r} {t}": u for (r, t), u in CALENDAR.items()})
    print("bookings:", BOOKINGS)
    print("emails:  ", EMAILS)

reset(); show_state()

# COMMAND ----------

# DBTITLE 1,Four tools: two reads, one write, one notification
from langchain_core.tools import tool

@tool
def check_room_availability(room_id: str, time_slot: str) -> bool:
    """Return True if the room is free at the time slot (HH:MM). Read-only. Check this before creating a booking."""
    return CALENDAR.get((room_id, time_slot)) is None

@tool
def verify_user_permissions(user_id: str, room_id: str) -> bool:
    """Return True if the user is allowed to book this room. Read-only. Check this before creating a booking."""
    return room_id in PERMISSIONS.get(user_id, set())

@tool
def create_booking(user_id: str, room_id: str, time_slot: str) -> str:
    """Write the booking to the calendar and return the booking id. Only call after availability and permission are both confirmed."""
    booking_id = f"BK-{len(BOOKINGS) + 1}"
    BOOKINGS.append((booking_id, user_id, room_id, time_slot))
    CALENDAR[(room_id, time_slot)] = user_id
    return booking_id

@tool
def send_confirmation_email(user_id: str, booking_id: str) -> str:
    """Email the user a confirmation for an existing booking id. Only call after create_booking returned an id."""
    EMAILS.append((user_id, booking_id))
    return f"sent to {user_id} for {booking_id}"

TOOLS = [check_room_availability, verify_user_permissions, create_booking, send_confirmation_email]
for t in TOOLS:
    print(f"{t.name:26s} args={list(t.args)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The agent picks order B on its own (the correct option, run first)
# MAGIC
# MAGIC `bind_tools` sends the four tool schemas with every request. The model answers either with text or with a
# MAGIC list of tool calls; the loop runs each call, appends the result as a `ToolMessage`, and asks again. The order
# MAGIC in the trace is not programmed anywhere: the model reads the docstrings and the dependencies between the
# MAGIC tools' inputs and outputs.

# COMMAND ----------

# DBTITLE 1,The model with the tools bound, and what one tool call looks like
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

llm = ChatDatabricks(endpoint=ENDPOINT, temperature=0)
llm_with_tools = llm.bind_tools(TOOLS)

SYSTEM = "You book meeting rooms for employees using the tools. Gather the facts before you act."
REQUEST = "Book room R-202 at 10:00 for user u-42 and confirm by email."

first = llm_with_tools.invoke([SystemMessage(SYSTEM), HumanMessage(REQUEST)])
print(first.tool_calls)

# COMMAND ----------

# DBTITLE 1,The agent loop: call, run, observe, repeat
TOOL_BY_NAME = {t.name: t for t in TOOLS}

def run_agent(request, tools=TOOLS, system=SYSTEM, max_steps=8):
    model = llm.bind_tools(tools)
    by_name = {t.name: t for t in tools}
    messages = [SystemMessage(system), HumanMessage(request)]
    for step in range(1, max_steps + 1):
        ai = model.invoke(messages)
        messages.append(ai)
        if not ai.tool_calls:
            return ai.content
        for call in ai.tool_calls:
            result = by_name[call["name"]].invoke(call["args"])
            print(f"step {step}: {call['name']}({call['args']}) -> {result}")
            messages.append(ToolMessage(content=str(result), tool_call_id=call["id"]))
    return "stopped: too many steps"

reset()
print(run_agent(REQUEST))
show_state()

# COMMAND ----------

# DBTITLE 1,The reads let the agent stop before any side effect: a room that is taken
reset()
print(run_agent("Book room R-101 at 10:00 for user u-42 and confirm by email."))
show_state()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The three wrong orders, executed by hand on the same calendar
# MAGIC
# MAGIC Each wrong option is run exactly as written, for a request where the checks would fail. The reads cannot
# MAGIC undo what a write did before them.
# MAGIC
# MAGIC | Option | First mistake | What is left behind |
# MAGIC |---|---|---|
# MAGIC | A | writes the booking, then checks | a double booking in the calendar, and an email for it |
# MAGIC | C | emails first | a confirmation for a booking id that does not exist |
# MAGIC | D | books after one check, availability last | a booking on a slot someone else holds |

# COMMAND ----------

# DBTITLE 1,A helper that runs tools in a given order and shows the damage
def run_order(order, user_id, room_id, time_slot):
    reset()
    booking_id = None
    for name in order:
        t = TOOL_BY_NAME[name]
        if name == "check_room_availability":
            r = t.invoke({"room_id": room_id, "time_slot": time_slot})
        elif name == "verify_user_permissions":
            r = t.invoke({"user_id": user_id, "room_id": room_id})
        elif name == "create_booking":
            r = booking_id = t.invoke({"user_id": user_id, "room_id": room_id, "time_slot": time_slot})
        else:
            r = t.invoke({"user_id": user_id, "booking_id": str(booking_id)})
        print(f"  {name:26s} -> {r}")
    show_state()

# COMMAND ----------

# DBTITLE 1,Option A: book first, check afterwards (the room is already taken)
run_order(["create_booking", "check_room_availability", "verify_user_permissions", "send_confirmation_email"],
          user_id="u-42", room_id="R-101", time_slot="10:00")

# COMMAND ----------

# DBTITLE 1,Option C: email first (there is no booking id yet)
run_order(["send_confirmation_email", "create_booking", "check_room_availability", "verify_user_permissions"],
          user_id="u-42", room_id="R-202", time_slot="10:00")

# COMMAND ----------

# DBTITLE 1,Option D: permission, then book, then availability (the user may book it, but the slot is taken)
run_order(["verify_user_permissions", "create_booking", "check_room_availability", "send_confirmation_email"],
          user_id="u-42", room_id="R-101", time_slot="10:00")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The other shape the exam uses: pure data dependencies
# MAGIC
# MAGIC "What is today's NVDA close and how far below the 52-week high is it?" has no side effects at all; the order
# MAGIC is forced by the arguments. The calculation needs two numbers that only the two lookups produce, so it
# MAGIC cannot go first no matter how it is described.

# COMMAND ----------

# DBTITLE 1,Three tools, one of which needs the other two's outputs
PRICES = {"NVDA": 118.4}
HIGHS = {"NVDA": 153.1}

@tool
def get_current_price(ticker: str) -> float:
    """Return today's closing price for the ticker."""
    return PRICES[ticker]

@tool
def get_52_week_high(ticker: str) -> float:
    """Return the 52-week high price for the ticker."""
    return HIGHS[ticker]

@tool
def calculate_percentage_diff(value: float, reference: float) -> float:
    """Return how far value is below reference, as a percentage of reference."""
    return round((reference - value) / reference * 100, 2)

STOCK_TOOLS = [get_current_price, get_52_week_high, calculate_percentage_diff]
print(run_agent("What is today's closing price of NVDA and by what percentage is it below its 52-week high?",
                tools=STOCK_TOOLS, system="You answer stock questions using the tools."))

# COMMAND ----------

# DBTITLE 1,The calculation first (option A of that question) cannot even be called
try:
    calculate_percentage_diff.invoke({})
except Exception as e:
    print(type(e).__name__, "->", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The ordering rule behind every version of this question
# MAGIC
# MAGIC | Stage | Tools of this kind | Why here |
# MAGIC |---|---|---|
# MAGIC | 1 gather | read-only lookups (availability, price, a retriever, a Genie query) | they produce the facts every later step needs, and they cost nothing to repeat |
# MAGIC | 2 check | validations (permission, policy, a threshold) | a failed check must stop the agent before anything is written |
# MAGIC | 3 act | the one write (create, update, charge) | once, after every check passed |
# MAGIC | 4 notify | side effects that need the write's result (email, ticket, message) | they carry the id or status the write produced |
# MAGIC
# MAGIC On Databricks the tools are usually Unity Catalog functions (`CREATE FUNCTION`, SQL or Python) exposed
# MAGIC to the agent through `UCFunctionToolkit`, and the agent runs inside Agent Framework with MLflow tracing
# MAGIC showing the same trace you printed here. The ordering rule does not change with the plumbing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The rule to memorise
# MAGIC
# MAGIC Order tools by what each one needs and what it changes: reads first because they produce the facts, checks
# MAGIC before any write because a failed check must cost nothing, the write once, notifications last because they
# MAGIC need the write's result. Put those preconditions in the tool descriptions; that is how the agent learns the
# MAGIC order, and it is what the exam means by "define and order tools".
