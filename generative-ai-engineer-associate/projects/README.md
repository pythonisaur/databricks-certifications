# Projects

Hands-on builds that cut across the exam sections. Each project is a folder
of Databricks notebooks (`# Databricks notebook source` format: import the
folder into a workspace or sync it with the Databricks CLI / Repos).

| Project | Sections it exercises | Notebooks |
|---|---|---|
| [data-agent/](data-agent/) — a read-only SQL agent over a fake US food-delivery warehouse | 03 agents & tools, 05 governance (read-only grants, SELECT-only guard), 06 evaluation (right/wrong scorecard) | `00_build_warehouse` → `01_plain_chatbot` → `02_data_agent` → `03_right_and_wrong` |

## data-agent in one picture

```
food_delivery.main            (00: 6 cities, 240 restaurants, ~16K customers, ~170K orders, Delta)
   cities  restaurants  customers  orders  refunds  weather_daily  business_definitions
        │
        │  SELECT only (UC grant + code guard)
        ▼
   data agent  (02: Claude on a Foundation Model API endpoint, 3-4 tools, plan→look→write→run→check)
        │
        ▼
   01 chatbot guesses  ·  02 agent proves  ·  03 scorecard: raw tables vs business context
```

Planted stories (seed 42, verified locally): Chicago delivered orders fall
46 % in the week of 2026-10-19 (heavy snow, 35 % of orders end as
`cancel_reason = 'no_driver_available'`), and "revenue last month" is
$1,150,069 if you `SUM(total)` over every row but $884,732 by the business
definition (delivered subtotal − refunds).

Requirements: a Unity Catalog workspace, a cluster (DBR 15+), and one chat
endpoint in **Serving** (default widget `databricks-gpt-oss-120b`;
any tool-calling endpoint works). `pip install databricks-sdk` is
preinstalled on DBR.

Two ways to build the warehouse:

1. **In the workspace:** run notebook `00` once (≈2 min).
2. **From this laptop with the CLI (what we did 2026-10-27):**
   ```bash
   databricks auth login --host https://dbc-1b7422b2-c5a1.cloud.databricks.com --profile lakehouse   # browser OAuth
   cd data-agent
   python make_csv.py                      # data/*.csv from the notebook's generator cells, same seed
   python ingest_csv.py --profile lakehouse  # catalog + schema + volume `landing`, fs cp, CTAS via read_files
   ```
   `ingest_csv.py` needs only the CLI and a SQL warehouse (first one listed; `--warehouse <id>` to pick).
   CSVs land in `/Volumes/food_delivery/main/landing/`. The CLI `catalogs create` is refused on
   Default-Storage accounts; `CREATE CATALOG` through the warehouse works, and the script falls back to
   `dev_catalog.food_delivery` if even that fails.

Then `01`, `02` (flip the `use_context` widget), `03` for the scorecard. From the laptop:

```bash
databricks workspace import-dir data-agent /Users/byambaa1982@gmail.com/databricks-certifications/genai/projects/data-agent --overwrite
python run_in_workspace.py --profile lakehouse            # 4 serverless runs: 01, 02 raw, 02 context, 03; exit values -> runs/<ts>.json
python run_in_workspace.py --only 03                      # just the scorecard
```

Model: the workspace has no Claude endpoint, so the default is `databricks-gpt-oss-120b` (tool calling verified;
`databricks-meta-llama-3-3-70b-instruct` also works). It is a reasoning model: its thinking comes back in
`reasoning_content` and can eat the token budget, so the loop uses `max_tokens=4000`, prints the thinking as the
PLAN box, and nudges once if a turn comes back empty. The notebooks call the endpoint through
`WorkspaceClient().api_client.do(...)` because the serverless runtime lacks the `openai` extra.

First real runs (2026-10-27, `runs/2026-10-27_1547.json`): chatbot = 0 tables, generic checklist. Agent, raw tables:
Chicago 1,095 -> 898 orders, heavy snow found in `weather_daily` (one run also found the 311 `no_driver_available`
cancellations, one blamed temperature only); revenue answered **$1,092,135** (delivered `total`, tax and tips
included) = wrong. Agent with context: Chicago snow + no-driver cancellations, revenue **$884,732** = right. One SQL
error (`date_sub` with an interval) was retried and fixed by the model on its own. The video series uses `02`/`03` output as
the screen recordings.
