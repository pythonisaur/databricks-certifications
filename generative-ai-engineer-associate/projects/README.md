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
endpoint in **Serving** (default widget `databricks-claude-sonnet-4-5`;
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

Then `01`, `02` (flip the `use_context` widget), `03` for the scorecard. The video series uses `02`/`03` output as
the screen recordings.
