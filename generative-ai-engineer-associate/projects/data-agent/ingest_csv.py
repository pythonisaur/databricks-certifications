"""Ingest data/*.csv into Unity Catalog tables using only the Databricks CLI (OAuth profile) and a SQL warehouse.

    python ingest_csv.py [--profile lakehouse] [--catalog food_delivery] [--schema main] [--warehouse <id>]

Steps: CREATE CATALOG (falls back to dev_catalog.<schema> when the account only allows UI-created
Default Storage catalogs) -> CREATE SCHEMA -> CREATE VOLUME landing -> `databricks fs cp` each CSV ->
CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files(...) with explicit schemas -> row counts.
"""
import argparse, json, subprocess, sys, time, os
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
HERE = Path(__file__).resolve().parent
os.environ["MSYS_NO_PATHCONV"] = "1"

SCHEMAS = {   # explicit types: read_files' inference would make ids BIGINT (fine) but dates STRING; delivery_minutes read as DOUBLE then cast (tolerates "38.0")
    "cities": "city_id INT, city STRING, state STRING, timezone STRING, sales_tax_rate DOUBLE",
    "restaurants": "restaurant_id INT, name STRING, cuisine STRING, city_id INT, rating DOUBLE, is_active BOOLEAN",
    "customers": "customer_id INT, full_name STRING, city_id INT, signup_date DATE, platform STRING",
    "orders": "order_id INT, placed_at TIMESTAMP, city_id INT, restaurant_id INT, customer_id INT, status STRING, "
              "cancel_reason STRING, subtotal DOUBLE, sales_tax DOUBLE, tip DOUBLE, total DOUBLE, delivery_minutes DOUBLE",
    "refunds": "refund_id INT, order_id INT, refunded_at TIMESTAMP, amount DOUBLE, reason STRING",
    "weather_daily": "city_id INT, weather_date DATE, condition STRING, snow_inches DOUBLE, temp_f INT",
    "business_definitions": "term STRING, definition STRING",
}
COMMENTS = {"cities": "one row per city we deliver in", "restaurants": "one row per restaurant partner",
            "customers": "one row per customer", "orders": "every order: status, amounts, delivery time",
            "refunds": "refunds issued against delivered orders", "weather_daily": "daily weather per city (fake)",
            "business_definitions": "how the business defines its metrics"}


def cli(*args, check=True):
    r = subprocess.run(["databricks", *args], capture_output=True, text=True, encoding="utf-8")
    if check and r.returncode:
        raise SystemExit(f"databricks {' '.join(args)}\n{r.stderr.strip()}")
    return r


def sql(profile, warehouse, statement):
    body = {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}
    r = cli("api", "post", "/api/2.0/sql/statements", "-p", profile, "--json", json.dumps(body))
    d = json.loads(r.stdout)
    while d["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        d = json.loads(cli("api", "get", f"/api/2.0/sql/statements/{d['statement_id']}", "-p", profile).stdout)
    if d["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"SQL failed: {statement[:120]}...\n{d['status'].get('error', {}).get('message')}")
    return d.get("result", {}).get("data_array", [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="lakehouse"); ap.add_argument("--catalog", default="food_delivery")
    ap.add_argument("--schema", default="main"); ap.add_argument("--warehouse")
    a = ap.parse_args()
    wh = a.warehouse or json.loads(cli("warehouses", "list", "-p", a.profile, "-o", "json").stdout)[0]["id"]
    print(f"profile={a.profile} warehouse={wh}")

    catalog, schema = a.catalog, a.schema
    r = cli("catalogs", "get", catalog, "-p", a.profile, check=False)
    if r.returncode:
        try:
            sql(a.profile, wh, f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'demo warehouse for the data-agent project'")
            print(f"created catalog {catalog}")
        except SystemExit as e:
            print(f"[fallback] cannot create catalog via CLI/SQL ({str(e).splitlines()[-1][:90]}); using dev_catalog.{catalog}")
            catalog, schema = "dev_catalog", a.catalog
    sql(a.profile, wh, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    sql(a.profile, wh, f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
    vol = f"/Volumes/{catalog}/{schema}/landing"
    print(f"target {catalog}.{schema}, volume {vol}")

    for name in SCHEMAS:
        src = HERE / "data" / f"{name}.csv"
        cli("fs", "cp", str(src), f"dbfs:{vol}/{name}.csv", "-p", a.profile, "--overwrite")
        sql(a.profile, wh, f"""
            CREATE OR REPLACE TABLE {catalog}.{schema}.{name}
            COMMENT '{COMMENTS[name]}'
            AS SELECT * {"EXCEPT (delivery_minutes), CAST(delivery_minutes AS INT) AS delivery_minutes" if name == "orders" else ""}
            FROM read_files('{vol}/{name}.csv', format => 'csv', header => true,
                            schema => '{SCHEMAS[name]}', nullValue => '')""")
        n = sql(a.profile, wh, f"SELECT COUNT(*) FROM {catalog}.{schema}.{name}")[0][0]
        print(f"{name:22s} {int(n):>8,} rows")
    print("done:", catalog + "." + schema)


if __name__ == "__main__":
    main()
