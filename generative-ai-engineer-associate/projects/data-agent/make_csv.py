"""Write data/*.csv from the generator cells of 00_build_warehouse.py (same seed, same rows as the notebook).

    python make_csv.py        -> data/{cities,restaurants,customers,orders,refunds,weather_daily,business_definitions}.csv
"""
import os, sys
from pathlib import Path
from unittest.mock import MagicMock
sys.stdout.reconfigure(encoding="utf-8")
HERE = Path(__file__).resolve().parent
src = (HERE / "00_build_warehouse.py").read_text(encoding="utf-8").split("# MAGIC ## Write the Delta tables")[0]
dbutils = MagicMock(); dbutils.widgets.get = lambda k: {"catalog": "food_delivery", "schema": "main"}[k]
g = {"dbutils": dbutils, "spark": MagicMock(), "display": lambda x: None}
exec(compile(src, "00_build_warehouse.py", "exec"), g)
orders = g["orders"].copy(); orders["delivery_minutes"] = orders["delivery_minutes"].astype("Int64")   # ints, blank for NULL
tables = {"cities": g["cities"].drop(columns=["_baseline"]), "restaurants": g["restaurants"], "customers": g["customers"],
          "orders": orders, "refunds": g["refunds"], "weather_daily": g["weather_daily"], "business_definitions": g["business_definitions"]}
(HERE / "data").mkdir(exist_ok=True); (HERE / "data" / ".gitignore").write_text("*.csv\n")
for n, df in tables.items():
    p = HERE / "data" / f"{n}.csv"; df.to_csv(p, index=False)
    print(f"{n:22s} {len(df):>8,} rows  {os.path.getsize(p) / 1e6:5.1f} MB")
