# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Build the demo warehouse (`food_delivery`)
# MAGIC
# MAGIC A small, fake US food-delivery company: **6 cities, 240 restaurants,
# MAGIC 15,987 customers, 171,337 orders** (April–October 2026) written as Delta
# MAGIC tables in Unity Catalog. Two stories are planted in the data on purpose
# MAGIC so the data agent has something real to find:
# MAGIC
# MAGIC | Story | Where it lives | What a good agent says |
# MAGIC |---|---|---|
# MAGIC | Chicago orders dropped last week | `weather_daily` (heavy snow) + `orders.cancel_reason = 'no_driver_available'` | "Heavy snow, no drivers" |
# MAGIC | "What was revenue last month?" | `orders.status` + `refunds` | revenue = delivered subtotal − refunds, **not** `SUM(total)` of every row |
# MAGIC
# MAGIC Runs on any cluster with Unity Catalog. Deterministic (`SEED = 42`), so
# MAGIC every rebuild gives the same numbers.

# COMMAND ----------

dbutils.widgets.text("catalog", "food_delivery")
dbutils.widgets.text("schema", "main")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"writing to {CATALOG}.{SCHEMA}")

# COMMAND ----------

import random
from datetime import date, datetime, timedelta
import pandas as pd

SEED = 42
rng = random.Random(SEED)

START, END = date(2026, 4, 1), date(2026, 10, 24)
SNOW_START, SNOW_END = date(2026, 10, 19), date(2026, 10, 23)   # "last week" in Chicago

CITIES = [
    # city_id, name, state, timezone, sales_tax_rate, daily_orders_baseline
    (1, "New York",    "NY", "America/New_York",    0.08875, 210),
    (2, "Chicago",     "IL", "America/Chicago",     0.1025,  155),
    (3, "Los Angeles", "CA", "America/Los_Angeles", 0.095,   170),
    (4, "Houston",     "TX", "America/Chicago",     0.0825,   95),
    (5, "Phoenix",     "AZ", "America/Phoenix",     0.086,    72),
    (6, "Miami",       "FL", "America/New_York",    0.07,     80),
]
CUISINES = ["pizza", "burgers", "sushi", "tacos", "thai", "indian", "salads", "bbq", "chinese", "dessert"]
FIRST = ["Ava", "Liam", "Noah", "Emma", "Mia", "Lucas", "Olivia", "Ethan", "Sofia", "James", "Zoe", "Mason",
         "Chloe", "Logan", "Grace", "Aiden", "Lily", "Caleb", "Nora", "Owen"]
LAST = ["Smith", "Johnson", "Garcia", "Brown", "Miller", "Davis", "Martinez", "Lopez", "Wilson", "Anderson",
        "Taylor", "Thomas", "Moore", "Jackson", "Lee", "Nguyen", "Harris", "Clark", "Lewis", "Walker"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension tables: cities, restaurants, customers

# COMMAND ----------

cities = pd.DataFrame(CITIES, columns=["city_id", "city", "state", "timezone", "sales_tax_rate", "_baseline"])

restaurants = []
rid = 1
for c in CITIES:
    for _ in range(40):                                   # 40 per city = 240
        cuisine = rng.choice(CUISINES)
        restaurants.append((rid, f"{rng.choice(LAST)}'s {cuisine.title()}", cuisine, c[0],
                            round(rng.uniform(3.4, 4.9), 1), rng.choice([True, True, True, False])))
        rid += 1
restaurants = pd.DataFrame(restaurants, columns=["restaurant_id", "name", "cuisine", "city_id", "rating", "is_active"])

customers = []
for cid in range(1, 15_988):                              # 15,987 customers
    c = rng.choices(CITIES, weights=[x[5] for x in CITIES])[0]
    signup = START - timedelta(days=rng.randint(0, 700))
    customers.append((cid, f"{rng.choice(FIRST)} {rng.choice(LAST)}", c[0], signup,
                      rng.choice(["ios", "android", "web"])))
customers = pd.DataFrame(customers, columns=["customer_id", "full_name", "city_id", "signup_date", "platform"])
print(len(cities), "cities |", len(restaurants), "restaurants |", len(customers), "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather, then orders
# MAGIC Orders per city per day = baseline × weekday curve × noise. During the
# MAGIC Chicago snow week demand is only slightly lower (people still order) but
# MAGIC **drivers are not available**, so a large share of those orders end as
# MAGIC `cancelled` with `cancel_reason = 'no_driver_available'`.

# COMMAND ----------

days = [START + timedelta(d) for d in range((END - START).days + 1)]
weather = []
for c in CITIES:
    for d in days:
        if c[1] == "Chicago" and SNOW_START <= d <= SNOW_END:
            cond, snow, temp = "heavy_snow", round(rng.uniform(6, 14), 1), rng.randint(14, 26)
        else:
            cond = rng.choices(["clear", "clouds", "rain"], weights=[6, 3, 1])[0]
            snow, temp = 0.0, rng.randint(45, 95)
        weather.append((c[0], d, cond, snow, temp))
weather_daily = pd.DataFrame(weather, columns=["city_id", "weather_date", "condition", "snow_inches", "temp_f"])

rest_by_city = {c[0]: restaurants[restaurants.city_id == c[0]].restaurant_id.tolist() for c in CITIES}
cust_by_city = {c[0]: customers[customers.city_id == c[0]].customer_id.tolist() for c in CITIES}
WEEKDAY = [0.92, 0.90, 0.95, 1.0, 1.18, 1.32, 1.15]        # Mon..Sun

orders, refunds = [], []
oid, refid = 1, 1
for c in CITIES:
    city_id, tax = c[0], c[4]
    for d in days:
        snow_day = city_id == 2 and SNOW_START <= d <= SNOW_END
        n = int(c[5] * WEEKDAY[d.weekday()] * rng.uniform(0.85, 1.15) * (0.9 if snow_day else 1.0))
        for _ in range(n):
            placed = datetime(d.year, d.month, d.day, rng.choices(range(10, 23), weights=[1, 2, 4, 5, 3, 2, 2, 3, 5, 6, 5, 3, 2])[0],
                              rng.randint(0, 59), rng.randint(0, 59))
            subtotal = round(rng.lognormvariate(3.55, 0.45), 2)      # median ~$35
            tip = round(subtotal * rng.choice([0, 0.1, 0.15, 0.18, 0.2]), 2)
            sales_tax = round(subtotal * tax, 2)
            # status
            r = rng.random()
            if snow_day:
                status, reason = ("cancelled", "no_driver_available") if r < 0.42 else (("cancelled", "customer") if r < 0.45 else ("delivered", None))
            else:
                status, reason = ("cancelled", rng.choice(["customer", "restaurant_closed", "no_driver_available"])) if r < 0.05 else ("delivered", None)
            minutes = None if status == "cancelled" else int(rng.gauss(38 if not snow_day else 61, 9))
            orders.append((oid, placed, city_id, rng.choice(rest_by_city[city_id]), rng.choice(cust_by_city[city_id]),
                           status, reason, subtotal, sales_tax, tip, round(subtotal + sales_tax + tip, 2), minutes))
            if status == "delivered" and rng.random() < 0.025:       # 2.5 % of delivered orders get a refund
                amount = round(subtotal * rng.choice([0.25, 0.5, 1.0]), 2)
                refunds.append((refid, oid, placed + timedelta(hours=rng.randint(1, 72)), amount,
                                rng.choice(["missing_item", "cold_food", "late", "wrong_order"])))
                refid += 1
            oid += 1

orders = pd.DataFrame(orders, columns=["order_id", "placed_at", "city_id", "restaurant_id", "customer_id", "status",
                                       "cancel_reason", "subtotal", "sales_tax", "tip", "total", "delivery_minutes"])
refunds = pd.DataFrame(refunds, columns=["refund_id", "order_id", "refunded_at", "amount", "reason"])
print(f"{len(orders):,} orders | {len(refunds):,} refunds | {len(weather_daily):,} weather rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business definitions
# MAGIC The table the agent can read **when we give it context**. Without it, the
# MAGIC agent will guess what "revenue" means — which is the whole point of the
# MAGIC demo.

# COMMAND ----------

business_definitions = pd.DataFrame([
    ("revenue",         "SUM(subtotal) of orders with status = 'delivered', minus SUM(refunds.amount). Sales tax and tips are NOT revenue."),
    ("order volume",    "COUNT(*) of orders placed, any status. 'Completed orders' means status = 'delivered' only."),
    ("last week",       "The most recent full Monday–Sunday week before today (today is a widget in the agent notebook)."),
    ("last month",      "The previous calendar month."),
    ("cancellation",    "status = 'cancelled'; cancel_reason explains why (no_driver_available, customer, restaurant_closed)."),
    ("delivery time",   "orders.delivery_minutes; NULL for cancelled orders, never average it over cancelled rows."),
], columns=["term", "definition"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the Delta tables

# COMMAND ----------

TABLES = {
    "cities": cities.drop(columns=["_baseline"]),
    "restaurants": restaurants,
    "customers": customers,
    "orders": orders,
    "refunds": refunds,
    "weather_daily": weather_daily,
    "business_definitions": business_definitions,
}
COMMENTS = {
    "cities": "one row per city we deliver in",
    "restaurants": "one row per restaurant partner",
    "customers": "one row per customer",
    "orders": "every order: status, amounts, delivery time",
    "refunds": "refunds issued against delivered orders",
    "weather_daily": "daily weather per city (NOAA-style, fake)",
    "business_definitions": "how the business defines its metrics",
}
for name, pdf in TABLES.items():
    sdf = spark.createDataFrame(pdf)
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    spark.sql(f"COMMENT ON TABLE {name} IS '{COMMENTS[name]}'")
    print(f"{name:22s} {sdf.count():>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: both planted stories are visible in plain SQL

# COMMAND ----------

display(spark.sql("""
SELECT date_trunc('week', placed_at) AS week,
       COUNT(*)                                            AS orders_placed,
       SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
       SUM(CASE WHEN cancel_reason = 'no_driver_available' THEN 1 ELSE 0 END) AS no_driver
FROM orders o JOIN cities c USING (city_id)
WHERE c.city = 'Chicago' AND placed_at >= '2026-09-28'
GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

display(spark.sql("""
SELECT 'naive  SUM(total), all rows'              AS metric, ROUND(SUM(total), 2) AS usd
FROM orders WHERE placed_at >= '2026-09-01' AND placed_at < '2026-10-01'
UNION ALL
SELECT 'correct  delivered subtotal - refunds',
       ROUND(SUM(CASE WHEN status = 'delivered' THEN subtotal ELSE 0 END), 2)
       - (SELECT COALESCE(SUM(r.amount), 0) FROM refunds r JOIN orders o2 USING (order_id)
          WHERE o2.placed_at >= '2026-09-01' AND o2.placed_at < '2026-10-01')
FROM orders WHERE placed_at >= '2026-09-01' AND placed_at < '2026-10-01'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read-only access for the agent (optional, recommended)
# MAGIC The agent should never be able to write. In a real workspace give it a
# MAGIC service principal or group with **SELECT only**:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG food_delivery TO `data-agent`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  food_delivery.main TO `data-agent`;
# MAGIC GRANT SELECT      ON SCHEMA  food_delivery.main TO `data-agent`;
# MAGIC ```
# MAGIC
# MAGIC Notebook `02_data_agent` also blocks anything that is not a `SELECT`
# MAGIC before it reaches Spark — defence at two layers, which is how the exam
# MAGIC wants you to think about governance.
