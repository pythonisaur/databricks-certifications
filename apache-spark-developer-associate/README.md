# Databricks Associate Developer for Apache Spark

Study notes organized by the official exam sections (the current
**August 2026 guide**, 7 sections), one folder per section. The weight
next to each section is its share of the exam. 45 scored questions, 90
minutes, $200, Python variant.
Source: the [official exam page](https://www.databricks.com/learn/certification/apache-spark-developer-associate)
and its [exam guide (PDF)](https://www.databricks.com/sites/default/files/2026-10/databricks-certified-associate-developer-apache-spark-exam-guide-august-2026.pdf).

| # | Official section | Weight | Folder | Notebooks | Start with |
|---|---|---|---|---|---|
| 01 | Apache Spark Architecture and Components | 20% | [`01-architecture/`](01-architecture/) | 1 | [`01_spark_architecture.py`](01-architecture/01_spark_architecture.py) |
| 02 | Using Spark SQL | 20% | [`02-spark-sql/`](02-spark-sql/) | 1 | [`01_spark_sql.py`](02-spark-sql/01_spark_sql.py) |
| 03 | Developing Apache Spark DataFrame/DataSet API Applications | 30% | [`03-dataframe-api/`](03-dataframe-api/) | 1 | [`01_dataframe_api_drill.py`](03-dataframe-api/01_dataframe_api_drill.py) |
| 04 | Troubleshooting and Tuning Apache Spark DataFrame API Applications | 10% | [`04-troubleshooting-and-tuning/`](04-troubleshooting-and-tuning/) | 1 | [`01_tuning_essentials.py`](04-troubleshooting-and-tuning/01_tuning_essentials.py) |
| 05 | Structured Streaming | 10% | [`05-structured-streaming/`](05-structured-streaming/) | 1 | [`01_streaming_essentials.py`](05-structured-streaming/01_streaming_essentials.py) |
| 06 | Using Spark Connect to deploy applications | 5% | [`06-spark-connect/`](06-spark-connect/) | 1 | [`01_spark_connect.py`](06-spark-connect/01_spark_connect.py) |
| 07 | Using Pandas API on Apache Spark | 5% | [`07-pandas-api/`](07-pandas-api/) | 1 | [`01_pandas_api_on_spark.py`](07-pandas-api/01_pandas_api_on_spark.py) |

Every section has one overview notebook today. Sections 03, 01 and 02 are
**70% of the exam**: if you have one evening, spend it on the DataFrame drill.

This is the most code-literal Databricks exam: questions show a snippet and
ask what it returns, or show four snippets and ask which works. **Predict
before running** is the entire study method — every notebook here is
runnable on any Spark cluster (no Databricks-specific services needed).

## Overlap warning (in your favor)

Sections 01/04/05 are the engineering tracks' Spark internals wearing an
OSS hat — the notebooks cross-link instead of repeating. What's *new* here:
pure-API precision (section 03), Spark SQL ↔ DataFrame equivalence (02),
Spark Connect (06), and Pandas API on Spark (07).

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 📖 [Official exam page](https://www.databricks.com/learn/certification/apache-spark-developer-associate)
