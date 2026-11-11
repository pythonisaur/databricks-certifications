# 08 — Data Governance (7%)

Where section 07 asked *may you touch it*, this section asks *what is it and
where did it come from*. Two notebooks: the Unity Catalog object model (the
hierarchy, managed vs external and what DROP really deletes, the storage
plumbing) and the metadata layer (automatic lineage, tags you can sweep with
SQL, discovery). Overlaps deliberately with 04 (sharing lives in UC) and 07
(grants) — the exam blurs these lines too.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_unity_catalog_object_model.py`](01_unity_catalog_object_model.py) | Metastore → catalog → schema hierarchy, managed vs external DROP/UNDROP semantics (checked live with `DESCRIBE EXTENDED`), storage credentials → external locations, volumes vs legacy mounts, `information_schema` queries |
| [`02_lineage_tags_and_discovery.py`](02_lineage_tags_and_discovery.py) | `system.access.table_lineage` probed live, why path-based reads break lineage, column-level lineage for impact analysis, tags set/queried/unset live (the PII sweep), comments and certification |
| [`q_76e3b91cf6_comments_and_tags.py`](q_76e3b91cf6_comments_and_tags.py) | Exam question on discoverability: `COMMENT` on schema/tables/columns plus `pii`/`layer` tags set live and searched through `information_schema` (option A, run first), then `TBLPROPERTIES`, a mapping table, name prefixes and a Marketplace listing stand-in each run to where they fall short |

## Setup

Run section 01's `00_setup.py` first — both notebooks introspect and tag the
`de_pro` clones. Widgets default to `dev_catalog` / `de_pro` as usual.

## How to study

1. The DROP semantics table in notebook 01 is the section's most-asked fact:
   managed = data deleted (UNDROP within 30 days), external = metadata only.
   If you remember one thing, make it that row.
2. Lineage questions hinge on *automatic* and *what breaks it*: no manual
   instrumentation, but path-based access and non-UC compute fall outside the
   graph.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 7% weight ≈ 4 of 60 questions.
