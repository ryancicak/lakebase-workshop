# Store-Aware Retail Search

> **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.

This lab asks one question:

> Can the same product query return different results by store, and can we see why in SQL?

It uses normal PostgreSQL joins and predicates to apply inventory and assortment rules before vector and BM25 ranking.

## What to run

Run [`Retail_Search_Context.py`](Retail_Search_Context.py) after the foundation notebook.

Plan for about 60 minutes:

| Time | What you will do |
|------|------------------|
| 5 min | Start with the question we are testing |
| 5 min | Inspect the fictional products, stores, and expected answers |
| 8 min | Look up an exact SKU with SQL |
| 12 min | Filter products, compare vector and BM25, then flip the weights |
| 13 min | Require `CORE` assortment and compare the same query at two stores |
| 6 min | Run a negative case without relaxing the request |
| 7 min | Change one inventory value, predict the result, and rerun |
| 4 min | Decide whether this is worth testing further |

## Prerequisites

- Complete `notebooks/00_Setup_Lakebase_Project`.
- Use PostgreSQL 16 or later.
- Enable Lakebase Search on the lab project before the session.
- Expect all project computes to restart when Search is enabled. Active connections are dropped, and Search cannot be disabled afterward.

The notebook does not enable Search. It stops if the extensions are unavailable.

Its first executable lab cell prints one readiness report before any fixture data is created. It
checks the pre-created project, PostgreSQL version, production endpoint, OAuth connection, Search
preload, extension installability, setup schema, and the exact table and index permissions used by
the lab. The functional probe rolls back its table, indexes, rows, and extension state.

## What you will see

- SKU and UPC use exact SQL equality, not vector search.
- Product attributes and excluded brands stay in SQL.
- Vector and BM25 use the same query text and produce different ranks.
- Store assortment and stock filter candidates before ranking.
- When no product meets the hard constraints, return no rows.

## What this does not answer

- This is not a comparison with Vector Search or a recommendation to replace it.
- The 24-row fixture uses a teaching encoder. It does not measure production relevance, latency, cost, recall, or scale.
- Query parsing is out of scope. The same store rules may also work in a split path.

If this looks useful, the next step is a small test with representative queries and expected answers.

## Files

- [`Retail_Search_Context.py`](Retail_Search_Context.py): runnable Databricks notebook
- [`DEMO.md`](DEMO.md): architecture, facilitation flow, and expected answers
- [`TASKS.md`](TASKS.md): implementation and validation state

## Attribution

This lab was added in a modified fork of the [Databricks Solutions Lakebase Workshop](https://github.com/databricks-solutions/lakebase-workshop). It is distributed under the repository's [DB License](../../LICENSE.md). See [NOTICE](../../NOTICE.md).
