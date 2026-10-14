# Store-Aware Retail Search

> **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.

This lab asks one question:

> Can the same product query return different results by store, and can we see why in SQL?

It uses normal PostgreSQL joins and predicates to apply inventory and assortment rules before vector and BM25 ranking.

## What to run

Open [`Retail_Search_Context.py`](Retail_Search_Context.py) and run it directly. This lab is
standalone. Do not run `notebooks/00_Setup_Lakebase_Project` first.

The first practical cell creates a distinct project for the current participant when missing. It
prepares PostgreSQL 17, configures `production/primary` at 1/1 CU with a 1-hour scale-to-zero timeout,
enables Lakebase Search, waits and reconnects, creates only that participant's lab schema, and
validates exact, ANN, and BM25 search. It does not use or seed the foundation project.

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

- Use a Databricks workspace with Lakebase and Lakebase Search available.
- Each participant must be able to create a Lakebase project and enable Lakebase Search in that workspace.
- Run the first practical cell before creating any fixture data.

If the participant's exact project does not exist, the cell creates it as PostgreSQL 17. If it
already exists, the cell reuses it only when the current participant owns it and it carries both
`purpose=lakebase-workshop` and `lab=retail-search-context`. The cell configures the exact
`production/primary` endpoint at 1/1 CU with a 1-hour scale-to-zero timeout.

The cell enables Lakebase Search when needed. That change cannot be undone. It restarts the
project's computes and drops active connections, which is why this lab uses a dedicated project.
After the restart, the cell reconnects with a fresh OAuth credential, creates only the participant
schema, and runs the exact, ANN, and BM25 readiness checks. The functional probe rolls back its
table, indexes, rows, and extension state.

Rerun the same cell if provisioning or the Search restart is still in progress. It resumes from
the current state and safely reuses only the same exact owned project with both tags.

Pauses are fine. The project stays on through an idle hour, then scales to zero. If that closes
the connection, the next cell reconnects on its own, prints `Reconnected`, and retries once.

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
