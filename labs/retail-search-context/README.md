# Search products with store inventory

> Added by Ryan Cicak on 2026-10-13. Presentation wording revised 2026-11-06.

A customer wants a driver for long deck screws. We'll find tools that fit, then check what's in stock at their store. We'll change the stock and see how the results change.

The notebook uses 24 fictional products and two stores. It supplies the request fields and creates small example vectors from fixed word groups. It doesn't parse the sentence or call a trained model.

## Run the notebook

Open [Retail_Search_Context.py](Retail_Search_Context.py) on **Serverless** and run one cell at a time. This lab is standalone; don't run the foundation setup notebook first.

Start with all of step 0: choose and install libraries, restart Python, load shared setup, create or reopen your lab project, and load query helpers. Continue when the project reports `READY` and the helpers are loaded. Each executable cell has a short **What**, **Why**, and **Look for** introduction.

| Step | What we'll do | Minutes |
|---|---|---|
| 0 | Connect to our own lab project | 5 |
| 1 | Load the products and store inventory | 5 |
| 2 | Look up a SKU and a barcode | 5 |
| 3 | Set the customer's requirements | 5 |
| 4 | Compare vector and keyword results | 12 |
| 5 | Recommend only the store's regular lineup | 10 |
| 6 | Try a request nothing can satisfy | 3 |
| 7 | Restock a product and search again | 8 |
| 8 | Check the results and discuss a real test | 7 |

These are teaching allocations, totaling 60 minutes. Step 4 defines the query; step 4a runs it. Step 4b is an optional five-minute weight experiment. You can skip 4b and continue to step 5; later searches use the original weights. Keep the no-result exercise in step 6. The final check reports eight answers with 4b, or seven when it was skipped.

## What to notice

- An exact SKU or barcode returns a product even when that store has zero stock.
- Product requirements and store rules determine which products qualify before ranking.
- Vector and keyword rankings can disagree. We combine their ranks into one result list.
- `CORE` means regular lineup; `EXTENDED` means carried outside it. Both can have stock. The CORE-only exercise is a fictional merchandising policy that can exclude stocked products.
- If nothing meets the requirements, the query returns no rows.
- Restocking one product can change the top result without changing the query.

These examples show query behavior. They don't measure production search quality, speed, or cost. A real test needs representative queries, expected results, real embeddings, and a way to extract and validate the request fields.

## Setup and lifecycle

Each participant needs a workspace with Lakebase Search and Serverless notebooks, package access for the install cells, and permission to create a project and enable Search.

The project setup cell creates or reuses the participant's dedicated PostgreSQL 17 project. Reuse requires the expected owner and both tags: `purpose=lakebase-workshop` and `lab=retail-search-context`. It configures `production/primary` at 1/1 CU with a one-hour idle timeout, creates the participant schema, and checks exact, ANN, and BM25 queries.

Enabling Search cannot be undone and restarts the project's compute. Setup enables it only when needed, waits, and reconnects. If provisioning is still in progress, rerun the project setup cell. If it reports `NOT READY`, stop and follow the instructor.

The connection helpers retry once after an idle disconnect. If Python restarts later, rerun setup and the exercises in order.

The table-build cell resets the lab's two tables. The restock cell returns the shelf count to zero. **Closing the notebook doesn't delete the project.** When finished, check the project name printed during setup, confirm ownership and the lab tag, and delete that project when you no longer need it.

## Advance setup

Release `2026-11-06a` supports a full preparation run from the session setup notebook. That job supplies `prepare_for_session=true`, the matching `release_version`, a `validation_run_id`, and `session_environment=6`, and runs on environment version 6. The lab checks those parameters before opening a project.

Preparation requires all eight exercises, including step 4b. Its final cell verifies 24 products, 48 inventory rows, and Pine Junction's NorthPeak stock reset to zero, then returns a structured result. The project and data stay available for the session. In an interactive run, that reporting cell is skipped and the normal lab behavior stays the same.

## Files

- [Retail_Search_Context.py](Retail_Search_Context.py): runnable notebook
- [DEMO.md](DEMO.md): short presenter cues and expected results
- [TASKS.md](TASKS.md): current changes and historical validation

## Attribution

This lab was added in a modified fork of the [Databricks Solutions Lakebase Workshop](https://github.com/databricks-solutions/lakebase-workshop). It is distributed under the repository's [DB License](../../LICENSE.md). See [NOTICE](../../NOTICE.md).
