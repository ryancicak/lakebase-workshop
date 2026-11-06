# Store-Aware Retail Search: Implementation Status

> **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.

## Preparation support: release 2026-11-06a

The READY notebook downloads this release's exact source files and runs both main labs under
the participant's identity on serverless environment version 6. In preparation mode, this
Search notebook runs all eight answer checks, verifies the fixture counts and stock reset,
and returns completion evidence tied to the release, identity, and preparation run.

The lab project and data remain for the session. During the live exercise, the notebook runs
normally from the top and can still skip the optional weight experiment.

The earlier cloud results below apply to their recorded source hashes. They do not establish
live validation of this release's new preparation path. Read the current READY result and its
job evidence for the workspace and identity being prepared.

## Presentation revision: November 6, 2026

The notebook now introduces every executable cell with what it does, why it runs, and what to
notice. The request is for a 20V impact driver, tool only, excluding EmberVale. The fields are
supplied manually, and the example vectors use fixed word groups rather than a trained model.

The presentation plan is 60 minutes: 5/5/5/5/12/10/3/8/7 for steps 0 through 8. Setup is a
sequence of cells. Step 4 defines the SQL; 4a runs it. Step 4b is optional and later searches
use the original weights. The final summary checks eight answers when 4b ran, or seven when
skipped. The no-result exercise remains required. The restock cell resets stock even if its
search check raises, and the notebook explains that closing it does not delete the project.

README.md and DEMO.md describe this revision. The runtime hashes and cloud runs below are
historical evidence for an older version. They do **not** establish live validation of these
November 6 changes. Review the final notebook cell by cell in the UI and record new runtime
evidence separately.

## Lab behavior already implemented

| Requirement | Evidence in the lab |
|-------------|---------------------|
| Exact SKU and UPC lookup | SKU and UPC use an indexable equality predicate |
| Product filters | Product type, voltage, kit type, and excluded brands are bound SQL parameters |
| Vector and BM25 disagree | Vector ranks `10001235` first; BM25 ranks `10001234` first; swapping weights changes the winner |
| Store filters run before ranking | Both candidate queries join store assortment and stock before calculating ranks |
| Stock changes the result | Pine Junction has a `CORE` stockout, two eligible `CORE` candidates, and a reversible restock assertion |
| Standalone preparation | After the install and shared setup cells, the project cell creates or safely reuses the exact owned, tagged project before fixture creation |
| Failures stop the run | Missing Search extensions raise, expected answers assert, and the negative case must return no rows |
| Idle pauses recover | The project stays on through an idle hour; if scale-to-zero closes the connection, the helpers reconnect and retry the statement once |
| Reruns touch only lab tables | The notebook drops and recreates only `retail_product_search` and `retail_store_product` |

## Standalone preparation

The lab is fully standalone. After the install and shared setup cells, its project setup cell:

- derives the current participant's exact project and schema names
- creates a distinct PostgreSQL 17 project when missing
- configures the exact `production/primary` endpoint at 1/1 CU with a 1-hour scale-to-zero timeout
- applies `purpose=lakebase-workshop` and `lab=retail-search-context`
- enables Lakebase Search if needed, then waits for its compute restart
- reconnects with a fresh OAuth credential
- creates only the participant schema
- runs and rolls back the exact, ANN, and BM25 readiness probe
- resumes safely on rerun, accepting only the participant's exact owned project with both tags

Each participant must be able to create projects and enable Search. The lab does not call
`notebooks/00_Setup_Lakebase_Project`, use the foundation project, or seed its tables.

## Historical validation before the November 6 revision

Validated on AWS and Azure with the 1-hour idle timeout and reconnect helpers:

| Check | Result |
|-------|--------|
| Static validator | 46 checks, 0 failures, 2 existing warnings |
| Routing, identity, and contract tests | 12 passed |
| AWS and Azure cold run and warm rerun | `READY 13/13`; lab `8/8` on both runs |
| AWS and Azure 13-minute idle pause | Compute stayed on; no reconnect needed; lab `8/8` |
| AWS and Azure forced scale-to-zero | Two suspends per run; the next read and the next write each reconnected once; lab `8/8` |

On both clouds, the cold run created the missing dedicated PostgreSQL 17 project with a 1-hour
scale-to-zero timeout and enabled Search. The warm rerun reused the same project, branch, and
endpoint without enabling Search again. To force a real suspend, the test shortened only its own
project's timeout to 1 minute. Independent checks confirmed the exact owner and tags; 1/1 CU;
PostgreSQL 17.11; both Search extensions; 24 products and 48 store rows; restored stock; zero probe
objects; and zero foundation tables. Guarded cleanup completed, and both projects were confirmed
absent afterward.

Validated runtime hashes:

- `_setup.py`: `a408f8124eec18fa68918fe74e579ce5861186ed4f30739d68719bcdd5ed8491`
- Retail notebook: `00c87461e55cd1f4ba1759de53397ec323a2f4dd9cf8070b6c9c7e4555398126`

This is synthetic lab evidence only. It is not a performance, relevance, scale, cost, production,
or customer result.
