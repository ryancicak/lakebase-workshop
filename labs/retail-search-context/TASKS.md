# Store-Aware Retail Search: Implementation Status

> **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.

## Behavior implemented

| Requirement | Evidence in the lab |
|-------------|---------------------|
| Exact SKU and UPC lookup | SKU and UPC use an indexable equality predicate |
| Product filters | Product type, voltage, kit type, and excluded brands are bound SQL parameters |
| Vector and BM25 disagree | Vector ranks `10001235` first; BM25 ranks `10001234` first; swapping weights changes the winner |
| Store filters run before ranking | Both candidate queries join store assortment and stock before calculating ranks |
| Stock changes the result | Pine Junction has a `CORE` stockout, two eligible `CORE` candidates, and a reversible restock assertion |
| Readiness fails before fixture creation | The first lab cell checks the pre-created project, endpoint, OAuth, Search preload, extensions, schema, ownership, and a rolled-back exact/vector/BM25 probe |
| Failures stop the run | Missing Search extensions raise, expected answers assert, and the negative case must return no rows |
| Reruns touch only lab tables | The notebook drops and recreates only `retail_product_search` and `retail_store_product` |

## Validation completed on 2026-10-13

- `uv run --no-project python scripts/validate_workshop.py`: passed.
- Databricks Asset Bundle validation: passed with only the repository's existing unmatched exclude-pattern warnings.
- `bash -n setup.sh`: passed.
- `git diff --check`: passed.
- Manifest self-check: passed with no problems.
- Deterministic fixture check: confirmed opposite vector and BM25 leaders, plus a winner change when the weights are swapped.

## Cross-cloud live validation on 2026-10-13

The unchanged notebook at tested commit `cde3b56` passed twice in isolated, Search-enabled Lakebase projects on both clouds.

| Cloud | PostgreSQL | Runs | Result |
|-------|------------|------|--------|
| AWS | 17.11 | 2 | All assertions passed, including the reversible restock and final 8/8 check |
| Azure | 17.11 | 2 | All assertions passed, including the reversible restock and final 8/8 check |

Independent checks on both clouds confirmed 24 products, 48 store rows, exactly two eligible `CORE` candidates per store, and restored stock after the mutation exercise. Exact, vector, and BM25 plans were inspected. The actual 24-row queries correctly favored sequential scans at this size. Azure index-form plans also confirmed the intended exact-key, ANN, and BM25 access paths.

The two lab tables, temporary workspace files, jobs, and dedicated validation projects were removed after testing. Run the same preflight in the exact session workspace before live use. This synthetic fixture is not a scale, latency, relevance, or production benchmark.

## First-cell preflight validation on 2026-10-13

The preflight implementation at commit `16e38df` was tested from the same file hashes on new 1/1 CU PostgreSQL 17 projects in both AWS and Azure.

| Cloud | Search off | Search enabled, run 1 | Search enabled, run 2 |
|-------|------------|-----------------------|-----------------------|
| AWS | Stopped at `NOT READY`; no fixture, probe, or extension state changed | `READY`, all 13 checks passed, final 8/8 | `READY`, all 13 checks passed, final 8/8 |
| Azure | Stopped at `NOT READY`; no fixture, probe, or extension state changed | `READY`, all 13 checks passed, final 8/8 | `READY`, all 13 checks passed, final 8/8 |

Both enabled runs exercised exact lookup, ANN, and BM25 table, index, and query operations in one transaction. The transaction was rolled back, extension state was unchanged, and no readiness-probe object remained. The second run also proved that the current user could replace the existing lab tables. The reversible stock change returned to zero on both clouds.

All temporary workspace files, jobs, lab tables, and dedicated projects were removed after testing. The session workspace still needs to run this preflight itself before live use.
