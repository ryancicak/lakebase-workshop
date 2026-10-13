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
| Failures stop the run | Missing Search extensions raise, expected answers assert, and the negative case must return no rows |
| Reruns touch only lab tables | The notebook drops and recreates only `retail_product_search` and `retail_store_product` |

## Validation completed on 2026-10-13

- `uv run --no-project python scripts/validate_workshop.py`: passed.
- Databricks Asset Bundle validation: passed with only the repository's existing unmatched exclude-pattern warnings.
- `bash -n setup.sh`: passed.
- `git diff --check`: passed.
- Manifest self-check: passed with no problems.
- Deterministic fixture check: confirmed opposite vector and BM25 leaders, plus a winner change when the weights are swapped.

## Before using this live

- Run it once in a Search-enabled sandbox.
- Run it a second time to prove database cleanup and expected answers are repeatable.
- Inspect and record the `EXPLAIN` output. With 24 rows, a sequential scan can be the right plan.

Run those checks in an isolated, Search-enabled development project.
