# Lakebase Search (Beta)

Add **vector (semantic)**, **keyword (BM25)**, and **hybrid** search directly inside your Lakebase Postgres database — no separate search service.

## Labs

| Order | Lab | What You'll Learn |
|-------|-----|-------------------|
| 1 | `Lakebase_Search` | Install `lakebase_vector` + `lakebase_text`, build ANN + BM25 indexes, run vector / keyword / hybrid (RRF) queries |

## Prerequisites

- A Lakebase project — your workshop project from **`00_Setup_Lakebase_Project`** by default, or any existing project via the notebook's **`project_id`** widget. This lab doesn't depend on any other workshop notebook.
- **Lakebase Search enabled** for that project (Beta) — a workspace admin enables Beta access from the **Previews** page; then enable it in the project **Settings → Lakebase Search**
- **Postgres 16+**

## ⚠️ Enablement is irreversible

Enabling Lakebase Search **restarts all computes in the project**, makes the extensions installable, and **cannot be turned off**. The notebook never enables Search itself: it checks first and, if Search is off, stops with a link to the setting, so it's safe to run before enabling.

## Key Concepts

- **`lakebase_vector`** — ANN vector search via the `lakebase_ann` index; a drop-in companion to pgvector (same vector types, `<=>` / `<->` / `<#>` operators). IVF + RaBitQ quantization; storage-backed, survives scale-to-zero.
- **`lakebase_text`** — BM25 full-text search via the `lakebase_bm25` index; compatible with Postgres `tsvector`. Use the `<@>` operator with `to_bm25query(...)` (lower score = more relevant). Build the index **after** inserting data.
- **Hybrid search (RRF)** — run both searches, then fuse the ranked lists with Reciprocal Rank Fusion: `Σ 1 / (k + rank)`.
- **Embeddings** — generate with a model (Databricks Model Serving / `ai_query`); the `VECTOR(n)` dimension must match the model output.

## Documentation

- [Lakebase Search](https://docs.databricks.com/aws/en/oltp/projects/lakebase-search)
- [lakebase_vector](https://docs.databricks.com/aws/en/oltp/projects/lakebase-vector)
- [lakebase_text](https://docs.databricks.com/aws/en/oltp/projects/lakebase-text)
- [Postgres extensions](https://docs.databricks.com/aws/en/oltp/projects/extensions)
