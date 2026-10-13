# Demo Design: Store-Aware Retail Search

> **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.

## Goal

Show one thing clearly with fictional data:

> The same search request can return a different winner for two stores because a store rule changes which products are eligible before ranking.

The lab is useful only if participants can point to the exact SQL rule that changed the answer.

## Architecture

```text
request
  |
  +-- exact SKU or UPC --> B-tree equality lookup
  |
  +-- natural-language query
        +-- parser outputs: product type, voltage, kit type, excluded brands
        +-- one query text --> deterministic demo vector
        +-- one query text --> BM25 query
        +-- store join --> carried and in-stock products only
        +-- optional CORE-only filter
        +-- vector rank + BM25 rank --> weighted RRF
        +-- result: SKU, ranks, quantity, price, aisle, and rule
```

Parsing is out of scope. The notebook passes the fields directly so every predicate stays visible.

## Data

All names, products, stores, SKUs, UPCs, prices, quantities, and aisles are synthetic.

- `retail_product_search`: 24 fictional products with exact identifiers, typed attributes, search text, and an 8-dimensional teaching vector.
- `retail_store_product`: two fictional stores with assortment status, on-hand quantity, price, and aisle.

Five products match the typed request. Excluding EmberVale leaves four. Each store still has two
in-stock `CORE` candidates, and Pine Junction also includes one `CORE` stockout.

## Prepared request

Natural language:

```text
20V brushless tool for driving long screws into deck boards, tool only, exclude EmberVale
```

Parser outputs supplied by the lab:

```text
product_type = impact_driver
voltage = 20
kit_type = TOOL_ONLY
excluded_brands = [EmberVale]
search_text = tool for driving long screws into deck boards
```

The exact identifier exercise uses SKU `10001234`.

## Expected answers

| Exercise | Expected result | Why |
|----------|-----------------|-----|
| Exact SKU | `10001234` | Indexable equality route |
| Base hybrid, Pine Junction | `10001235` | Best weighted result across carried, in-stock products |
| Base hybrid, River Bend | `10001235` | Same request and same base winner |
| Keyword-forward hybrid, River Bend | `10001234` | BM25 and vector disagree, so the weights matter |
| Store-aware, Pine Junction | `10001237` | The strongest lexical match is out of stock; this wins against the other eligible `CORE` product |
| Store-aware, River Bend | `10001235` | It wins against the other eligible `CORE` product at this store |
| Negative case | No rows | Every eligible matching brand is explicitly excluded |
| Restock what-if, Pine Junction | `10001234` | Restocking the out-of-stock `CORE` product makes it eligible and it wins both search ranks |

The notebook checks every expected result.

Before those exercises, the first executable lab cell must report `READY`. It checks project access,
PostgreSQL, the production endpoint, OAuth, Search preload, both extensions, the setup schema, and a
rolled-back exact/vector/BM25 probe. If it reports `NOT READY`, stop there. Do not debug setup during
the customer-driven session.

## 60-minute facilitation flow

1. Start with the current split in 5 minutes: retrieve products, then add store data. Ask whether store rules should affect the candidate set earlier.
2. Inspect the schema and expected answers in 5 minutes.
3. Look up the exact SKU in 8 minutes. Exact IDs do not need vector search.
4. Run the hybrid query in 12 minutes. Compare vector and BM25 ranks, then flip the weights once.
5. Run the same query for both stores in 13 minutes. Ask which SQL predicates changed the candidates.
6. Run the negative case in 6 minutes. Return no rows rather than ignore a hard constraint.
7. Change one stock value in 7 minutes. Predict the result before rerunning.
8. Close in 4 minutes. Decide whether to test this with representative queries and expected answers.

## Keep this honest

- The encoder is a teaching fixture, not an embedding model.
- Do not treat the scores or 24 rows as a benchmark for relevance, latency, cost, or scale.
- Price and aisle are displayed but do not affect ranking.
- The candidate limit, weights, and rule order are demo choices. Test them before using this pattern.
- A split retrieval path may support the same rule. This lab only shows how to do it in one SQL query.
- Enabling Lakebase Search restarts computes and cannot be undone. Pre-enable it in a dedicated workshop project.

## Visual style

Keep the output short. Show the rule beside each result.
