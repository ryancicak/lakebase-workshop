# Presenting the retail Search lab

> Added by Ryan Cicak on 2026-10-13. Presentation wording revised 2026-11-06.

A customer wants a driver for long deck screws. We find tools that meet the requirements, check each store's stock and lineup, then restock one product and repeat the search.

Use the notebook's short introductions. Say what to expect, run the cell, and read the result before moving on. The cues below are reminders, not a script.

## The request and the data

> I need a 20V impact driver for long deck screws. Tool only. Not EmberVale.

We supply these fields manually:

```text
product_type = impact_driver
voltage = 20
kit_type = TOOL_ONLY
excluded_brands = [EmberVale]
search_text = tool for driving long screws into deck boards
```

The notebook doesn't interpret that sentence. Its vectors count words in eight fixed groups; they aren't produced by a trained model.

Two tables hold the fictional examples:

| Table | What it contains |
|---|---|
| Product catalog | 24 products: SKU, barcode, brand, attributes, description, example vector |
| Store inventory | 48 rows: each product at two stores, with lineup, stock, price, and aisle |

`CORE` means regular lineup. `EXTENDED` means carried outside that lineup. Both can be in stock. The CORE-only exercise is a fictional merchandising policy, so it can exclude a product the store has on the shelf.

## The 60-minute flow

| Step | Minutes | Short cue | What to show |
|---|---|---|---|
| 0. Connect | 5 | "These cells connect each of us to our own project." | Run the library, restart, shared setup, project, and helper cells in order. Continue after READY and helpers loaded |
| 1. Load data | 5 | "Here's the catalog, and here's what each store has available." | One cell prepares Python lists; the next writes the two tables. NorthPeak 10001234 has no stock at Pine Junction |
| 2. Exact lookup | 5 | "We have a barcode, so we can look this up." | SKU and UPC return the same product, including its zero stock |
| 3. Requirements | 5 | "These are the requirements. The description helps rank what remains." | Prepared fields reduce 24 products to 4. No parser is running |
| 4. Rankings | 12 | "This cell loads the query. The next one runs it." | Step 4 defines SQL and the call function. Step 4a runs both stores. Step 4b is the optional weight experiment |
| 5. Regular lineup | 10 | "ForgeWorks has stock here, but it's outside the regular lineup." | Predict which products remain at each store before running. Explain Pine Junction's stocked ForgeWorks exclusion |
| 6. No result | 3 | "Nothing fits. We shouldn't quietly remove one of the requirements." | Excluding every suitable brand returns zero rows. This exercise is required |
| 7. Restock | 8 | "NorthPeak is back in stock. Which result do you expect now?" | Stock changes from 0 to 5, NorthPeak wins, then stock resets and BlueOak returns |
| 8. Check and discuss | 7 | "Which of your queries would be useful to test this way?" | Read the checks, discuss representative queries, and confirm project cleanup |

For step 4a: "Both stores start with ForgeWorks. Now compare the two rankings."

For optional step 4b: "Which product wins if we give keywords a little more weight?"

Step 4b takes five minutes of the twelve-minute ranking block. Skip it if needed and continue to step 5. Later searches use the original weights. The summary checks eight answers when 4b ran, or seven when skipped. Don't skip step 6.

## Expected results

| Exercise | Expected result | Why |
|---|---|---|
| Exact SKU and UPC | NorthPeak `10001234`, zero stock at Pine Junction | An exact ID finds the product; it doesn't guarantee stock |
| Base search, Pine Junction | ForgeWorks `10001235` | Highest combined rank among carried, in-stock products |
| Base search, River Bend | ForgeWorks `10001235` | The default blend gives vector rank slightly more weight |
| Optional keyword-weight experiment, River Bend | NorthPeak `10001234` | Giving keyword rank more weight reverses the first two products |
| CORE-only, Pine Junction | BlueOak `10001237` | ForgeWorks is outside the regular lineup; NorthPeak 10001234 is out of stock. BlueOak wins the blend among the remaining two |
| CORE-only, River Bend | ForgeWorks `10001235` | It is in the regular lineup and in stock |
| No suitable brand | No rows | All suitable brands are excluded |
| Restock, Pine Junction | NorthPeak `10001234` | Now eligible, it ranks first by both vector and keyword. Resetting stock restores BlueOak |

At River Bend, ForgeWorks and NorthPeak 10001234 have opposite first and second ranks. **They would tie with equal weights.** The default weights are unequal, so don't describe the displayed result as a tie. The same distinction matters for Pine Junction's two CORE candidates.

The final checks confirm these prepared examples. They don't grade search quality.

## Setup and cleanup

Run the setup cells before loading products. The project cell creates or reuses only the participant's expected owned project, with `purpose=lakebase-workshop` and `lab=retail-search-context`. It prepares PostgreSQL 17, `production/primary` at 1/1 CU, a one-hour idle timeout, Search, and the participant schema.

Enabling Search restarts compute and can't be undone. The cell waits and reconnects. Rerun it if provisioning is still in progress; if it reports NOT READY, the participant follows the instructor. Don't switch to the foundation setup notebook.

The table-build cell resets the examples. The restock cell resets its own stock change. Closing the notebook leaves the project in place. At the end, check the printed project name, ownership, and lab tag before deleting it.

## Keep the explanation precise

- Prepared fields enforce product requirements; the description contributes to ranking.
- The vector calculation uses fixed word groups, not learned meaning.
- Price and aisle are displayed, but don't affect ranking.
- The CORE filter is an example store policy, not a universal retail rule.
- This query shows one way to combine search and inventory. Other application designs can apply the same rules.
- Real search quality, performance, and cost need a separate test with representative data.
