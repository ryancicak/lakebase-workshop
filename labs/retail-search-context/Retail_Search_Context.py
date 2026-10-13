# Databricks notebook source
# MAGIC %md
# MAGIC # Store-Aware Retail Search
# MAGIC
# MAGIC > **Modification notice:** Added by Ryan Cicak on 2026-10-13 as a fictional, store-aware retail search lab.
# MAGIC
# MAGIC **Path:** Store-Aware Retail Search &nbsp;|&nbsp; **Prerequisite:** `00_Setup_Lakebase_Project`
# MAGIC
# MAGIC This lab tests one thing:
# MAGIC
# MAGIC > Can the same query return a different result by store, and can we explain why in SQL?
# MAGIC
# MAGIC You will:
# MAGIC
# MAGIC 1. Look up an exact SKU or UPC.
# MAGIC 2. Run one query through vector and BM25 search.
# MAGIC 3. Apply product and store filters before ranking.
# MAGIC 4. Compare the same query at two stores.
# MAGIC 5. Restock one product and rerun without changing the query.
# MAGIC
# MAGIC All products, brands, stores, identifiers, prices, quantities, and aisles are fictional.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before you run
# MAGIC
# MAGIC This lab requires PostgreSQL 16 or later and Lakebase Search enabled on the workshop project.
# MAGIC Enabling Search restarts every compute in the project, drops active connections, and cannot be
# MAGIC undone. Enable it ahead of time in a dedicated workshop project.
# MAGIC
# MAGIC The notebook does not enable Search or change project settings. If the extensions are not
# MAGIC available, it fails immediately instead of skipping the exercises.
# MAGIC
# MAGIC This 24-row fixture shows SQL behavior only. It does not measure relevance, latency, cost,
# MAGIC recall, or production scale, and it does not compare search systems.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0" "protobuf>=5.29.5,<6" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

import math
import re

conn = get_connection("production")
print("✓ Connected to production branch")


def execute(sql, params=()):
    """Execute a statement and commit it. Roll back and raise on any error."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def fetch_rows(sql, params=(), finish_transaction=True):
    """Return query rows as dictionaries."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        if finish_transaction:
            conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install the Search extensions
# MAGIC
# MAGIC `lakebase_vector` supplies the vector type and ANN index. `lakebase_text` supplies the BM25
# MAGIC index and query operator. The lab owns only its two tables and rebuilds them on every run.

# COMMAND ----------

try:
    execute("CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE;")
    execute("CREATE EXTENSION IF NOT EXISTS lakebase_text;")
except Exception as exc:
    print("✗ Lakebase Search extensions are unavailable for this project.")
    print("  Enable Search on a PostgreSQL 16+ workshop project, then rerun the notebook.")
    raise RuntimeError("Lakebase Search is required for this lab") from exc

print("✓ Lakebase Search extensions ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build repeatable demo vectors
# MAGIC
# MAGIC This lab uses one deterministic teaching function for both product and query text. That keeps
# MAGIC the result repeatable and makes the vector behavior easy to inspect.
# MAGIC
# MAGIC This is not an embedding model. A real evaluation would replace `demo_embedding` with one
# MAGIC chosen embedding endpoint for both the corpus and each query.

# COMMAND ----------

CONCEPT_AXES = (
    {"impact", "driver", "drive", "driving", "fasten", "fastening", "screw", "screws", "bolt", "bolts", "tool", "torque"},
    {"drill", "drilling", "hole", "holes", "masonry"},
    {"saw", "cut", "cutting", "blade", "jigsaw", "reciprocating"},
    {"outdoor", "leaf", "blower", "trim", "trimmer", "hedge", "yard"},
    {"vacuum", "wet", "dry", "dust", "cleanup", "clean", "sand", "sander", "sanding", "grind", "grinder"},
    {"deck", "decks", "decking", "board", "boards", "framing", "long", "structural", "lumber"},
    {"battery", "charger", "light", "cordless", "power", "brushless"},
    {"socket", "wrench", "hand", "ratchet", "manual", "screwdriver"},
)


def demo_embedding(text):
    """Return a normalized VECTOR(8) literal from transparent concept counts."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    values = [0.05 + sum(token in axis for token in tokens) for axis in CONCEPT_AXES]
    norm = math.sqrt(sum(value * value for value in values))
    return "[" + ",".join(f"{value / norm:.6f}" for value in values) + "]"


print("✓ Deterministic teaching encoder ready: 8 visible concept axes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build a fictional product catalog and two stores
# MAGIC
# MAGIC `retail_product_search` owns exact identifiers, typed product attributes, search text, and a
# MAGIC vector. `retail_store_product` owns the volatile store context: assortment, quantity, price,
# MAGIC and aisle.
# MAGIC
# MAGIC The target products have different `CORE` and `EXTENDED` status by store. Pine Junction also
# MAGIC has one `CORE` stockout so we can see how assortment and availability change the eligible set.

# COMMAND ----------

PRODUCTS = [
    (1, "10001234", "850000100001", "NorthPeak", "impact_driver", 20, "TOOL_ONLY",
     "NorthPeak 20V Brushless Impact Driver, Tool Only",
     "Compact driver for driving long screws into deck boards. Cordless battery platform with brushless power.", 129.00),
    (2, "10001235", "850000100002", "ForgeWorks", "impact_driver", 20, "TOOL_ONLY",
     "ForgeWorks 20V Brushless Impact Driver, Tool Only",
     "High torque fastening tool built for structural decking and long framing boards. Cordless body only.", 139.00),
    (3, "10001236", "850000100003", "EmberVale", "impact_driver", 20, "TOOL_ONLY",
     "EmberVale 20V Brushless Impact Driver, Tool Only",
     "Brushless tool for long structural screws, decks, and framing. Cordless body only.", 119.00),
    (4, "10001237", "850000100004", "BlueOak", "impact_driver", 20, "TOOL_ONLY",
     "BlueOak 20V Compact Impact Driver, Tool Only",
     "Balanced cordless impact driver for cabinet screws and general fastening.", 109.00),
    (5, "10001238", "850000100005", "NorthPeak", "impact_driver", 20, "TOOL_ONLY",
     "NorthPeak 20V Impact Driver, Tool Only",
     "Compact cordless driver for screws and light fastening work.", 99.00),
    (6, "10001239", "850000100006", "ForgeWorks", "impact_driver", 20, "KIT",
     "ForgeWorks 20V Impact Driver Kit",
     "Cordless impact driver with battery, charger, and case for deck fasteners.", 189.00),
    (7, "10001240", "850000100007", "BlueOak", "drill_driver", 20, "TOOL_ONLY",
     "BlueOak 20V Brushless Drill Driver, Tool Only",
     "Cordless drill for wood holes, pilot holes, and everyday drilling.", 119.00),
    (8, "10001241", "850000100008", "CedarRun", "screwdriver", 12, "KIT",
     "CedarRun 12V Cordless Screwdriver Kit",
     "Light cordless screwdriver for furniture assembly and small screws.", 69.00),
    (9, "10001242", "850000100009", "NorthPeak", "impact_wrench", 20, "TOOL_ONLY",
     "NorthPeak 20V Impact Wrench, Tool Only",
     "High torque cordless wrench for wheel nuts, sockets, and large bolts.", 159.00),
    (10, "10001243", "850000100010", "StoneRiver", "corded_drill", None, "CORDED",
     "StoneRiver 8A Corded Drill",
     "Variable speed drill for steel, wood, and repeated hole drilling.", 79.00),
    (11, "10001244", "850000100011", "PineArc", "circular_saw", 20, "TOOL_ONLY",
     "PineArc 20V Circular Saw, Tool Only",
     "Cordless circular saw and blade for framing lumber and straight cuts.", 149.00),
    (12, "10001245", "850000100012", "BlueOak", "jigsaw", 20, "TOOL_ONLY",
     "BlueOak 20V Jigsaw, Tool Only",
     "Cordless jigsaw for curved cuts in wood and sheet material.", 109.00),
    (13, "10001246", "850000100013", "ForgeWorks", "reciprocating_saw", 20, "TOOL_ONLY",
     "ForgeWorks 20V Reciprocating Saw, Tool Only",
     "Cordless reciprocating saw for demolition cuts and pruning blades.", 139.00),
    (14, "10001247", "850000100014", "EmberVale", "oscillating_tool", 20, "TOOL_ONLY",
     "EmberVale 20V Oscillating Multi-Tool, Tool Only",
     "Cordless tool for flush cutting, sanding, and detail work.", 129.00),
    (15, "10001248", "850000100015", "CedarRun", "orbital_sander", 20, "TOOL_ONLY",
     "CedarRun 20V Random Orbit Sander, Tool Only",
     "Cordless sander for smooth finishing and dust-controlled sanding.", 99.00),
    (16, "10001249", "850000100016", "StoneRiver", "angle_grinder", 20, "TOOL_ONLY",
     "StoneRiver 20V Angle Grinder, Tool Only",
     "Cordless grinder for cutting, grinding, and surface cleanup.", 119.00),
    (17, "10001250", "850000100017", "PineArc", "work_light", 20, "TOOL_ONLY",
     "PineArc 20V Folding Work Light, Tool Only",
     "Cordless LED light for jobsite and emergency illumination.", 59.00),
    (18, "10001251", "850000100018", "GardenTrail", "leaf_blower", 20, "TOOL_ONLY",
     "GardenTrail 20V Leaf Blower, Tool Only",
     "Cordless outdoor blower for leaves, yard cleanup, and patios.", 89.00),
    (19, "10001252", "850000100019", "GardenTrail", "hedge_trimmer", 20, "TOOL_ONLY",
     "GardenTrail 20V Hedge Trimmer, Tool Only",
     "Cordless outdoor trimmer for hedges, shrubs, and yard edges.", 99.00),
    (20, "10001253", "850000100020", "WorkshopBay", "wet_dry_vacuum", None, "CORDED",
     "WorkshopBay 8 Gallon Wet Dry Vacuum",
     "Wet and dry shop vacuum for dust, debris, and jobsite cleanup.", 119.00),
    (21, "10001254", "850000100021", "NorthPeak", "battery_kit", 20, "KIT",
     "NorthPeak 20V Battery and Charger Kit",
     "Two cordless tool batteries with a fast charger.", 149.00),
    (22, "10001255", "850000100022", "ForgeWorks", "driver_bit_set", None, "ACCESSORY",
     "ForgeWorks 45-Piece Impact Driver Bit Set",
     "Driver bits for deck screws, fasteners, and common screw heads.", 29.00),
    (23, "10001256", "850000100023", "StoneRiver", "ratchet_set", None, "HAND_TOOL",
     "StoneRiver 40-Piece Ratchet and Socket Set",
     "Hand tool set with ratchet, sockets, extensions, and wrench adapters.", 69.00),
    (24, "10001257", "850000100024", "CedarRun", "deck_screws", None, "ACCESSORY",
     "CedarRun Exterior Deck Screws, 5 Pound Box",
     "Long coated screws for outdoor deck boards and framing connections.", 39.00),
]

STORE_NAMES = {
    "store_pine": "Pine Junction",
    "store_river": "River Bend",
}

TARGET_STATUS = {
    ("store_pine", 1): "CORE",
    ("store_pine", 2): "EXTENDED",
    ("store_pine", 3): "CORE",
    ("store_pine", 4): "CORE",
    ("store_pine", 5): "CORE",
    ("store_river", 1): "EXTENDED",
    ("store_river", 2): "CORE",
    ("store_river", 3): "CORE",
    ("store_river", 4): "CORE",
}

TARGET_ON_HAND = {
    ("store_pine", 1): 0,
    ("store_pine", 4): 6,
}


def build_store_rows():
    rows = []
    for product in PRODUCTS:
        product_id, _, _, _, _, _, _, _, _, base_price = product
        for store_index, store_id in enumerate(STORE_NAMES):
            status = "CORE" if (product_id + store_index) % 4 == 0 else "EXTENDED"
            if product_id in (11, 18) and store_id == "store_river":
                status = "NOT_CARRIED"
            status = TARGET_STATUS.get((store_id, product_id), status)
            on_hand = 0 if status == "NOT_CARRIED" else 2 + ((product_id * (3 + store_index)) % 13)
            on_hand = TARGET_ON_HAND.get((store_id, product_id), on_hand)
            price_multiplier = 1.00 if store_id == "store_pine" else 1.02
            price = round(base_price * price_multiplier, 2)
            aisle = None if status == "NOT_CARRIED" else f"A{1 + (product_id % 12):02d}"
            rows.append((store_id, product_id, status, on_hand, price, aisle))
    return rows


STORE_ROWS = build_store_rows()

# COMMAND ----------

execute("DROP TABLE IF EXISTS retail_store_product CASCADE;")
execute("DROP TABLE IF EXISTS retail_product_search CASCADE;")

execute("""
    CREATE TABLE retail_product_search (
        product_id       INTEGER PRIMARY KEY,
        sku              TEXT UNIQUE NOT NULL,
        upc              TEXT UNIQUE NOT NULL,
        brand            TEXT NOT NULL,
        product_type     TEXT NOT NULL,
        voltage          SMALLINT,
        kit_type         TEXT NOT NULL,
        title            TEXT NOT NULL,
        description      TEXT NOT NULL,
        embedding        VECTOR(8) NOT NULL,
        search_tsv       TSVECTOR NOT NULL
    );
""")

execute("""
    CREATE TABLE retail_store_product (
        store_id          TEXT NOT NULL,
        product_id        INTEGER NOT NULL REFERENCES retail_product_search(product_id),
        assortment_status TEXT NOT NULL CHECK (assortment_status IN ('CORE', 'EXTENDED', 'NOT_CARRIED')),
        on_hand           INTEGER NOT NULL CHECK (on_hand >= 0),
        price             NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
        aisle             TEXT,
        PRIMARY KEY (store_id, product_id)
    );
""")

product_insert = """
    INSERT INTO retail_product_search (
        product_id, sku, upc, brand, product_type, voltage, kit_type,
        title, description, embedding, search_tsv
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s::vector, to_tsvector('simple', %s)
    );
"""

product_rows = []
for product in PRODUCTS:
    product_id, sku, upc, brand, product_type, voltage, kit_type, title, description, _ = product
    searchable_text = f"{brand} {title} {description}"
    product_rows.append((
        product_id, sku, upc, brand, product_type, voltage, kit_type,
        title, description, demo_embedding(searchable_text), searchable_text,
    ))

try:
    with conn.cursor() as cur:
        cur.executemany(product_insert, product_rows)
        cur.executemany("""
            INSERT INTO retail_store_product (
                store_id, product_id, assortment_status, on_hand, price, aisle
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """, STORE_ROWS)
    conn.commit()
except Exception:
    conn.rollback()
    raise

# Build both Search indexes after the corpus exists. BM25 needs the corpus statistics.
execute("CREATE INDEX retail_product_search_ann ON retail_product_search USING lakebase_ann (embedding vector_cosine_ops);")
execute("CREATE INDEX retail_product_search_bm25 ON retail_product_search USING lakebase_bm25 (search_tsv);")
execute("CREATE INDEX retail_product_search_attributes ON retail_product_search (product_type, voltage, kit_type, brand);")
execute("CREATE INDEX retail_store_product_eligibility ON retail_store_product (store_id, assortment_status, on_hand, product_id);")

counts = fetch_rows("""
    SELECT
        (SELECT count(*) FROM retail_product_search) AS products,
        (SELECT count(*) FROM retail_store_product) AS store_rows,
        (SELECT count(DISTINCT store_id) FROM retail_store_product) AS stores;
""")[0]

assert counts["products"] == 24
assert counts["store_rows"] == 48
assert counts["stores"] == 2
print("✓ Retail corpus built: 24 products across 2 stores")

target_store_context = fetch_rows("""
    SELECT s.store_id, p.sku, p.brand, s.assortment_status, s.on_hand, s.price, s.aisle
    FROM retail_store_product s
    JOIN retail_product_search p USING (product_id)
    WHERE p.sku IN ('10001234', '10001235', '10001237', '10001238')
    ORDER BY s.store_id, p.sku;
""")
print("\nTarget store context")
for row in target_store_context:
    print(dict(row))

pine_context_by_sku = {
    row["sku"]: row for row in target_store_context if row["store_id"] == "store_pine"
}
assert pine_context_by_sku["10001234"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001234"]["on_hand"] == 0
assert pine_context_by_sku["10001237"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001237"]["on_hand"] > 0
assert pine_context_by_sku["10001238"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001238"]["on_hand"] > 0
print("✓ Store fixture includes a CORE stockout and two eligible CORE candidates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Look up an exact SKU or UPC
# MAGIC
# MAGIC Use an equality predicate for SKU and UPC. Do not send exact IDs through vector search or
# MAGIC BM25. PostgreSQL may still scan this 24-row fixture.

# COMMAND ----------

def lookup_identifier(identifier, store_id):
    return fetch_rows("""
        SELECT
            p.sku, p.upc, p.title, p.brand,
            s.assortment_status, s.on_hand, s.price, s.aisle
        FROM retail_product_search p
        LEFT JOIN retail_store_product s
          ON s.product_id = p.product_id
         AND s.store_id = %s
        WHERE p.sku = %s OR p.upc = %s
        ORDER BY (p.sku = %s) DESC
        LIMIT 1;
    """, (store_id, identifier, identifier, identifier))


exact_rows = lookup_identifier("10001234", "store_pine")
assert exact_rows and exact_rows[0]["sku"] == "10001234"
print("Route: exact identifier -> indexable SQL equality")
print(dict(exact_rows[0]))
print("✓ Exact identifier route returned 10001234")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Use explicit query fields
# MAGIC
# MAGIC Parsing is outside this lab. We provide the fields so every constraint is visible in SQL. The
# MAGIC same `search_text` goes to the vector fixture and BM25.

# COMMAND ----------

RAW_QUERY = "20V brushless tool for driving long screws into deck boards, tool only, exclude EmberVale"

REQUEST = {
    "search_text": "tool for driving long screws into deck boards",
    "product_type": "impact_driver",
    "voltage": 20,
    "kit_type": "TOOL_ONLY",
    "excluded_brands": ["EmberVale"],
}

print("Natural language:", RAW_QUERY)
print("Parser outputs supplied by the lab:", REQUEST)
print("Query vector:", demo_embedding(REQUEST["search_text"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Put search and store rules in one SQL query
# MAGIC
# MAGIC The vector and BM25 queries join store data and apply the hard filters before ranking. Stock is
# MAGIC always required. `core_only=True` adds one more filter: the product must be in the store's
# MAGIC `CORE` assortment. Price and aisle are returned but do not affect rank.

# COMMAND ----------

STORE_AWARE_SEARCH_SQL = """
WITH request AS (
    SELECT
        %s::text AS store_id,
        %s::vector AS query_embedding,
        %s::text AS search_text,
        %s::text AS product_type,
        %s::smallint AS voltage,
        %s::text AS kit_type,
        %s::text[] AS excluded_brands,
        %s::boolean AS core_only,
        %s::double precision AS vector_weight,
        %s::double precision AS bm25_weight
),
vector_scored AS (
    SELECT
        p.product_id,
        p.embedding <=> r.query_embedding AS vector_distance
    FROM retail_product_search p
    JOIN retail_store_product s ON s.product_id = p.product_id
    CROSS JOIN request r
    WHERE s.store_id = r.store_id
      AND s.assortment_status IN ('CORE', 'EXTENDED')
      AND s.on_hand > 0
      AND (NOT r.core_only OR s.assortment_status = 'CORE')
      AND p.product_type = r.product_type
      AND p.voltage = r.voltage
      AND p.kit_type = r.kit_type
      AND NOT (p.brand = ANY(r.excluded_brands))
    ORDER BY vector_distance, p.product_id
    LIMIT 20
),
vector_ranked AS (
    SELECT
        product_id,
        vector_distance,
        RANK() OVER (ORDER BY vector_distance) AS vector_rank
    FROM vector_scored
),
keyword_scored AS (
    SELECT
        p.product_id,
        p.search_tsv <@> to_bm25query(
            to_tsvector('simple', r.search_text),
            'retail_product_search_bm25'
        ) AS bm25_score
    FROM retail_product_search p
    JOIN retail_store_product s ON s.product_id = p.product_id
    CROSS JOIN request r
    WHERE s.store_id = r.store_id
      AND s.assortment_status IN ('CORE', 'EXTENDED')
      AND s.on_hand > 0
      AND (NOT r.core_only OR s.assortment_status = 'CORE')
      AND p.product_type = r.product_type
      AND p.voltage = r.voltage
      AND p.kit_type = r.kit_type
      AND NOT (p.brand = ANY(r.excluded_brands))
    ORDER BY bm25_score, p.product_id
    LIMIT 20
),
keyword_ranked AS (
    SELECT
        product_id,
        bm25_score,
        RANK() OVER (ORDER BY bm25_score) AS bm25_rank
    FROM keyword_scored
),
fused AS (
    SELECT
        COALESCE(v.product_id, k.product_id) AS product_id,
        v.vector_distance,
        v.vector_rank,
        k.bm25_score,
        k.bm25_rank
    FROM vector_ranked v
    FULL OUTER JOIN keyword_ranked k USING (product_id)
),
scored AS (
    SELECT
        p.*,
        s.assortment_status,
        s.on_hand,
        s.price,
        s.aisle,
        f.vector_distance,
        f.vector_rank,
        f.bm25_score,
        f.bm25_rank,
        COALESCE(r.vector_weight / (60.0 + f.vector_rank::double precision), 0.0)
          + COALESCE(r.bm25_weight / (60.0 + f.bm25_rank::double precision), 0.0) AS weighted_rrf,
        CASE
            WHEN r.core_only THEN 'CORE and in stock before ranking'
            ELSE 'carried and in stock before ranking'
        END AS rule_applied
    FROM fused f
    JOIN retail_product_search p USING (product_id)
    JOIN retail_store_product s USING (product_id)
    CROSS JOIN request r
    WHERE s.store_id = r.store_id
),
final_ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY weighted_rrf DESC, product_id
        ) AS final_rank,
        *
    FROM scored
)
SELECT
    final_rank,
    sku,
    title,
    brand,
    product_type,
    voltage,
    kit_type,
    vector_distance,
    vector_rank,
    bm25_score,
    bm25_rank,
    weighted_rrf,
    assortment_status,
    on_hand,
    price,
    aisle,
    rule_applied
FROM final_ranked
ORDER BY final_rank
LIMIT 10;
"""


def retail_search(store_id, request, core_only=True, vector_weight=1.15, bm25_weight=1.00):
    params = (
        store_id,
        demo_embedding(request["search_text"]),
        request["search_text"],
        request["product_type"],
        request["voltage"],
        request["kit_type"],
        request["excluded_brands"],
        core_only,
        vector_weight,
        bm25_weight,
    )
    return fetch_rows(STORE_AWARE_SEARCH_SQL, params)


def show_results(label, rows):
    print(f"\n{label}")
    print("rank | sku      | brand       | vec | bm25 | status   | qty | price  | aisle | weighted_rrf")
    print("-----+----------+-------------+-----+------+----------+-----+--------+-------+-------------")
    for row in rows:
        vector_rank = "-" if row["vector_rank"] is None else str(row["vector_rank"])
        bm25_rank = "-" if row["bm25_rank"] is None else str(row["bm25_rank"])
        print(
            f"{row['final_rank']:>4} | {row['sku']:<8} | {row['brand']:<11} | "
            f"{vector_rank:>3} | {bm25_rank:>4} | "
            f"{row['assortment_status']:<8} | {row['on_hand']:>3} | "
            f"{float(row['price']):>6.2f} | {str(row['aisle']):<5} | "
            f"{float(row['weighted_rrf']):.8f}"
        )
    if rows:
        print("Rule:", rows[0]["rule_applied"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Start with carried, in-stock candidates
# MAGIC
# MAGIC With `core_only=False`, both stores allow `CORE` and `EXTENDED` products, while still requiring
# MAGIC positive stock. The same product should win in both stores with the semantic-forward weights.

# COMMAND ----------

pine_base = retail_search("store_pine", REQUEST, core_only=False)
river_base = retail_search("store_river", REQUEST, core_only=False)

show_results("Pine Junction, carried and in stock", pine_base)
show_results("River Bend, carried and in stock", river_base)

assert pine_base and river_base
assert pine_base[0]["sku"] == "10001235"
assert river_base[0]["sku"] == "10001235"

river_base_by_sku = {row["sku"]: row for row in river_base}
assert river_base_by_sku["10001235"]["vector_rank"] < river_base_by_sku["10001234"]["vector_rank"]
assert river_base_by_sku["10001234"]["bm25_rank"] < river_base_by_sku["10001235"]["bm25_rank"]

for row in pine_base + river_base:
    assert row["product_type"] == "impact_driver"
    assert row["voltage"] == 20
    assert row["kit_type"] == "TOOL_ONLY"
    assert row["brand"] != "EmberVale"

print("✓ Typed filters and exclusion stayed in SQL")
print("✓ Vector and BM25 disagree on the leading product")
print("✓ Base hybrid returned the same winner for both stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change the retrieval weights
# MAGIC
# MAGIC The two retrieval signals intentionally disagree. The semantic-forward defaults put SKU
# MAGIC `10001235` first. Flip the weights toward BM25 and SKU `10001234` should win without changing
# MAGIC the query, store, or hard filters. Use the rank columns to explain why the winner changes.

# COMMAND ----------

river_keyword_forward = retail_search(
    "store_river",
    REQUEST,
    core_only=False,
    vector_weight=1.00,
    bm25_weight=1.15,
)
show_results("River Bend, keyword-forward hybrid", river_keyword_forward)
assert river_keyword_forward[0]["sku"] == "10001234"
print("✓ Keyword-forward weights changed the base hybrid winner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Require CORE assortment
# MAGIC
# MAGIC Now set `core_only=True` without changing the query, parser outputs, vector, or BM25 weights.
# MAGIC Each store still has two eligible candidates. At Pine Junction, SKU `10001234` is `CORE` but
# MAGIC out of stock, and SKU `10001237` wins the remaining ranking. At River Bend, SKU `10001235`
# MAGIC stays first. The same policy runs before both candidate lists are ranked.

# COMMAND ----------

pine_store_aware = retail_search("store_pine", REQUEST, core_only=True)
river_store_aware = retail_search("store_river", REQUEST, core_only=True)

show_results("Pine Junction, CORE and in stock before ranking", pine_store_aware)
show_results("River Bend, CORE and in stock before ranking", river_store_aware)

assert pine_store_aware[0]["sku"] == "10001237"
assert pine_store_aware[0]["assortment_status"] == "CORE"
assert pine_store_aware[0]["on_hand"] > 0
assert len(pine_store_aware) == 2
assert river_store_aware[0]["sku"] == "10001235"
assert river_store_aware[0]["assortment_status"] == "CORE"
assert len(river_store_aware) == 2
assert pine_store_aware[0]["sku"] != river_store_aware[0]["sku"]

print("✓ Same query, two stores, two explainable winners")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Negative case: do not weaken hard constraints
# MAGIC
# MAGIC Exclude all four brands with a matching 20V tool-only impact driver. The correct result is no
# MAGIC rows. Do not ignore an exclusion or product filter just to return something.

# COMMAND ----------

negative_request = {
    **REQUEST,
    "excluded_brands": ["NorthPeak", "ForgeWorks", "EmberVale", "BlueOak"],
}

negative_rows = retail_search("store_pine", negative_request, core_only=True)
assert negative_rows == []
print("✓ Negative case returned no eligible products")

# COMMAND ----------

assert exact_rows[0]["sku"] == "10001234"
assert pine_base[0]["sku"] == "10001235"
assert river_base[0]["sku"] == "10001235"
assert pine_store_aware[0]["sku"] == "10001237"
assert river_store_aware[0]["sku"] == "10001235"
assert negative_rows == []
assert river_keyword_forward[0]["sku"] == "10001234"
print("✓ Baseline answers passed: 7/7")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Change one inventory value
# MAGIC
# MAGIC Before running the next cell, predict what happens when Pine Junction restocks SKU `10001234`.
# MAGIC The query and weights do not change. The cell checks the new winner and restores the original
# MAGIC stock.

# COMMAND ----------

try:
    execute("""
        UPDATE retail_store_product
        SET on_hand = 5
        WHERE store_id = 'store_pine'
          AND product_id = (
              SELECT product_id FROM retail_product_search WHERE sku = '10001234'
          );
    """)
    pine_restocked = retail_search("store_pine", REQUEST, core_only=True)
    show_results("Pine Junction after restocking 10001234", pine_restocked)
    assert pine_restocked[0]["sku"] == "10001234"
    assert pine_restocked[0]["vector_rank"] == 1
    assert pine_restocked[0]["bm25_rank"] == 1
finally:
    execute("""
        UPDATE retail_store_product
        SET on_hand = 0
        WHERE store_id = 'store_pine'
          AND product_id = (
              SELECT product_id FROM retail_product_search WHERE sku = '10001234'
          );
    """)

pine_restored = retail_search("store_pine", REQUEST, core_only=True)
assert pine_restored[0]["sku"] == "10001237"
print("✓ Restocking 10001234 changed the winner; fixture restored")
print("✓ Expected answers passed: 8/8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. What did we actually learn?
# MAGIC
# MAGIC This does not tell us which search system is better. It shows two clear routes in Postgres:
# MAGIC
# MAGIC - SKU and UPC use exact equality.
# MAGIC - Natural-language search uses product filters, vector and BM25 ranks, stock, and `CORE`
# MAGIC   assortment in one query.
# MAGIC
# MAGIC If that is useful, compare it with the current split path using representative queries and
# MAGIC expected answers. Add a latency target before choosing an approach. If it is not useful, stop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)
# MAGIC
# MAGIC The notebook drops and rebuilds its two tables on every run. To remove them now:
# MAGIC
# MAGIC ```python
# MAGIC execute("DROP TABLE IF EXISTS retail_store_product CASCADE;")
# MAGIC execute("DROP TABLE IF EXISTS retail_product_search CASCADE;")
# MAGIC ```
