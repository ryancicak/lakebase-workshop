# Databricks notebook source

# MAGIC %md
# MAGIC # Search products with store inventory
# MAGIC
# MAGIC A customer wants a driver for long deck screws. We'll find tools that fit, then check what's in stock at their store. We'll change the stock and see how the results change.
# MAGIC
# MAGIC We'll use two fictional stores and 24 made-up products. Each result includes the store's stock, price, and aisle.
# MAGIC
# MAGIC The customer asks: "I need a 20V impact driver for long deck screws. Tool only. Not EmberVale."
# MAGIC
# MAGIC We supply the request fields and small example vectors. This notebook doesn't interpret the sentence or call an AI model. We're looking at how search and inventory work together in SQL.
# MAGIC
# MAGIC | Step | What we'll do | Minutes |
# MAGIC |---|---|---|
# MAGIC | 0 | Connect to our own lab project | 5 |
# MAGIC | 1 | Load the products and store inventory | 5 |
# MAGIC | 2 | Look up a SKU and a barcode | 5 |
# MAGIC | 3 | Set the customer's requirements | 5 |
# MAGIC | 4 | Compare vector and keyword results | 12 |
# MAGIC | 5 | Recommend only the store's regular lineup | 10 |
# MAGIC | 6 | Try a request nothing can satisfy | 3 |
# MAGIC | 7 | Restock a product and search again | 8 |
# MAGIC | 8 | Check the results and discuss a real test | 7 |
# MAGIC
# MAGIC Run one cell at a time on **Serverless**. Step 4b is optional if we need five minutes back. Everything else stays in order.
# MAGIC
# MAGIC These examples show query behavior. They don't measure production search quality, speed, or cost.
# MAGIC
# MAGIC > Added by Ryan Cicak on 2026-10-13. Presentation and advance-setup checks revised 2026-11-06. Release 2026-11-06b.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Connect to our lab project
# MAGIC ### Choose the library versions
# MAGIC
# MAGIC **What:** we're preparing the library list for this notebook.
# MAGIC
# MAGIC **Why:** it needs a Lakebase client and a Postgres driver. We keep the environment's protobuf major version to avoid the restart problem caused by replacing it.
# MAGIC
# MAGIC **Look for:** `Installing:` followed by the library list. The next cell installs it.

# COMMAND ----------

# DBTITLE 1,Choose the library versions
# Pick library versions that fit this environment: keep protobuf on the major version it already has.
# On some serverless environment versions protobuf is a core package, and changing its major version
# stops the Python restart in the next cell ("Kernel exited").
import importlib.metadata as _md
_reqs = ["databricks-sdk>=0.81.0", "psycopg[binary]>=3.0"]
try:
    _pb = _md.version("protobuf")
    _reqs.append(f"protobuf>={_pb},<{int(_pb.split('.')[0]) + 1}")
except _md.PackageNotFoundError:
    pass
with open("/tmp/lakebase_lab_requirements.txt", "w") as _f:
    _f.write("\n".join(_reqs) + "\n")
print("Installing: " + ", ".join(_reqs))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install the libraries
# MAGIC
# MAGIC **What:** we're installing the libraries from the list we prepared.
# MAGIC
# MAGIC **Why:** they let Python create the lab project and query Postgres.
# MAGIC
# MAGIC **Look for:** the cell finishes successfully. Then run the restart cell below.

# COMMAND ----------

# DBTITLE 1,Install the libraries
# MAGIC %pip install --quiet -r /tmp/lakebase_lab_requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Python
# MAGIC
# MAGIC **What:** we're restarting Python so it loads the installed libraries.
# MAGIC
# MAGIC **Why:** the running session may still have older versions loaded.
# MAGIC
# MAGIC **Look for:** the restart completes. Continue with the next cell. If you restart later, rerun the setup and exercises in order.

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the shared setup
# MAGIC
# MAGIC **What:** we're loading the code that identifies your user and connects to Lakebase.
# MAGIC
# MAGIC **Why:** each person needs their own lab project and connection.
# MAGIC
# MAGIC **Look for:** `Lab helpers loaded` with your user name. The next cell opens the project.

# COMMAND ----------

# DBTITLE 1,Load the shared setup
# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create or reopen your Search project
# MAGIC
# MAGIC **What:** we're opening your own lab project, enabling Search, and checking the connection.
# MAGIC
# MAGIC **Why:** enabling Search restarts compute and can't be undone. That's why each person uses a dedicated lab project.
# MAGIC
# MAGIC **Look for:** `READY`. If you see `NOT READY`, stop here and follow the instructor while we resolve it.
# MAGIC
# MAGIC When the session setup notebook runs this lab ahead of time, this cell also checks that it requested the right release.

# COMMAND ----------

# DBTITLE 1,Create or reopen your Search project
LAB_RELEASE_VERSION = "2026-11-06b"


def _session_parameter(name, default=""):
    """Job parameters become widgets; an interactive run has no preparation parameters."""
    try:
        return str(dbutils.widgets.get(name)).strip()
    except Exception:
        return default


_prepare_value = _session_parameter("prepare_for_session", "false").lower()
if _prepare_value not in ("true", "false"):
    raise RuntimeError("prepare_for_session must be true or false. No lab project has been opened.")
PREPARE_FOR_SESSION = _prepare_value == "true"
SESSION_VALIDATION_RUN_ID = _session_parameter("validation_run_id")
SESSION_ENVIRONMENT = _session_parameter("session_environment")
SEARCH_ANSWER_CHECKS = 0
if PREPARE_FOR_SESSION:
    if _session_parameter("release_version") != LAB_RELEASE_VERSION:
        raise RuntimeError("This Search notebook does not match the requested session release. Install the matching release first.")
    if not SESSION_VALIDATION_RUN_ID:
        raise RuntimeError("Session preparation needs a validation_run_id. Start it from the session setup notebook.")
    if SESSION_ENVIRONMENT != "6":
        raise RuntimeError("This session release requires the preparation job to use environment version 6.")

conn = prepare_retail_search_lab()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the query and display helpers
# MAGIC
# MAGIC **What:** we're loading functions that run SQL, make the example vectors, and display results.
# MAGIC
# MAGIC **Why:** the later cells can focus on the search we're testing. The vectors use fixed word groups so everyone gets the same example results.
# MAGIC
# MAGIC **Look for:** `Lab helpers ready`. This cell defines functions; it doesn't load products or run a search.

# COMMAND ----------

# DBTITLE 1,Load the query and display helpers
import math
import re


def run_sql(sql, params=(), fetch=False, finish_transaction=True):
    """Run one statement, commit it, and return rows when fetch is True.

    The project scales to zero after an idle hour, which closes this
    connection. If that happened between statements, reconnect and retry once.
    Any other error rolls back and raises.
    """
    global conn
    for attempt in (1, 2):
        between_statements = conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if fetch else None
            if finish_transaction:
                conn.commit()
            return rows
        except Exception as exc:
            if attempt == 1 and between_statements and connection_was_closed(conn, exc):
                print("↻ The project scaled to zero while idle. Reconnecting...")
                conn = reconnect_retail_search_lab()
                print("✓ Reconnected")
                continue
            if not conn.closed:
                conn.rollback()
            raise


def execute(sql, params=()):
    """Execute a statement and commit it. Roll back and raise on any error."""
    run_sql(sql, params)


def fetch_rows(sql, params=(), finish_transaction=True):
    """Return query rows as dictionaries."""
    return run_sql(sql, params, fetch=True, finish_transaction=finish_transaction)


# Demo vectors for "search by meaning". A real app sends text to an embedding model and gets back
# hundreds of numbers. This stand-in uses 8, one per idea below, so results are the same every run.
CONCEPT_AXES = (
    {"impact", "driver", "drive", "driving", "fasten", "fastening", "screw", "screws", "bolt", "bolts", "tool", "torque"},  # driving screws
    {"drill", "drilling", "hole", "holes", "masonry"},  # drilling
    {"saw", "cut", "cutting", "blade", "jigsaw", "reciprocating"},  # cutting
    {"outdoor", "leaf", "blower", "trim", "trimmer", "hedge", "yard"},  # yard work
    {"vacuum", "wet", "dry", "dust", "cleanup", "clean", "sand", "sander", "sanding", "grind", "grinder"},  # cleanup
    {"deck", "decks", "decking", "board", "boards", "framing", "long", "structural", "lumber"},  # decks and framing
    {"battery", "charger", "light", "cordless", "power", "brushless"},  # cordless power
    {"socket", "wrench", "hand", "ratchet", "manual", "screwdriver"},  # hand tools
)


def demo_embedding(text):
    """Return a normalized VECTOR(8) literal from transparent concept counts."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    values = [0.05 + sum(token in axis for token in tokens) for axis in CONCEPT_AXES]
    norm = math.sqrt(sum(value * value for value in values))
    return "[" + ",".join(f"{value / norm:.6f}" for value in values) + "]"


# Readable output
def place(rank):
    """1 -> '1st', 2 -> '2nd', None -> '-'."""
    if rank is None:
        return "-"
    rank = int(rank)
    suffix = "th" if 10 <= rank % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(rank % 10, "th")
    return f"{rank}{suffix}"


def short_title(title):
    return title.replace(", Tool Only", "")


def show_ranking(store_id, rows, rule):
    """Print one store's ranked results with both search ranks side by side."""
    print(f"\n{STORE_NAMES[store_id]}  |  rule: {rule}")
    print(f"  {'Final':>5}  {'SKU':<8}  {'Brand':<11}  {'Vector rank':>11}  {'Keyword rank':>12}  {'Lineup':<8}  {'Stock':>5}")
    for row in rows:
        print(
            f"  {row['final_rank']:>5}  {row['sku']:<8}  {row['brand']:<11}  "
            f"{place(row['vector_rank']):>11}  {place(row['bm25_rank']):>12}  "
            f"{row['assortment_status']:<8}  {row['on_hand']:>5}"
        )
    if not rows:
        print("  (no products fit)")


def say_answer(store_id, rows):
    """Print the answer the associate would see: the top result, where it is, and what it costs."""
    if not rows:
        print("  Answer: none fit")
        return
    top = rows[0]
    print(
        f"  Answer: {short_title(top['title'])} (SKU {top['sku']}), "
        f"{top['on_hand']} on the shelf, aisle {top['aisle']}, ${float(top['price']):.2f}"
    )


print("✓ Lab helpers ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the products and store inventory
# MAGIC ### Prepare the example data
# MAGIC
# MAGIC **What:** we're putting 24 products and two stores' inventory into Python lists.
# MAGIC
# MAGIC **Why:** everyone needs the same starting data. The next cell writes it to Lakebase.
# MAGIC
# MAGIC ```text
# MAGIC Product catalog                   Store inventory
# MAGIC SKU, barcode, brand               Store and product
# MAGIC Type, voltage, kit                Regular or extended lineup
# MAGIC Description                       Stock, price, aisle
# MAGIC ```
# MAGIC
# MAGIC **Look for:** `24 products and 2 stores`. No database tables change in this cell.

# COMMAND ----------

# DBTITLE 1,Prepare the example data
# The made-up catalog. Products 1 to 5 are the impact drivers this lab is about.
# The other 19 are there so search has something to tell apart.
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

# The lineup for the five drivers is set on purpose. Everything else follows a simple pattern.
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

# Pine Junction is out of the NorthPeak 10001234. That's the trap.
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
river_keyword_forward = None
SEARCH_ANSWER_CHECKS = 0
print(f"✓ Loaded the made-up data: {len(PRODUCTS)} products and {len(STORE_NAMES)} stores (Pine Junction and River Bend)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the catalog and inventory tables
# MAGIC
# MAGIC **What:** we're replacing this lab's two tables, loading the examples, and creating the search indexes.
# MAGIC
# MAGIC **Why:** search needs the product catalog and each store's inventory in the database. Rebuilding gives us the same starting point on every run.
# MAGIC
# MAGIC **Look for:** 24 products and 48 inventory rows, one per product at each store. NorthPeak 10001234 is out of stock at Pine Junction.
# MAGIC
# MAGIC `CORE` means regular lineup. `EXTENDED` means carried outside that lineup. Both can have stock.

# COMMAND ----------

# DBTITLE 1,Build the catalog and inventory tables
try:
    execute("CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE;")
    execute("CREATE EXTENSION IF NOT EXISTS lakebase_text;")
except Exception as exc:
    print("✗ Lakebase Search extension installation failed after preparation.")
    raise RuntimeError("Lakebase Search extension installation failed") from exc

execute("DROP TABLE IF EXISTS retail_store_product CASCADE;")
execute("DROP TABLE IF EXISTS retail_product_search CASCADE;")

# The catalog: what each product is. The same at every store.
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

# The shelves: one row per product per store.
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

# Search indexes: one for meaning (vector), one for keywords (BM25). Built after the data is in,
# because BM25 needs word counts across the whole catalog.
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
print("✓ Built the catalog (24 products) and the shelves (48 rows: every product at both stores)")

drivers = fetch_rows("""
    SELECT p.sku, p.brand, p.title, s.store_id, s.assortment_status, s.on_hand
    FROM retail_product_search p
    JOIN retail_store_product s USING (product_id)
    WHERE p.sku IN ('10001234', '10001235', '10001236', '10001237', '10001238')
    ORDER BY p.sku, s.store_id;
""")
by_sku = {}
for row in drivers:
    entry = by_sku.setdefault(row["sku"], {"title": row["title"], "brand": row["brand"]})
    entry[row["store_id"]] = row

print("\nThe five 20V tool-only impact drivers in the catalog")
print(f"  {'SKU':<8}  {'Brand':<11}  {'Pine Junction':<20}  {'River Bend':<20}")
for sku, entry in by_sku.items():
    shelves = []
    notes = []
    for store_id, store_name in STORE_NAMES.items():
        shelf = entry[store_id]
        shelves.append(f"{shelf['assortment_status']}, {shelf['on_hand']} on shelf")
        if shelf["on_hand"] == 0:
            notes.append(f"out of stock at {store_name}")
    if entry["brand"] == "EmberVale":
        notes.append('the customer said "not EmberVale"')
    print(f"  {sku:<8}  {entry['brand']:<11}  {shelves[0]:<20}  {shelves[1]:<20}")

print("NorthPeak 10001234 is out of stock at Pine Junction. The customer excluded EmberVale.")

pine_context_by_sku = {sku: entry["store_pine"] for sku, entry in by_sku.items()}
assert pine_context_by_sku["10001234"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001234"]["on_hand"] == 0
assert pine_context_by_sku["10001237"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001237"]["on_hand"] > 0
assert pine_context_by_sku["10001238"]["assortment_status"] == "CORE"
assert pine_context_by_sku["10001238"]["on_hand"] > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Look up a SKU and a barcode
# MAGIC
# MAGIC **What:** we're looking up NorthPeak 10001234 at Pine Junction, first by SKU, then by barcode.
# MAGIC
# MAGIC **Why:** we already have the product's ID. An exact lookup gets the product and its store inventory without ranking alternatives.
# MAGIC
# MAGIC **Look for:** both IDs return the same product. Pine Junction has zero in stock. Finding a product doesn't mean we can sell it today.

# COMMAND ----------

# DBTITLE 1,2. Look up a SKU and a barcode
def lookup_identifier(identifier, store_id):
    """Exact lookup by SKU or barcode, with this store's shelf data. No search involved."""
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
upc_rows = lookup_identifier("850000100001", "store_pine")
assert exact_rows and exact_rows[0]["sku"] == "10001234"
assert upc_rows and upc_rows[0]["sku"] == "10001234"

hit = exact_rows[0]
print("Typed SKU 10001234 at Pine Junction:")
print(f"  {hit['title']} (barcode {hit['upc']})")
print(f"  Lineup: {hit['assortment_status']}  |  On the shelf: {hit['on_hand']}  |  Aisle {hit['aisle']}  |  ${float(hit['price']):.2f}")
print(f"Scanned barcode 850000100001 at Pine Junction: the same product, SKU {upc_rows[0]['sku']}")
print("\n→ One WHERE clause found it: sku = ... OR upc = ...  No search involved.")
print("→ It's out of stock at Pine Junction. That matters in step 5.")
print("✓ Exact lookup found SKU 10001234 by SKU and by barcode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Set the customer's requirements
# MAGIC
# MAGIC **What:** we're supplying the request fields and counting how many products meet them.
# MAGIC
# MAGIC ```text
# MAGIC Must match:  impact driver, 20V, tool only, not EmberVale
# MAGIC Rank for:    driving long screws into deck boards
# MAGIC ```
# MAGIC
# MAGIC **Why:** voltage and brand exclusion are requirements. The description helps rank the products that meet those requirements. An application would need to extract and validate these fields; we've filled them in here.
# MAGIC
# MAGIC **Look for:** 24 products become 4. The kit and the excluded brand are removed before we compare search results.

# COMMAND ----------

# DBTITLE 1,3. Set the customer's requirements
RAW_QUERY = "I need a 20V impact driver for long deck screws. Tool only. Not EmberVale."

REQUEST = {
    "search_text": "tool for driving long screws into deck boards",
    "product_type": "impact_driver",
    "voltage": 20,
    "kit_type": "TOOL_ONLY",
    "excluded_brands": ["EmberVale"],
}

print("The customer said:")
print(f'  "{RAW_QUERY}"\n')
print("Hard rules (a product has to match all of them):")
print(f"  type         {REQUEST['product_type']}")
print(f"  voltage      {REQUEST['voltage']}V")
print(f"  kit          {REQUEST['kit_type']}")
print(f"  not brand    {', '.join(REQUEST['excluded_brands'])}")
print("The job, in words (search ranks on this):")
print(f'  "{REQUEST["search_text"]}"')

funnel = fetch_rows("""
    SELECT
        count(*) AS catalog,
        count(*) FILTER (WHERE product_type = %s) AS by_type,
        count(*) FILTER (WHERE product_type = %s AND voltage = %s) AS by_voltage,
        count(*) FILTER (WHERE product_type = %s AND voltage = %s AND kit_type = %s) AS by_kit,
        count(*) FILTER (WHERE product_type = %s AND voltage = %s AND kit_type = %s
                           AND NOT (brand = ANY(%s::text[]))) AS by_brand
    FROM retail_product_search;
""", (
    REQUEST["product_type"],
    REQUEST["product_type"], REQUEST["voltage"],
    REQUEST["product_type"], REQUEST["voltage"], REQUEST["kit_type"],
    REQUEST["product_type"], REQUEST["voltage"], REQUEST["kit_type"], REQUEST["excluded_brands"],
))[0]
assert (funnel["catalog"], funnel["by_type"], funnel["by_voltage"], funnel["by_kit"], funnel["by_brand"]) == (24, 6, 6, 5, 4)

print("\nHow the hard rules shrink the catalog:")
print(f"  {funnel['catalog']:>2}  products in the catalog")
print(f"  {funnel['by_type']:>2}  are impact drivers")
print(f"  {funnel['by_voltage']:>2}  ...and 20V")
print(f"  {funnel['by_kit']:>2}  ...and tool only (the ForgeWorks kit with a battery is out)")
print(f"  {funnel['by_brand']:>2}  ...and not EmberVale")
print(f"\n→ Only these {funnel['by_brand']} can be the answer, at any store. Next, we rank them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compare vector and keyword results
# MAGIC ### Load the search query
# MAGIC
# MAGIC **What:** we're saving the SQL query and a function to call it. The next cell runs it.
# MAGIC
# MAGIC **Why:** we want to compare two rankings over the same eligible products. Vector search compares the example vectors. Keyword search (BM25) scores word matches. We combine their ranks to produce one list.
# MAGIC
# MAGIC ```text
# MAGIC Products that meet the customer and store rules
# MAGIC                 /                   \
# MAGIC         Vector ranking         Keyword ranking
# MAGIC                 \                   /
# MAGIC                  Combined ranking
# MAGIC ```
# MAGIC
# MAGIC **Look for:** `Search query loaded`. In the SQL, `WHERE` sets eligibility, the two ranking sections score products, and `scored` combines the ranks. We don't need to read every line aloud.
# MAGIC
# MAGIC The vector example uses fixed word groups, not a trained model. These rankings help us follow the query; real search quality needs a separate test.

# COMMAND ----------

# DBTITLE 1,Load the search query
STORE_AWARE_SEARCH_SQL = """
-- One query does the whole search. Read it top to bottom:
--   request         the inputs: which store, the question, the customer rules, the store rule, the blend
--   vector_scored   rank by meaning, over eligible products only
--   keyword_scored  rank by keyword, over the same eligible products
--   fused           the two rankings side by side
--   scored          blend the two places into one score
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
    WHERE s.store_id = r.store_id                               -- this store
      AND s.assortment_status IN ('CORE', 'EXTENDED')           -- the store carries it
      AND s.on_hand > 0                                         -- it is on the shelf
      AND (NOT r.core_only OR s.assortment_status = 'CORE')     -- step 5 turns this on: regular lineup only
      AND p.product_type = r.product_type                       -- the customer rules
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
    WHERE s.store_id = r.store_id                               -- the same rules as above
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
        -- the blend: a higher place in either list earns more points (reciprocal rank fusion)
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
    """Run the one query. The default blend leans a little toward meaning (1.15 vs 1.00); step 4b flips it."""
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


BASIC_RULE = "carries it, and it's on the shelf"
CORE_RULE = "regular lineup (CORE), and it's on the shelf"
print("✓ Search query loaded. The next cell runs it at both stores.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Search both stores
# MAGIC
# MAGIC **What:** we're running the same request at Pine Junction and River Bend. Products must be carried by that store and in stock.
# MAGIC
# MAGIC **Why:** this gives us a starting result before we add the regular-lineup rule in step 5.
# MAGIC
# MAGIC **Look for:** ForgeWorks is the top result at both stores. At River Bend, the vector and keyword rankings disagree. `Final` is their combined order.

# COMMAND ----------

# DBTITLE 1,4a. Search both stores
pine_base = retail_search("store_pine", REQUEST, core_only=False)
river_base = retail_search("store_river", REQUEST, core_only=False)

show_ranking("store_pine", pine_base, BASIC_RULE)
say_answer("store_pine", pine_base)
show_ranking("store_river", river_base, BASIC_RULE)
say_answer("store_river", river_base)

assert pine_base and river_base
assert pine_base[0]["sku"] == "10001235"
assert river_base[0]["sku"] == "10001235"

river_base_by_sku = {row["sku"]: row for row in river_base}
assert river_base_by_sku["10001235"]["vector_rank"] < river_base_by_sku["10001234"]["vector_rank"]
assert river_base_by_sku["10001234"]["bm25_rank"] < river_base_by_sku["10001235"]["bm25_rank"]
assert "10001234" not in {row["sku"] for row in pine_base}

for row in pine_base + river_base:
    assert row["product_type"] == "impact_driver"
    assert row["voltage"] == 20
    assert row["kit_type"] == "TOOL_ONLY"
    assert row["brand"] != "EmberVale"

print("\n→ Same answer at both stores: ForgeWorks 10001235.")
print("→ At River Bend the rankings disagree on 1st place: by vector it's ForgeWorks, by keyword it's NorthPeak 10001234.")
print("→ NorthPeak 10001234 isn't on Pine Junction's list at all. Zero on the shelf, so it never gets ranked.")
print("✓ Ranked two ways and blended, and every result follows the customer's hard rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Give keywords more weight (optional, 5 minutes)
# MAGIC
# MAGIC **What:** we're giving River Bend's keyword ranking a little more weight. The request, stock, and eligibility rules stay the same.
# MAGIC
# MAGIC **Why:** ForgeWorks ranks first by vector; NorthPeak ranks first by keyword. With equal weights they'd tie. Changing the weights lets us test which result to prefer.
# MAGIC
# MAGIC **Look for:** predict which product will come first, then run the cell. These are example weights. We'd choose production weights using real questions and expected results.
# MAGIC
# MAGIC You can skip this cell and continue to step 5. Later searches use the original weights.

# COMMAND ----------

# DBTITLE 1,4b. Give keywords more weight (optional, 5 minutes)
river_keyword_forward = retail_search(
    "store_river",
    REQUEST,
    core_only=False,
    vector_weight=1.00,
    bm25_weight=1.15,
)
show_ranking("store_river", river_keyword_forward, BASIC_RULE + " (blend leans toward keywords)")
say_answer("store_river", river_keyword_forward)
assert river_keyword_forward[0]["sku"] == "10001234"

print("\n→ Leaning toward keywords flipped River Bend's answer to NorthPeak 10001234. Nothing else changed.")
print("✓ Changing only the blend changed the answer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Recommend only the store's regular lineup
# MAGIC
# MAGIC **What:** for this exercise, the store asks us to recommend its regular lineup only. We turn on the `CORE` filter in both rankings.
# MAGIC
# MAGIC **Why:** a product can be in stock and still be outside that lineup. This is an example store policy we control, not a rule every retailer needs.
# MAGIC
# MAGIC | Product | Pine Junction | River Bend |
# MAGIC |---|---|---|
# MAGIC | NorthPeak 10001234 | Regular, but out of stock | Extended lineup |
# MAGIC | ForgeWorks 10001235 | Extended lineup | Regular, in stock |
# MAGIC | BlueOak 10001237 | Regular, in stock | Regular, in stock |
# MAGIC | NorthPeak 10001238 | Regular, in stock | Extended lineup |
# MAGIC
# MAGIC **Look for:** which products are still eligible at each store? Pine Junction drops ForgeWorks even though it has stock. Compare the results with step 4a; the ranking weights are the same.

# COMMAND ----------

# DBTITLE 1,5. Recommend only the store's regular lineup
pine_store_aware = retail_search("store_pine", REQUEST, core_only=True)
river_store_aware = retail_search("store_river", REQUEST, core_only=True)

show_ranking("store_pine", pine_store_aware, CORE_RULE)
say_answer("store_pine", pine_store_aware)
show_ranking("store_river", river_store_aware, CORE_RULE)
say_answer("store_river", river_store_aware)

assert pine_store_aware[0]["sku"] == "10001237"
assert pine_store_aware[0]["assortment_status"] == "CORE"
assert pine_store_aware[0]["on_hand"] > 0
assert len(pine_store_aware) == 2
assert river_store_aware[0]["sku"] == "10001235"
assert river_store_aware[0]["assortment_status"] == "CORE"
assert len(river_store_aware) == 2
assert pine_store_aware[0]["sku"] != river_store_aware[0]["sku"]

print()
for store_id, before, after in (("store_pine", pine_base, pine_store_aware), ("store_river", river_base, river_store_aware)):
    kept = {row["sku"] for row in after}
    dropped = [f"{row['brand']} {row['sku']}" for row in before if row["sku"] not in kept]
    print(f"→ {STORE_NAMES[store_id]} dropped {' and '.join(dropped)}: EXTENDED there, not regular lineup.")
first, second = pine_store_aware[0], pine_store_aware[1]
if first["vector_rank"] < second["vector_rank"] and first["bm25_rank"] > second["bm25_rank"]:
    print(f"→ At Pine Junction, {first['brand']} and {second['brand']} {second['sku']} would tie with equal weights "
          "(1st and 2nd in opposite rankings), so the higher vector weight puts BlueOak first.")
print("✓ Regular-lineup results: BlueOak at Pine Junction, ForgeWorks at River Bend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Try a request nothing can satisfy
# MAGIC
# MAGIC **What:** we're excluding all four brands that have a matching driver.
# MAGIC
# MAGIC **Why:** the query should respect the customer's requirements even when that leaves nothing to recommend. The app can ask whether they'd consider another brand.
# MAGIC
# MAGIC **Look for:** zero results. We haven't silently removed a requirement to fill the list.

# COMMAND ----------

# DBTITLE 1,6. Try a request nothing can satisfy
negative_request = {
    **REQUEST,
    "excluded_brands": ["NorthPeak", "ForgeWorks", "EmberVale", "BlueOak"],
}

negative_rows = retail_search("store_pine", negative_request, core_only=True)
assert negative_rows == []

print("The customer at Pine Junction says: not NorthPeak, not ForgeWorks, not EmberVale, not BlueOak.")
show_ranking("store_pine", negative_rows, CORE_RULE)
say_answer("store_pine", negative_rows)
print("\n→ Zero rows, on purpose. The app should say none fit, not drop one of the customer's rules to find something.")
print("✓ Nothing fit the customer's rules, and the query returned nothing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Restock a product and search again
# MAGIC
# MAGIC **What:** we're changing Pine Junction's NorthPeak 10001234 stock from 0 to 5, then repeating the search. The cell resets stock to 0 afterward.
# MAGIC
# MAGIC **Why:** the product was excluded because it was out of stock. We want to see what happens when it becomes available. The request, product descriptions, and search vectors stay the same.
# MAGIC
# MAGIC **Look for:** predict the new top result. After the reset, BlueOak should be first again.

# COMMAND ----------

# DBTITLE 1,7. Restock a product and search again
print("A delivery shows up at Pine Junction: NorthPeak 10001234 goes from 0 to 5 on the shelf.")
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
    show_ranking("store_pine", pine_restocked, CORE_RULE)
    say_answer("store_pine", pine_restocked)
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

print("✓ Put the shelf back to 0, so Pine Junction's answer is BlueOak 10001237 again")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the exercise results
# MAGIC
# MAGIC **What:** we're checking the results from the exercises we ran.
# MAGIC
# MAGIC **Why:** this confirms the examples matched our expectations. It doesn't grade production search quality.
# MAGIC
# MAGIC **Look for:** 8 checks if you ran step 4b, or 7 with that optional step skipped.

# COMMAND ----------

# DBTITLE 1,Check the exercise results
# Check the required exercises; the weight experiment is optional.
SEARCH_ANSWER_CHECKS = 0
assert exact_rows[0]["sku"] == "10001234"
assert pine_base[0]["sku"] == "10001235"
assert river_base[0]["sku"] == "10001235"
assert pine_store_aware[0]["sku"] == "10001237"
assert river_store_aware[0]["sku"] == "10001235"
assert negative_rows == []
assert pine_restocked[0]["sku"] == "10001234"
if river_keyword_forward is not None:
    assert river_keyword_forward[0]["sku"] == "10001234"
    SEARCH_ANSWER_CHECKS = 8
    print("✓ Lab check: all 8 answers came out the way this lab expects")
else:
    SEARCH_ANSWER_CHECKS = 7
    print("✓ Lab check: 7 answers checked; optional step 4b was skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Would this help your application?
# MAGIC
# MAGIC We looked up an exact product, searched for alternatives, applied store rules, and repeated the search after a restock.
# MAGIC
# MAGIC Where would this fit how you search today? Which store rules would you keep or change?
# MAGIC
# MAGIC If there's a useful case, the next step is a small test with 20 to 30 real questions and the expected results at each store. We'd need real embeddings and a way to extract the request fields. Today's examples don't establish search quality or performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Report the advance-setup result
# MAGIC
# MAGIC **What:** when the session setup notebook runs this lab, we're checking that all eight exercises passed and the inventory is back at its starting state.
# MAGIC
# MAGIC **Why:** the setup report needs to confirm that the full lab ran and left your project ready to use again.
# MAGIC
# MAGIC **Look for:** a preparation result during advance setup. In the room, this cell says the report is skipped and leaves your work in place.

# COMMAND ----------

# DBTITLE 1,Report the advance-setup result
if not PREPARE_FOR_SESSION:
    print("Advance-setup report skipped. Your lab project stays available.")
else:
    if SEARCH_ANSWER_CHECKS != 8 or river_keyword_forward is None:
        raise RuntimeError("Session preparation requires all eight exercise checks, including step 4b. Run the complete notebook.")
    prepared_state = fetch_rows("""
        SELECT
            (SELECT count(*) FROM retail_product_search) AS products,
            (SELECT count(*) FROM retail_store_product) AS store_rows,
            (SELECT s.on_hand
             FROM retail_store_product s
             JOIN retail_product_search p USING (product_id)
             WHERE s.store_id = 'store_pine' AND p.sku = '10001234') AS restored_stock;
    """)[0]
    if (prepared_state["products"], prepared_state["store_rows"], prepared_state["restored_stock"]) != (24, 48, 0):
        raise RuntimeError("Session preparation did not leave 24 products, 48 inventory rows, and the Pine Junction stock reset to zero.")
    import json

    dbutils.notebook.exit(json.dumps({
        "status": "PASSED",
        "lab": "search",
        "release_version": LAB_RELEASE_VERSION,
        "user": user_email,
        "project_id": RETAIL_SEARCH_PROJECT_ID,
        "schema": RETAIL_SEARCH_PG_SCHEMA,
        "validation_run_id": SESSION_VALIDATION_RUN_ID,
        "session_environment": "6",
        "prepared": True,
        "checks": ["exact", "hybrid", "store_rules", "no_result", "restock", "reset"],
    }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## When you're finished
# MAGIC
# MAGIC Rerunning the table-build cell resets this lab's two tables. The restock cell resets its own stock change.
# MAGIC
# MAGIC To remove the project, open Lakebase and check the project name printed during setup. Confirm it belongs to you and has the `retail-search-context` lab tag, then delete it when you no longer need your work. Closing the notebook doesn't delete the project.
