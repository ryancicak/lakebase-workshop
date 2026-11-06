# Databricks notebook source
# MAGIC %md
# MAGIC # Branch per change
# MAGIC
# MAGIC > Added by Ryan Cicak on 2026-11-05. Session preparation added for release 2026-11-06a.
# MAGIC
# MAGIC We want to find every store that carries one product. We'll test a SKU index on a separate database branch,
# MAGIC check that production hasn't changed, then add the same index to production.
# MAGIC
# MAGIC ```
# MAGIC production: store_inventory  ---------------------------> add the tested index
# MAGIC                 |                                               ^
# MAGIC                 +--> pr-101: run lookup -> add index -> check ---+
# MAGIC ```
# MAGIC
# MAGIC **How we'll run it:** choose Serverless, then run one cell at a time, from the top. We'll pause at the results.
# MAGIC Steps 1 to 9 cover the change. Step 10 is an optional recovery exercise if we have time.
# MAGIC
# MAGIC **Your lab only:** `production` is the name of a branch in your own `lakebase-lab-<you>` project.
# MAGIC All products and stores are made up. To repeat the exercise, start again at step 1, then run step 2 to reset it.
# MAGIC
# MAGIC **Docs:** [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches) |
# MAGIC [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) |
# MAGIC [Backup and restore](https://docs.databricks.com/aws/en/oltp/projects/backup-methods)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup A. Choose the libraries
# MAGIC
# MAGIC **What:** we're building the list of Python libraries this notebook needs.
# MAGIC
# MAGIC **Why:** We need to manage Lakebase branches and run SQL, using libraries compatible with this notebook environment.
# MAGIC
# MAGIC **Look for:** A library list. No project or table is created yet.

# COMMAND ----------

# DBTITLE 1,Setup A. Choose the libraries
# Prepare the library list while keeping protobuf on this environment's current major version.
import importlib.metadata as _md
_reqs = ["databricks-sdk>=0.81.0", "psycopg[binary]>=3.0"]
try:
    _pb = _md.version("protobuf")
    _reqs.append(f"protobuf>={_pb},<{int(_pb.split('.')[0]) + 1}")
except _md.PackageNotFoundError:
    pass
with open("/tmp/lakebase_lab_requirements.txt", "w") as _f:
    _f.write("\n".join(_reqs) + "\n")
print("Libraries selected: " + ", ".join(_reqs))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup B. Install the libraries
# MAGIC
# MAGIC **What:** we're installing the libraries selected above.
# MAGIC
# MAGIC **Why:** The following cells use them to connect to Lakebase.
# MAGIC
# MAGIC **Look for:** The cell finishes without an error. Then run the restart cell below.

# COMMAND ----------

# DBTITLE 1,Setup B. Install the libraries
# MAGIC %pip install --quiet -r /tmp/lakebase_lab_requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup C. Restart Python
# MAGIC
# MAGIC **What:** we're restarting this notebook's Python session.
# MAGIC
# MAGIC **Why:** Python needs to load the libraries we installed.
# MAGIC
# MAGIC **Look for:** The restart finishes. Run both helper cells below next, since a restart clears Python variables.

# COMMAND ----------

# DBTITLE 1,Setup C. Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup D. Load the shared connection helpers
# MAGIC
# MAGIC **What:** we're loading the shared code that identifies your workspace user and connects to Lakebase.
# MAGIC
# MAGIC **Why:** Each participant needs their own project name and connections.
# MAGIC
# MAGIC **Look for:** A confirmation that the lab helpers loaded for your user.

# COMMAND ----------

# DBTITLE 1,Setup D. Load the shared connection helpers
# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup E. Load this lab's helpers
# MAGIC
# MAGIC **What:** we're defining the functions for SQL, branches, and the sample inventory table.
# MAGIC
# MAGIC **Why:** The exercise cells can then show the change we're testing without repeating connection code.
# MAGIC
# MAGIC **Look for:** Your project name and the test branch name, `pr-101`. No data changes yet.
# MAGIC Run this cell after Setup D, including after any Python restart.

# COMMAND ----------

# DBTITLE 1,Setup E. Load this lab's helpers
import json
import re
import time
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp
from psycopg.rows import dict_row
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.postgres import (Branch, BranchSpec, Duration, Endpoint, EndpointSpec, FieldMask,
                                             EndpointType, Project, ProjectDefaultEndpointSettings, ProjectSpec)


def _param(name, default):
    """Notebook options. Interactive runs use defaults; a job can pass base parameters."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return type(default)(value) if str(value).strip() else default


RELEASE_VERSION = "2026-11-06a"
SESSION_ENVIRONMENT = "6"
_prepare_requested = _param("prepare_for_session", "false").strip().lower()
if _prepare_requested not in ("true", "false"):
    raise RuntimeError("prepare_for_session must be true or false. Run the session preparation notebook again.")
PREPARE_FOR_SESSION = _prepare_requested == "true"
VALIDATION_RUN_ID = _param("validation_run_id", "").strip()
PREPARATION_REQUEST_VALIDATED = False
if PREPARE_FOR_SESSION:
    if _param("release_version", "").strip() != RELEASE_VERSION:
        raise RuntimeError("The requested session release does not match this lab. Install the matching release first.")
    if _param("session_environment", "").strip() != SESSION_ENVIRONMENT:
        raise RuntimeError("Session preparation must use serverless environment version 6.")
    if not re.fullmatch(r"[0-9a-f]{8,64}", VALIDATION_RUN_ID):
        raise RuntimeError("The preparation run ID is missing or invalid. Start a new run from the preparation notebook.")
    PREPARATION_REQUEST_VALIDATED = True

PAUSE_SECONDS = _param("pause_seconds", 0)                    # rehearsal only: sleep between steps so the compute suspends
IDLE_TIMEOUT_SECONDS = _param("idle_timeout_seconds", 3600)   # one hour, so a pause in the room does not suspend the compute
_suffix = _param("project_suffix", "")                          # rehearsal only: run against a separate project
if _suffix:
    PROJECT_ID = f"{PROJECT_ID}-{_suffix}"

TABLE = "store_inventory"
INDEX = "store_inventory_sku_idx"
BRANCH = "pr-101"                 # the branch your pull request would get
STORES, SKUS = 2000, 1000         # two million rows
LOOKUP_SKU = "SKU-000777"
LOOKUP_SQL = f"SELECT store_id, on_hand, aisle FROM {TABLE} WHERE sku = %s"
MIGRATION_SQL = f"CREATE INDEX IF NOT EXISTS {INDEX} ON {TABLE} (sku)"   # the one statement that moves forward
TIMES = {}
RECOVERY_STAGE = "not_started"
PRODUCTION_ISOLATED = False
SCHEMA_CHECKED = False
LAB_COMPLETION_VERIFIED = False


def stamp(key, started):
    TIMES[key] = round(time.monotonic() - started, 1)
    return TIMES[key]


def _seconds(value):
    if value is None:
        return None
    if hasattr(value, "seconds"):
        return int(value.seconds or 0)
    if isinstance(value, dict):
        return int(value.get("seconds") or 0)
    try:
        return int(float(str(value).strip().rstrip("s")))
    except ValueError:
        return None


def pause(label):
    if PAUSE_SECONDS:
        print(f"(rehearsal) {label}: pausing {PAUSE_SECONDS} s so the compute can suspend ...")
        time.sleep(PAUSE_SECONDS)


_endpoints = {}


def endpoint_for(branch, refresh=False):
    if refresh or branch not in _endpoints:
        eps = list(w.postgres.list_endpoints(parent=f"projects/{PROJECT_ID}/branches/{branch}"))
        if not eps:
            raise RuntimeError(f"branch {branch} has no compute endpoint yet")
        _endpoints[branch] = w.postgres.get_endpoint(name=eps[0].name)
    return _endpoints[branch]


def q(branch, sql, params=None, fetch=True, attempts=8):
    """Run one statement on a branch over a fresh OAuth connection. A suspended compute wakes on connect."""
    last = None
    for attempt in range(attempts):
        try:
            ep = endpoint_for(branch, refresh=attempt > 0)
            cred = w.postgres.generate_database_credential(endpoint=ep.name)
            with psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=user_email,
                                 password=cred.token, sslmode="require", connect_timeout=20, autocommit=True,
                                 options=f"-c search_path={PG_SCHEMA},public", row_factory=dict_row) as conn:
                if attempt:
                    print(f"   (woke the {branch} compute after {attempt} retr{'y' if attempt == 1 else 'ies'})")
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.fetchall() if fetch and cur.description else None
        except psycopg.OperationalError as exc:
            last = exc
            time.sleep(5 if attempt < 3 else 10)
    raise last


def explain(branch, sql, params):
    rows = q(branch, "EXPLAIN (ANALYZE, BUFFERS) " + sql, params)
    lines = [list(r.values())[0] for r in rows]
    node = next((l.strip() for l in lines if "Scan" in l), lines[0].strip()).split("(cost")[0].lstrip("-> ").strip()
    ms = next((float(l.split(":")[1].split("ms")[0]) for l in lines if l.strip().startswith("Execution Time")), 0.0)
    return node, ms, lines


def ensure_idle_timeout(branch, seconds):
    """Keep the compute up through a pause in the room. Non-fatal: every cell reconnects on its own anyway."""
    try:
        ep = endpoint_for(branch, refresh=True)
        current = _seconds(getattr(ep.status, "suspend_timeout_duration", None)
                           or getattr(ep.spec, "suspend_timeout_duration", None))
        if current != seconds:
            w.postgres.update_endpoint(
                name=ep.name,
                endpoint=Endpoint(name=ep.name, spec=EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                                                                  suspend_timeout_duration=Duration(seconds=seconds))),
                update_mask=FieldMask(field_mask=["spec.suspend_timeout_duration"]),
            ).wait()
            ep = endpoint_for(branch, refresh=True)
            current = _seconds(getattr(ep.status, "suspend_timeout_duration", None)
                               or getattr(ep.spec, "suspend_timeout_duration", None))
        return current
    except Exception as exc:
        print(f"⚠ Could not set the idle timeout on {branch} ({type(exc).__name__}: {str(exc)[:120]}). "
              "If the compute suspends during a pause, the next cell wakes it.")
        return None


def build_table(branch="production"):
    """Create and fill the production-shaped table if it is missing or short. Returns (rows, seconds, built?)."""
    started = time.monotonic()
    q(branch, f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}", fetch=False)
    q(branch, f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            store_id    integer     NOT NULL,
            sku         text        NOT NULL,
            on_hand     integer     NOT NULL,
            price_cents integer     NOT NULL,
            aisle       text        NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )""", fetch=False)
    count = q(branch, f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
    if count >= STORES * SKUS:
        return count, 0.0, False
    q(branch, f"TRUNCATE {TABLE}", fetch=False)
    q(branch, f"""
        INSERT INTO {TABLE} (store_id, sku, on_hand, price_cents, aisle)
        SELECT s, 'SKU-' || lpad(k::text, 6, '0'), (random() * 50)::int, 199 + (random() * 20000)::int, 'A' || (1 + k % 40)
        FROM generate_series(1, {STORES}) AS s CROSS JOIN generate_series(1, {SKUS}) AS k""", fetch=False)
    has_pk = q(branch, "SELECT 1 FROM pg_indexes WHERE schemaname = %s AND tablename = %s AND indexname LIKE %s",
               (PG_SCHEMA, TABLE, "%_pkey"))
    if not has_pk:
        q(branch, f"ALTER TABLE {TABLE} ADD PRIMARY KEY (store_id, sku)", fetch=False)
    q(branch, f"ANALYZE {TABLE}", fetch=False)
    count = q(branch, f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
    return count, round(time.monotonic() - started, 1), True


def branch_ids():
    return sorted(b.name.rsplit("/", 1)[-1] for b in w.postgres.list_branches(parent=f"projects/{PROJECT_ID}"))


def delete_branch_if_exists(branch_id):
    if branch_id in branch_ids():
        w.postgres.delete_branch(name=f"projects/{PROJECT_ID}/branches/{branch_id}").wait()
        _endpoints.pop(branch_id, None)
        return True
    return False


def create_branch(branch_id, source="production", ttl_seconds=86400, wait=True, no_expiry=False, at=None):
    """Create a copy-on-write branch. Work branches get a TTL so they clean themselves up. A branch that will
    have children must have no expiry. `at` (a timezone-aware datetime) makes it a point-in-time branch: the
    source branch exactly as it was at that moment. Returns (seconds to create, seconds to ready)."""
    started = time.monotonic()
    source_name = f"projects/{PROJECT_ID}/branches/{source}"
    fields = {"source_branch": source_name}
    if at is not None:
        ts = Timestamp()
        ts.FromDatetime(at)
        fields["source_branch_time"] = ts
    spec = BranchSpec(no_expiry=True, **fields) if no_expiry else BranchSpec(ttl=Duration(seconds=ttl_seconds), **fields)
    w.postgres.create_branch(
        parent=f"projects/{PROJECT_ID}",
        branch=Branch(spec=spec),
        branch_id=branch_id,
    ).wait()
    created = round(time.monotonic() - started, 1)
    if not wait:
        return created, None
    wait_for_endpoint(branch_id)
    ready = round(time.monotonic() - started, 1)
    ensure_idle_timeout(branch_id, IDLE_TIMEOUT_SECONDS)
    return created, ready


print(f"Project: {PROJECT_ID} | schema: {PG_SCHEMA} | branch for this change: {BRANCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create your lab project
# MAGIC
# MAGIC **What:** we're creating your own Lakebase project, or reopening it from an earlier run.
# MAGIC
# MAGIC **Why:** You can change this lab's `production` branch without affecting anyone else's work.
# MAGIC
# MAGIC **Look for:** `Production compute ready` and a link to your project.

# COMMAND ----------

# DBTITLE 1,1. Create your lab project
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
try:
    existing_project = w.postgres.get_project(name=f"projects/{PROJECT_ID}")
    existing_owner = getattr(getattr(existing_project, "status", None), "owner", None)
    if existing_owner is None:
        existing_owner = getattr(getattr(existing_project, "spec", None), "owner", None)
    if not isinstance(existing_owner, str) or existing_owner.strip().casefold() != user_email.strip().casefold():
        raise RuntimeError(
            f"NOT READY: refusing to change {PROJECT_ID} because its owner could not be verified as your identity."
        )
    print(f"✓ Project: reused {PROJECT_ID}")
    TIMES["project"] = "reused"
except PermissionDenied as exc:
    raise RuntimeError(f"NOT READY: {user_email} cannot inspect project {PROJECT_ID}: {exc}") from exc
except NotFound:
    print(f"Creating project {PROJECT_ID} ...")
    try:
        w.postgres.create_project(
            project=Project(spec=ProjectSpec(
                display_name=f"Lakebase lab: {PROJECT_ID}",
                pg_version=17,
                default_endpoint_settings=ProjectDefaultEndpointSettings(
                    autoscaling_limit_min_cu=1,
                    autoscaling_limit_max_cu=1,
                    suspend_timeout_duration=Duration(seconds=IDLE_TIMEOUT_SECONDS),
                ),
            )),
            project_id=PROJECT_ID,
        ).wait()
    except PermissionDenied as exc:
        raise RuntimeError(f"NOT READY: {user_email} cannot create a Lakebase project in this workspace. "
                           "Ask a workspace admin.") from exc
    stamp('project', started)
    print(f"✓ Project: created {PROJECT_ID}")

wait_for_endpoint("production")
idle = ensure_idle_timeout("production", IDLE_TIMEOUT_SECONDS)
print(f"✓ Production compute ready; idle timeout {idle} s")
show_view_link(f"Open {PROJECT_ID} in Lakebase", lakebase_project_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load the store inventory
# MAGIC
# MAGIC **What:** we're creating two million made-up inventory rows: 2,000 stores with 1,000 products each.
# MAGIC
# MAGIC **Why:** The primary key is `(store_id, sku)`. We'll test a different lookup: one SKU across every store.
# MAGIC
# MAGIC **Look for:** `2,000,000 rows` and `no SKU index on production`.
# MAGIC This also resets earlier runs by removing the added index, the extra SKU, and this exercise's temporary branches.

# COMMAND ----------

# DBTITLE 1,2. Load the store inventory
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
count, seconds, built = build_table("production")

# Reset the story so every run starts the same way: no SKU index on production, no leftover branches, and none of the
# rows step 10 adds (its pretend new SKU, its repair staging table).
q("production", f"DROP INDEX IF EXISTS {INDEX}", fetch=False)
q("production", f"DROP TABLE IF EXISTS {TABLE}_repair", fetch=False)
q("production", f"DELETE FROM {TABLE} WHERE sku = 'SKU-001001'", fetch=False)
removed = [b for b in (BRANCH, "qa-stable", "qa", "oops", "recovered", "checkpoint-before-change") if delete_branch_if_exists(b)]
count = q("production", f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
RECOVERY_STAGE = "not_started"
PRODUCTION_ISOLATED = False
SCHEMA_CHECKED = False
LAB_COMPLETION_VERIFIED = False
print(f"✓ Baseline: {TABLE} has {count:,} rows" + (" (loaded)" if built else " (already there)"))
print("✓ Reset: no SKU index on production" + (f"; removed leftover branches {removed}" if removed else ""))
show_view_link(f"See {TABLE} on production", lakebase_tables_url("production"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create a branch for the index change
# MAGIC
# MAGIC **What:** we're creating `pr-101` from this lab's production branch.
# MAGIC
# MAGIC **Why:** We can test the index against the same data while keeping production separate.
# MAGIC
# MAGIC **Look for:** `pr-101 sees 2,000,000 rows`. Open its link to see the new branch.
# MAGIC The branches share unchanged data. This branch expires after 24 hours if we leave it behind.

# COMMAND ----------

# DBTITLE 1,3. Create the test branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
created, ready = create_branch(BRANCH)
TIMES["branch_create"], TIMES["branch_ready"] = created, ready
print(f"✓ Branch {BRANCH}: created from production; its compute is ready")
n = q(BRANCH, f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
print(f"✓ {BRANCH} sees {n:,} rows")
show_view_link(f"Open branch {BRANCH} in Lakebase", lakebase_project_url(branch=BRANCH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Find one product across all stores
# MAGIC
# MAGIC **What:** we're looking up `SKU-000777` on the test branch to see how Postgres reads the table.
# MAGIC
# MAGIC **Why:** We need to see the current query plan before deciding whether an index helps.
# MAGIC
# MAGIC **Look for:** `Seq Scan`, meaning Postgres scans the table. The query should return 2,000 stores.
# MAGIC The SQL is `SELECT store_id, on_hand, aisle FROM store_inventory WHERE sku = 'SKU-000777'`.

# COMMAND ----------

# DBTITLE 1,4. Find one product across all stores
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
node, ms, plan = explain(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
rows = q(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
if PREPARE_FOR_SESSION and "seq scan" not in node.casefold():
    raise RuntimeError(f"Preparation expected a sequential scan before the index; observed {node}. Review this plan.")
TIMES["before_ms"] = ms
print(f"Lookup on {BRANCH} before the change: {node}")
print(f"Result: {len(rows):,} stores carry {LOOKUP_SKU}. Seq Scan means the table was scanned to find them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add the index on the test branch
# MAGIC
# MAGIC **What:** we're creating an index on `sku`, then running the same lookup on `pr-101`.
# MAGIC
# MAGIC **Why:** The index gives Postgres a way to find that SKU without scanning the whole table.
# MAGIC
# MAGIC **Look for:** An `Index` or `Bitmap` scan in place of `Seq Scan`. We haven't changed production yet.
# MAGIC The change is `CREATE INDEX store_inventory_sku_idx ON store_inventory (sku)`.

# COMMAND ----------

# DBTITLE 1,5. Add the index on the test branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
q(BRANCH, MIGRATION_SQL, fetch=False)
q(BRANCH, f"ANALYZE {TABLE}", fetch=False)
stamp('index_branch', started)
print(f"✓ Index built on {BRANCH}: {MIGRATION_SQL}")
node, ms, plan = explain(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
if PREPARE_FOR_SESSION and not any(kind in node.casefold() for kind in ("index", "bitmap")):
    raise RuntimeError(f"Preparation expected an index or bitmap scan on the test branch; observed {node}.")
TIMES["after_ms"] = ms
print(f"Lookup on {BRANCH} after the change: {node}")
print("The lookup is unchanged. Postgres now has the SKU index available.")
pause("after the change on the branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check production before changing it
# MAGIC
# MAGIC **What:** we're comparing each branch's indexes and running the lookup on production.
# MAGIC
# MAGIC **Why:** We need to confirm that testing the index didn't add it to production.
# MAGIC
# MAGIC **Look for:** `store_inventory_sku_idx` on `pr-101` only. Production should still use `Seq Scan`.

# COMMAND ----------

# DBTITLE 1,6. Check production before changing it
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
IDX_SQL = "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s ORDER BY indexname"
prod_idx = [r["indexname"] for r in q("production", IDX_SQL, (PG_SCHEMA, TABLE))]
branch_idx = [r["indexname"] for r in q(BRANCH, IDX_SQL, (PG_SCHEMA, TABLE))]
node, ms, _ = explain("production", LOOKUP_SQL, (LOOKUP_SKU,))
print(f"{BRANCH} indexes:    {branch_idx}")
print(f"production indexes: {prod_idx}")
assert INDEX in branch_idx and INDEX not in prod_idx, "expected the index on the branch only"
PRODUCTION_ISOLATED = True
print(f"✓ Production untouched: the lookup there is still {node}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add the tested index to production
# MAGIC
# MAGIC **What:** we're running the same `CREATE INDEX` statement on production, then checking its lookup.
# MAGIC
# MAGIC **Why:** We've tested the change on the branch. Now we can apply that change to production.
# MAGIC
# MAGIC **Look for:** Production uses an `Index` or `Bitmap` scan too.
# MAGIC We're doing this manually. A deployment pipeline would run the reviewed migration after approval.

# COMMAND ----------

# DBTITLE 1,7. Add the tested index to production
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
q("production", MIGRATION_SQL, fetch=False)
q("production", f"ANALYZE {TABLE}", fetch=False)
stamp('index_prod', started)
print("✓ Promoted: the same CREATE INDEX statement ran on production")
node, ms, _ = explain("production", LOOKUP_SQL, (LOOKUP_SKU,))
if PREPARE_FOR_SESSION and not any(kind in node.casefold() for kind in ("index", "bitmap")):
    raise RuntimeError(f"Preparation expected an index or bitmap scan after promotion; observed {node}.")
TIMES["prod_after_ms"] = ms
print(f"Lookup on production after promote: {node}")
pause("after promote")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compare the table definitions
# MAGIC
# MAGIC **What:** we're comparing column names, types, and index names on both branches.
# MAGIC
# MAGIC **Why:** We want to confirm that production has the table structure we tested.
# MAGIC
# MAGIC **Look for:** `Schema diff: no differences`. This checks structure, not every row's contents.

# COMMAND ----------

# DBTITLE 1,8. Compare the table definitions
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
def schema_of(branch):
    cols = q(branch, "SELECT column_name, data_type FROM information_schema.columns "
                     "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (PG_SCHEMA, TABLE))
    idx = q(branch, IDX_SQL, (PG_SCHEMA, TABLE))
    return {"columns": [(c["column_name"], c["data_type"]) for c in cols], "indexes": [i["indexname"] for i in idx]}


on_branch, on_prod = schema_of(BRANCH), schema_of("production")
print(f"{BRANCH}:    {len(on_branch['columns'])} columns, indexes {on_branch['indexes']}")
print(f"production: {len(on_prod['columns'])} columns, indexes {on_prod['indexes']}")
if on_branch == on_prod:
    print("✓ Schema diff: no differences")
    SCHEMA_CHECKED = True
else:
    raise RuntimeError("The table definitions differ. Compare the columns and indexes before continuing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Delete the test branch
# MAGIC
# MAGIC **What:** we're deleting `pr-101` now that production has the index.
# MAGIC
# MAGIC **Why:** The test branch has finished its job. We can remove its compute and branch resources.
# MAGIC
# MAGIC **Look for:** The remaining branch list contains `production`.

# COMMAND ----------

# DBTITLE 1,9. Delete the branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
delete_branch_if_exists(BRANCH)
print(f"✓ Branch {BRANCH} deleted; branches now: {branch_ids()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where this fits in your pull request process
# MAGIC We ran these steps by hand. Your CI/CD pipeline would call the same operations around a reviewed migration file.
# MAGIC
# MAGIC | In the pull request | In Lakebase |
# MAGIC |---|---|
# MAGIC | Open the PR | Create a branch from the stable branch, with a TTL |
# MAGIC | CI runs the migration and the tests | Run the migration file against the branch; query it |
# MAGIC | Review | Schema diff between the branch and the stable branch |
# MAGIC | Merge | Replay the same migration file against the stable branch, then production |
# MAGIC | Close the PR | Delete the branch |
# MAGIC
# MAGIC Promotion here means running the reviewed SQL against production. Lakebase does not merge the branch's data for us.
# MAGIC This exercise uses a native Postgres table. Adding an index is also supported on synced tables; their data refresh
# MAGIC remains a separate process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional: recover deleted rows without losing newer work
# MAGIC An agent is asked to remove zero-stock rows for one store. It leaves out the store filter and deletes those rows
# MAGIC from every store. Meanwhile, stores keep updating inventory. We'll recover the missing rows and keep those updates.
# MAGIC
# MAGIC ```
# MAGIC production:  record time -> bad DELETE -> stores update inventory -> repair missing rows
# MAGIC                   |                                                        ^
# MAGIC                   +--> recovered: production at the recorded time ---------+
# MAGIC ```
# MAGIC
# MAGIC Run 10a through 10e in order. If we're short on time, skip the entire section and go to Wrap-up.
# MAGIC
# MAGIC ### 10a. Record the healthy state
# MAGIC
# MAGIC **What:** we're recording production's row count and the database time before the mistake.
# MAGIC
# MAGIC **Why:** We'll use that time to create a recovery branch containing the rows we're about to delete.
# MAGIC
# MAGIC **Look for:** `2,000,000 rows` and the time in UTC. Nothing has been deleted yet.

# COMMAND ----------

# DBTITLE 1,10a. Record the healthy state
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
if RECOVERY_STAGE != "not_started":
    raise RuntimeError("Recovery has already started. Continue with the next recovery cell, or restart at step 2.")
RECOVERED = "recovered"
AGENT_SQL = f"DELETE FROM {TABLE} WHERE on_hand = 0"   # the store filter the agent was asked for is missing
REPAIR_TABLE = f"{TABLE}_repair"
BUSY_STORE = 42
NEW_SKU = "SKU-001001"   # one more than the 1,000 the table was built with


def connect(branch):
    """A plain connection to one branch, for the copy between branches."""
    ep = endpoint_for(branch, refresh=True)
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    return psycopg.connect(host=ep.status.hosts.host, dbname="databricks_postgres", user=user_email, password=cred.token,
                           sslmode="require", connect_timeout=20, autocommit=True, options=f"-c search_path={PG_SCHEMA},public")


def health(branch):
    """(rows in the table, stores that carry the lookup SKU) on one branch."""
    rows = q(branch, f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
    stores = len(q(branch, LOOKUP_SQL, (LOOKUP_SKU,)))
    return rows, stores


# 1. Healthy. Note the time from the database clock, and pick one zero-stock row to watch.
rows0, stores0 = health("production")
t0 = q("production", "SELECT now() AS t")[0]["t"]
watched = q("production", f"SELECT store_id, sku FROM {TABLE} WHERE on_hand = 0 ORDER BY store_id, sku LIMIT 1")[0]
print(f"✓ Production at {t0:%H:%M:%S} UTC: {rows0:,} rows; {stores0:,} stores carry {LOOKUP_SKU}")
time.sleep(5)  # a few seconds of history between healthy and damaged, so the restore point is unambiguous
RECOVERY_STAGE = "recorded"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10b. Run the agent's bad DELETE
# MAGIC
# MAGIC **What:** we're deleting every zero-stock row from this lab's production branch.
# MAGIC
# MAGIC **Why:** The missing store filter turns a one-store cleanup into a mistake across all stores.
# MAGIC
# MAGIC **Look for:** The row count drops. The table still answers queries, but the deleted store-and-SKU records are missing.
# MAGIC The mistake is `DELETE FROM store_inventory WHERE on_hand = 0`, with no `store_id` condition.

# COMMAND ----------

# DBTITLE 1,10b. Run the bad DELETE in your lab project
if globals().get("RECOVERY_STAGE") != "recorded":
    raise RuntimeError("Run 10a first, then 10b through 10e in order. To repeat recovery, restart at step 2.")

# 2. The agent's statement, against production.
q("production", AGENT_SQL, fetch=False)
rows1, stores1 = health("production")
deleted = rows0 - rows1
print(f"✗ After the agent's DELETE: {rows1:,} rows ({deleted:,} gone); {stores1:,} stores carry {LOOKUP_SKU}. "
      "The query succeeds, but inventory rows are missing.")
RECOVERY_STAGE = "deleted"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10c. Keep the stores working
# MAGIC
# MAGIC **What:** we're restocking three products, adding a new SKU, and re-listing one deleted item with five on hand.
# MAGIC
# MAGIC **Why:** A repair must keep this newer work. Replacing production with the earlier state would lose it.
# MAGIC
# MAGIC **Look for:** Three kinds of updates. The re-listed item is the conflict we'll check after the repair.

# COMMAND ----------

# DBTITLE 1,10c. Add the stores' newer inventory changes
if globals().get("RECOVERY_STAGE") != "deleted":
    raise RuntimeError("Run 10b first. To repeat recovery, restart at step 2.")

# 3. The day goes on. Stores keep working against production, and none of this may be lost in the repair.
q("production", f"UPDATE {TABLE} SET on_hand = on_hand + 12, updated_at = now() WHERE store_id = %s AND sku IN "
                f"(SELECT sku FROM {TABLE} WHERE store_id = %s AND on_hand > 0 ORDER BY sku LIMIT 3)",
  (BUSY_STORE, BUSY_STORE), fetch=False)
q("production", f"INSERT INTO {TABLE} (store_id, sku, on_hand, price_cents, aisle) VALUES (%s, %s, 25, 1999, 'A7') "
                "ON CONFLICT (store_id, sku) DO UPDATE SET on_hand = EXCLUDED.on_hand, updated_at = now()",
  (BUSY_STORE, NEW_SKU), fetch=False)
q("production", f"INSERT INTO {TABLE} (store_id, sku, on_hand, price_cents, aisle) VALUES (%s, %s, 5, 1299, 'A3') "
                "ON CONFLICT (store_id, sku) DO UPDATE SET on_hand = EXCLUDED.on_hand, updated_at = now()",
  (watched["store_id"], watched["sku"]), fetch=False)
print(f"✓ The day goes on: store {BUSY_STORE} restocked 3 SKUs, rolled out {NEW_SKU}, and store {watched['store_id']} "
      f"re-listed {watched['sku']} with 5 on hand (one of the deleted rows, updated after the restore point)")
RECOVERY_STAGE = "newer_work"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10d. Restore the earlier state to a separate branch
# MAGIC
# MAGIC **What:** we're creating `recovered` from production at the time we recorded in 10a.
# MAGIC
# MAGIC **Why:** It gives us the deleted rows to inspect while production keeps its newer updates.
# MAGIC
# MAGIC **Look for:** `recovered` has the original two million rows. Production has not been replaced.
# MAGIC This uses point-in-time restore within the project's available history window.

# COMMAND ----------

# DBTITLE 1,10d. Create the point-in-time recovery branch
if globals().get("RECOVERY_STAGE") != "newer_work":
    raise RuntimeError("Run 10c first. To repeat recovery, restart at step 2.")

# 4. Point-in-time restore: production exactly as it was at t0, as a new branch.
r_created, r_ready = create_branch(RECOVERED, source="production", at=t0)
rows_r, stores_r = health(RECOVERED)
print(f"✓ {RECOVERED}: production as of {t0:%H:%M:%S} UTC: "
      f"{rows_r:,} rows; {stores_r:,} stores carry {LOOKUP_SKU}")
RECOVERY_STAGE = "restored"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10e. Put back the missing rows
# MAGIC
# MAGIC **What:** we're copying the old zero-stock rows into a staging table, then inserting only the keys missing from production.
# MAGIC
# MAGIC **Why:** `ON CONFLICT DO NOTHING` leaves any existing `(store_id, sku)` row alone, including the re-listed item.
# MAGIC
# MAGIC **Look for:** Missing rows return, the re-listed item stays at five, and store 42 keeps its restock and new SKU.
# MAGIC
# MAGIC We know the DELETE's exact condition in this exercise. In an incident, confirm which rows were deleted before
# MAGIC repairing them. This rule keeps existing production rows; it does not compare timestamps or resolve arbitrary updates.

# COMMAND ----------

# DBTITLE 1,10e. Repair missing rows and verify the newer work
if globals().get("RECOVERY_STAGE") != "restored":
    raise RuntimeError("Run 10d first. To repeat recovery, restart at step 2.")

# 5. Repair: copy candidate rows from recovered, then insert only keys missing from production.
started = time.monotonic()
q("production", f"DROP TABLE IF EXISTS {REPAIR_TABLE}", fetch=False)
q("production", f"CREATE TABLE {REPAIR_TABLE} (LIKE {TABLE} INCLUDING DEFAULTS)", fetch=False)
with connect(RECOVERED) as src, connect("production") as dst, src.cursor() as sc, dst.cursor() as dc:
    with sc.copy(f"COPY (SELECT store_id, sku, on_hand, price_cents, aisle, updated_at FROM {TABLE} WHERE on_hand = 0) TO STDOUT") as out, \
         dc.copy(f"COPY {REPAIR_TABLE} (store_id, sku, on_hand, price_cents, aisle, updated_at) FROM STDIN") as inn:
        for chunk in out:
            inn.write(chunk)
candidates = q("production", f"SELECT count(*) AS n FROM {REPAIR_TABLE}")[0]["n"]
merged = q("production", f"INSERT INTO {TABLE} SELECT * FROM {REPAIR_TABLE} ON CONFLICT (store_id, sku) DO NOTHING RETURNING 1")
q("production", f"DROP TABLE {REPAIR_TABLE}", fetch=False)
rows2, stores2 = health("production")
kept = q("production", f"SELECT on_hand FROM {TABLE} WHERE store_id = %s AND sku = %s", (watched["store_id"], watched["sku"]))[0]["on_hand"]
newer = q("production", f"SELECT count(*) AS n FROM {TABLE} WHERE store_id = %s AND updated_at > %s", (BUSY_STORE, t0))[0]["n"]
stamp('repair', started)
assert rows_r == rows0 and rows2 == rows0 + 1, "The repaired row count does not match the expected inventory."
assert candidates - len(merged) == 1 and kept == 5, "The re-listed item was not preserved as expected."
assert newer == 4, "Store 42's three restocks and new SKU were not all preserved."
print(f"✓ Repaired production from {RECOVERED}: {len(merged):,} rows put back; {rows2:,} rows now.")
print(f"✓ Re-listed item preserved: store {watched['store_id']}, {watched['sku']} stays at {kept} on hand "
      f"({candidates - len(merged):,} existing key skipped).")
print(f"✓ Newer work preserved: store {BUSY_STORE} keeps three restocks and {NEW_SKU} ({newer} newer rows).")

delete_branch_if_exists(RECOVERED)
print(f"✓ Cleaned up: branches now {branch_ids()}")
RECOVERY_STAGE = "repaired"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up
# MAGIC
# MAGIC **What:** we're checking that production has the index and the temporary branches are gone.
# MAGIC
# MAGIC **Why:** We should finish with a tested change on production and no unfinished recovery exercise.
# MAGIC
# MAGIC **Look for:** `Branch per change: DONE`. The project stays available for another run.
# MAGIC We tested an index on a separate branch, then applied the same SQL to production. Where would that fit your review process?

# COMMAND ----------

# DBTITLE 1,Wrap-up
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run Setup D and Setup E first. They define this lab's "
                       "functions, and a Python restart clears them.")
if not {"before_ms", "after_ms", "prod_after_ms"}.issubset(TIMES) or not PRODUCTION_ISOLATED or not SCHEMA_CHECKED:
    raise RuntimeError("The branching exercise is incomplete. Finish steps 1 through 9 before wrapping up.")
remaining = branch_ids()
final_indexes = [r["indexname"] for r in q("production", IDX_SQL, (PG_SCHEMA, TABLE))]
if INDEX not in final_indexes or BRANCH in remaining:
    raise RuntimeError("The branching exercise is incomplete. Finish steps 1 through 9 before wrapping up.")
if RECOVERY_STAGE not in ("not_started", "repaired") or "recovered" in remaining:
    raise RuntimeError("Recovery is incomplete. Finish 10a through 10e, or reset the exercise at step 2.")
summary = (f"Branch per change: DONE | {PROJECT_ID} | SKU index on production | branches now {remaining}"
           f" | recovery {'verified' if RECOVERY_STAGE == 'repaired' else 'skipped'}")
print(summary)
LAB_COMPLETION_VERIFIED = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leave the lab ready for the session
# MAGIC
# MAGIC **What:** when the preparation notebook runs this lab, we remove the added index and extra SKU after all checks pass.
# MAGIC
# MAGIC **Why:** the project and inventory stay available, with the index exercise ready to repeat in the room.
# MAGIC
# MAGIC **Look for:** the preparation run confirms two million rows, the original primary key, and no test branches.
# MAGIC When you're presenting this notebook yourself, this cell skips the reset and leaves your completed work in place.

# COMMAND ----------

# DBTITLE 1,Leave the lab ready for the session
if "PREPARE_FOR_SESSION" not in globals():
    raise RuntimeError("Run the setup and exercise cells first.")
if not PREPARE_FOR_SESSION:
    print("Preparation reset skipped. Your completed lab stays available.")
else:
    if not PREPARATION_REQUEST_VALIDATED or not LAB_COMPLETION_VERIFIED or RECOVERY_STAGE != "repaired":
        raise RuntimeError("Session preparation requires the complete lab, including recovery and Wrap-up, to pass.")
    if branch_ids() != ["production"]:
        raise RuntimeError("Unexpected branches remain. Review them before preparing this project for the session.")

    q("production", f"DROP INDEX IF EXISTS {INDEX}", fetch=False)
    q("production", f"DROP TABLE IF EXISTS {TABLE}_repair", fetch=False)
    q("production", f"DELETE FROM {TABLE} WHERE sku = 'SKU-001001'", fetch=False)

    prepared_rows = q("production", f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
    prepared_indexes = [r["indexname"] for r in q("production", IDX_SQL, (PG_SCHEMA, TABLE))]
    primary_keys = q(
        "production",
        "SELECT count(*) AS n FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'p'",
        (PG_SCHEMA, TABLE),
    )[0]["n"]
    if prepared_rows != STORES * SKUS or primary_keys != 1 or INDEX in prepared_indexes:
        raise RuntimeError("The starting inventory state did not pass its checks. Run preparation again from the top.")
    if branch_ids() != ["production"]:
        raise RuntimeError("A branch appeared during preparation. Run again when this lab is not being used elsewhere.")

    print("✓ Session preparation: two million rows, primary key present, added index removed, production branch only.")
    dbutils.notebook.exit(json.dumps({
        "status": "PASSED",
        "lab": "branch",
        "release_version": RELEASE_VERSION,
        "user": user_email,
        "project_id": PROJECT_ID,
        "schema": PG_SCHEMA,
        "validation_run_id": VALIDATION_RUN_ID,
        "session_environment": SESSION_ENVIRONMENT,
        "prepared": True,
        "checks": ["isolation", "index", "schema", "recovery", "reset"],
    }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)
# MAGIC
# MAGIC **What:** this optional cell deletes your entire lab project when you no longer need it.
# MAGIC
# MAGIC **Why:** This removes the remaining lab data and compute resources.
# MAGIC
# MAGIC **Look for:** The cell is commented out, so running it as written does nothing. To delete the project,
# MAGIC uncomment its two lines and run it. You can create the lab again by starting from the top.

# COMMAND ----------

# DBTITLE 1,Optional. Delete your lab project
# Uncomment these two lines only when you're finished with this project.
# w.postgres.delete_project(name=f"projects/{PROJECT_ID}", purge=True).wait()
# print(f"✓ Project {PROJECT_ID} deleted")
