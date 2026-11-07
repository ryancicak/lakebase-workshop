# Databricks notebook source
# MAGIC %md
# MAGIC # Branch per change
# MAGIC
# MAGIC > Added by Ryan Cicak on 2026-11-05. Session preparation and wording revised 2026-11-06 (release 2026-11-06d).
# MAGIC
# MAGIC The question for this hour: how do you test a database change without touching production? In Lakebase the answer is a branch. You get a copy of production in seconds (nothing is copied; the branch shares the data), you make the change there, you check it, and then you run the same SQL on production.
# MAGIC
# MAGIC The change we'll test is small on purpose: an index for a lookup that today scans a two-million-row table.
# MAGIC
# MAGIC ```
# MAGIC production: store_inventory  ---------------------------> run the tested CREATE INDEX
# MAGIC                 |                                               ^
# MAGIC                 +--> pr-101: run lookup -> add index -> check ---+
# MAGIC ```
# MAGIC
# MAGIC **How we'll run it:** Serverless, one cell at a time, from the top. I'll stop at the results so you can see what changed. Steps 1 to 9 are the change. Step 10 is the part I like most, a recovery exercise, if we have time.
# MAGIC
# MAGIC **Your lab, your project:** `production` here is a branch in your own `lakebase-lab-<you>` project. Every store and product is made up. To run it again later, start at step 1; step 2 resets everything.
# MAGIC
# MAGIC **Docs:** [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches) |
# MAGIC [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) |
# MAGIC [Backup and restore](https://docs.databricks.com/aws/en/oltp/projects/backup-methods)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup A. Choose the libraries
# MAGIC
# MAGIC **What:** this cell writes the list of Python libraries the notebook needs: the Databricks client and a Postgres driver.
# MAGIC
# MAGIC **Why:** the one thing I want to avoid is a library install that breaks the notebook environment. So we keep protobuf on whatever major version your environment already has, and let everything else resolve around it.
# MAGIC
# MAGIC **Look for:** the list it prints. Nothing is created yet.

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
# MAGIC **What:** `%pip install` from that list.
# MAGIC
# MAGIC **Why:** the next cells use these libraries to talk to Lakebase and run SQL.
# MAGIC
# MAGIC **Look for:** it finishes with no red text. Then run the restart cell.

# COMMAND ----------

# DBTITLE 1,Setup B. Install the libraries
# MAGIC %pip install --quiet -r /tmp/lakebase_lab_requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup C. Restart Python
# MAGIC
# MAGIC **What:** we restart the notebook's Python so it picks up what we installed.
# MAGIC
# MAGIC **Why:** the running session still has the older versions loaded. A restart is the clean way to swap them.
# MAGIC
# MAGIC **Look for:** the restart finishes. One catch: a restart clears every Python variable, so run both helper cells below again after any restart.

# COMMAND ----------

# DBTITLE 1,Setup C. Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup D. Load the shared connection helpers
# MAGIC
# MAGIC **What:** `%run ../_setup` loads the shared code that figures out who you are and how to connect to Lakebase.
# MAGIC
# MAGIC **Why:** everyone in the room gets their own project name and their own connections from this one line.
# MAGIC
# MAGIC **Look for:** `Lab helpers loaded for <your email>`.

# COMMAND ----------

# DBTITLE 1,Setup D. Load the shared connection helpers
# MAGIC %run ../_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup E. Load this lab's helpers
# MAGIC
# MAGIC **What:** this defines the functions for running SQL, creating and deleting branches, and building the sample inventory table.
# MAGIC
# MAGIC **Why:** with the plumbing in one place, every exercise cell shows the one thing we're testing. Each SQL call opens a fresh connection, so if a compute went idle while we were talking, the next cell wakes it up on its own.
# MAGIC
# MAGIC **Look for:** your project name and the branch name `pr-101`. No data changes yet. Run this after Setup D, and again after any Python restart.

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


RELEASE_VERSION = "2026-11-06d"
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
# MAGIC **What:** we create your own Lakebase project (`lakebase-lab-<you>`), or reopen it if you ran this before.
# MAGIC
# MAGIC **Why:** you'll change this project's `production` branch in a minute. It's yours, so nobody else's work is in the way.
# MAGIC
# MAGIC **Look for:** `Production compute ready` and a link to the project. Usually under a minute.

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
# MAGIC **What:** two million made-up inventory rows: 2,000 stores times 1,000 products.
# MAGIC
# MAGIC **Why:** the primary key is `(store_id, sku)`, so finding one store's row is fast. The lookup we care about goes the other way: one SKU across every store. That's the one with no index today.
# MAGIC
# MAGIC **Look for:** `2,000,000 rows` and `no SKU index on production`. This cell also resets earlier runs (it drops the index we'll add, the extra SKU from step 10, and any leftover branches), so you can repeat the lab anytime.

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
# MAGIC **What:** we create `pr-101` from this lab's production branch.
# MAGIC
# MAGIC **Why:** this is the branch-per-change idea. If you open a pull request, you get a branch with production's data to test against, and production doesn't know you're there.
# MAGIC
# MAGIC **Look for:** `pr-101 sees 2,000,000 rows`, a few seconds after we asked for it. Nothing was copied; the branch shares the unchanged pages with production. It expires after 24 hours if we forget about it.

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
# MAGIC **What:** on the test branch, we run `SELECT store_id, on_hand, aisle FROM store_inventory WHERE sku = 'SKU-000777'` and ask Postgres how it did it.
# MAGIC
# MAGIC **Why:** before you add an index, look at the plan. If Postgres is already using one, you're done.
# MAGIC
# MAGIC **Look for:** `Seq Scan` and the milliseconds. Seq Scan means Postgres read all two million rows to find 2,000 of them. That number comes from your lab project on a synthetic table, so treat it as a before-and-after, not a production figure.

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
print(f"Lookup on {BRANCH} before the change: {node}, {ms:.0f} ms")
print(f"Result: {len(rows):,} stores carry {LOOKUP_SKU}. Seq Scan means Postgres read all two million rows to find them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add the index on the test branch
# MAGIC
# MAGIC **What:** `CREATE INDEX store_inventory_sku_idx ON store_inventory (sku)` on `pr-101`, then the same lookup again.
# MAGIC
# MAGIC **Why:** the index gives Postgres a direct path to one SKU instead of a scan. We're testing it where a mistake costs nothing.
# MAGIC
# MAGIC **Look for:** the plan flips to an `Index` or `Bitmap` scan and the milliseconds drop. Same SQL, new plan. Production hasn't changed yet. (If you're wondering whether that's true, that's the next cell.)

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
before_ms = TIMES.get("before_ms")
print(f"Lookup on {BRANCH} after the change: {node}, {ms:.1f} ms" + (f" (was {before_ms:.0f} ms)" if before_ms else ""))
print("Same SQL, new plan. Postgres took the index instead of scanning the table.")
pause("after the change on the branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Check production before changing it
# MAGIC
# MAGIC **What:** we list the indexes on both branches and run the lookup on production.
# MAGIC
# MAGIC **Why:** I want you to see this with your own eyes: the index exists on `pr-101` only. Testing on a branch left production alone.
# MAGIC
# MAGIC **Look for:** `store_inventory_sku_idx` in the branch list, not in production's, and production still on `Seq Scan`.

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
print(f"✓ Production untouched: the lookup there is still {node}, {ms:.0f} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add the tested index to production
# MAGIC
# MAGIC **What:** the same `CREATE INDEX` statement, now on production, then the lookup one more time.
# MAGIC
# MAGIC **Why:** this is the promote step. Lakebase doesn't merge a branch back for you, and you don't want it to: you want the reviewed SQL to run on production, which is what a deployment pipeline would do after approval. We're doing it by hand so you can see it.
# MAGIC
# MAGIC **Look for:** production on an `Index` or `Bitmap` scan, with the same drop in milliseconds you saw on the branch.

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
print(f"Lookup on production after promote: {node}, {ms:.1f} ms")
pause("after promote")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compare the table definitions
# MAGIC
# MAGIC **What:** column names, types, and index names on both branches, side by side.
# MAGIC
# MAGIC **Why:** after a promote, the question is "did production end up with what we tested?" This answers it.
# MAGIC
# MAGIC **Look for:** `Schema diff: no differences`. This compares structure, not row contents.

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
# MAGIC **What:** we delete `pr-101`.
# MAGIC
# MAGIC **Why:** the branch did its job. Deleting it drops its compute and its copy-on-write pages. In a pull request flow this happens when the PR closes.
# MAGIC
# MAGIC **Look for:** the remaining branch list: `['production']`.

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
# MAGIC
# MAGIC We did these steps by hand. The big thing is that a pipeline would call the same operations around a migration file that someone reviewed.
# MAGIC
# MAGIC | In the pull request | In Lakebase |
# MAGIC |---|---|
# MAGIC | Open the PR | Create a branch from the stable branch, with a TTL |
# MAGIC | CI runs the migration and the tests | Run the migration file against the branch; query it |
# MAGIC | Review | Schema diff between the branch and the stable branch |
# MAGIC | Merge | Run the same migration file against the stable branch, then production |
# MAGIC | Close the PR | Delete the branch |
# MAGIC
# MAGIC One thing to be clear about: promotion means running the reviewed SQL on production. Lakebase doesn't merge the branch's data back. This lab uses a native Postgres table; adding an index works on synced tables too, and the data refresh stays a separate process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional: recover deleted rows without losing newer work
# MAGIC
# MAGIC This is the failure I think about a lot now that agents run SQL. An agent is asked to clear one store's zero-stock rows. It leaves out the store filter, so the DELETE hits every store. Nothing crashes. The app keeps answering, and the answers are wrong. Meanwhile the stores keep working, so new rows land on top of the damage.
# MAGIC
# MAGIC The fix has two parts: get the deleted rows back from a point in time before the mistake, and keep everything that happened after it.
# MAGIC
# MAGIC ```
# MAGIC production:  record time -> bad DELETE -> stores update inventory -> put the missing rows back
# MAGIC                   |                                                        ^
# MAGIC                   +--> recovered: production as of the recorded time -------+
# MAGIC ```
# MAGIC
# MAGIC Run 10a through 10e in order. If we're short on time, skip the whole section and go to Wrap-up. If we've started 10b, finish it.
# MAGIC
# MAGIC ### 10a. Record the healthy state
# MAGIC
# MAGIC **What:** we note production's row count and the database clock before anything goes wrong.
# MAGIC
# MAGIC **Why:** that timestamp is what we'll hand to Lakebase in 10d: "give me production as of this moment."
# MAGIC
# MAGIC **Look for:** `2,000,000 rows` and a time in UTC. Nothing is deleted yet.

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
# MAGIC **What:** `DELETE FROM store_inventory WHERE on_hand = 0`, with no `store_id` in it, on this lab's production branch.
# MAGIC
# MAGIC **Why:** this is the mistake. A one-store cleanup becomes an every-store delete, about 20,000 rows, and nothing tells you.
# MAGIC
# MAGIC **Look for:** the row count drops, the lookup still works, and fewer stores "carry" SKU-000777 than a minute ago. That's the dangerous part: the database is healthy and the data is wrong.

# COMMAND ----------

# DBTITLE 1,10b. Run the bad DELETE in your lab project
if globals().get("RECOVERY_STAGE") != "recorded":
    raise RuntimeError("Run 10a first, then 10b through 10e in order. To repeat recovery, restart at step 2.")

# 2. The agent's statement, against production.
q("production", AGENT_SQL, fetch=False)
rows1, stores1 = health("production")
deleted = rows0 - rows1
print(f"✗ After the agent's DELETE: {rows1:,} rows ({deleted:,} gone); {stores1:,} stores carry {LOOKUP_SKU}. "
      "Nothing crashed. The app is answering with the wrong inventory.")
RECOVERY_STAGE = "deleted"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10c. Keep the stores working
# MAGIC
# MAGIC **What:** three real-world updates while nobody has noticed yet: store 42 restocks three products, a new SKU rolls out, and one store re-lists one of the deleted items with five on hand.
# MAGIC
# MAGIC **Why:** if your fix is "restore production to before the DELETE," you lose all three. The re-listed item is the tricky one, because it's a deleted row that now has a newer value.
# MAGIC
# MAGIC **Look for:** the three updates printed. Remember the re-listed item; we'll check it after the repair.

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
# MAGIC **What:** we create a branch named `recovered` from production as of the time we recorded in 10a. That's point-in-time restore.
# MAGIC
# MAGIC **Why:** the nice thing is that production keeps running. We get the deleted rows back on a branch we can read from, and nobody's app is pointed at a restored copy.
# MAGIC
# MAGIC **Look for:** `recovered` has the original two million rows. Production is untouched. This works within the project's history window (seven days by default).

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
# MAGIC **What:** we copy the rows the DELETE removed out of `recovered` into a staging table on production, then `INSERT ... ON CONFLICT (store_id, sku) DO NOTHING`.
# MAGIC
# MAGIC **Why:** `DO NOTHING` is the whole trick. Any `(store_id, sku)` that already exists on production is left alone, so the re-listed item keeps its five, store 42 keeps its restock, and the new SKU stays. Only the missing rows come back.
# MAGIC
# MAGIC **Look for:** about 20,000 rows put back and one skipped (the re-listed item). Then the count: two million plus the new SKU.
# MAGIC
# MAGIC Two honest caveats. We know the DELETE's exact condition here; in a real incident you'd confirm which rows went missing first (the query history helps). And this rule keeps existing production rows as they are; it doesn't compare timestamps or merge conflicting updates.

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
# MAGIC **What:** one last check that production has the index and the temporary branches are gone.
# MAGIC
# MAGIC **Why:** we should end with a tested change on production and nothing half-finished.
# MAGIC
# MAGIC **Look for:** `Branch per change: DONE`. The project stays, so you can run this again.
# MAGIC
# MAGIC So, where would this fit in how you review and ship database changes today? That's the conversation I want to have next.

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
# MAGIC **What:** when the START_HERE notebook runs this lab ahead of time, this cell removes the index and the extra SKU after everything passed.
# MAGIC
# MAGIC **Why:** so your project is loaded and the index exercise is ready to run again in the room.
# MAGIC
# MAGIC **Look for:** in an advance run, it confirms two million rows, the original primary key, and no test branches. When you run the notebook yourself, this cell skips the reset and leaves your work in place.

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
# MAGIC **What:** this cell deletes your whole lab project. It's commented out on purpose.
# MAGIC
# MAGIC **Why:** when you're done with the lab, this removes the data and the compute so nothing keeps running.
# MAGIC
# MAGIC **Look for:** as written, running it does nothing. To delete the project, uncomment the two lines and run it. You can rebuild the lab anytime from the top.

# COMMAND ----------

# DBTITLE 1,Optional. Delete your lab project
# Uncomment these two lines only when you're finished with this project.
# w.postgres.delete_project(name=f"projects/{PROJECT_ID}", purge=True).wait()
# print(f"✓ Project {PROJECT_ID} deleted")
