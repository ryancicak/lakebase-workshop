# Databricks notebook source
# MAGIC %md
# MAGIC # Branch per change
# MAGIC
# MAGIC **Lakebase feature:** copy-on-write database branches, used the way a pull request would use them.
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create your own Lakebase project (usually under a minute)
# MAGIC 2. Load a production-shaped table: 2,000 stores times 1,000 products, two million rows
# MAGIC 3. Branch it in seconds, with no data copied
# MAGIC 4. Reproduce a slow lookup on the branch and fix it there with an index
# MAGIC 5. Prove production never moved
# MAGIC 6. Promote the same change to production, diff the two, and delete the branch
# MAGIC 7. Optional: an agent breaks production with one bad DELETE; the stores keep working; you restore a point in time and merge only the deleted rows back
# MAGIC
# MAGIC **What this creates:** one project named `lakebase-lab-<you>`, one schema in it, one table, and short-lived branches.
# MAGIC It never touches any other project. Every cell can be re-run; the notebook resets its own story at step 2.
# MAGIC
# MAGIC **Docs:** [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches) |
# MAGIC [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) |
# MAGIC [Backup and restore](https://docs.databricks.com/aws/en/oltp/projects/backup-methods)

# COMMAND ----------

# Install only what this notebook needs, and keep protobuf on the major version this environment already ships.
# On some serverless environment versions protobuf is a core package: changing its major version stops the Python
# restart in the next cell ("Kernel exited"). Staying on the same major version is always safe.
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

# MAGIC %pip install --quiet -r /tmp/lakebase_lab_requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# DBTITLE 1,Helpers (run this after %run ../_setup, and again after any Python restart)
import time
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp
from psycopg.rows import dict_row
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.postgres import (Branch, BranchSpec, Duration, Endpoint, EndpointSpec, FieldMask,
                                             EndpointType, Project, ProjectDefaultEndpointSettings, ProjectSpec)


def _param(name, default):
    """Rehearsal knobs. Interactive runs use the defaults; a job can pass them as base parameters."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return type(default)(value) if str(value).strip() else default


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
# MAGIC ## 1. Your project
# MAGIC Every person gets their own project. If it already exists, it's reused. The compute gets a one-hour idle timeout,
# MAGIC so a pause in the room doesn't suspend it. Even if it does, every cell here opens its own short connection, and
# MAGIC the next cell simply wakes the compute.

# COMMAND ----------

# DBTITLE 1,1. Your project (usually under a minute)
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
try:
    w.postgres.get_project(name=f"projects/{PROJECT_ID}")
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
    print(f"✓ Project: created {PROJECT_ID} in {stamp('project', started)} s")

wait_for_endpoint("production")
idle = ensure_idle_timeout("production", IDLE_TIMEOUT_SECONDS)
print(f"✓ Production compute ready; idle timeout {idle} s")
show_view_link(f"Open {PROJECT_ID} in Lakebase", lakebase_project_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A production-shaped table
# MAGIC One row per store per product: 2,000 stores times 1,000 products, two million rows, keyed by store and SKU.
# MAGIC Lookups by store and SKU are fast because the primary key covers them. A lookup by SKU alone, across all stores,
# MAGIC has no index to use. Queries like that turn into full table scans and time out as the table grows.
# MAGIC
# MAGIC This step also resets the story so you can run the notebook again: it drops the SKU index on production if an
# MAGIC earlier run promoted it, and removes branches an earlier run left behind.

# COMMAND ----------

# DBTITLE 1,2. Production baseline (about a minute the first time)
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
count, seconds, built = build_table("production")

# Reset the story so every run starts the same way: no SKU index on production, no leftover branches, and none of the
# rows step 10 adds (its pretend new SKU, its repair staging table).
q("production", f"DROP INDEX IF EXISTS {INDEX}", fetch=False)
q("production", f"DROP TABLE IF EXISTS {TABLE}_repair", fetch=False)
q("production", f"DELETE FROM {TABLE} WHERE sku = 'SKU-001001'", fetch=False)
removed = [b for b in (BRANCH, "qa-stable", "qa", "oops", "recovered", "checkpoint-before-change") if delete_branch_if_exists(b)]
count = q("production", f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
print(f"✓ Baseline: {TABLE} has {count:,} rows" + (f" (built in {seconds} s)" if built else " (already there)"))
print("✓ Reset: no SKU index on production" + (f"; removed leftover branches {removed}" if removed else ""))
show_view_link(f"See {TABLE} on production", lakebase_tables_url("production"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Branch it
# MAGIC A branch is a copy-on-write child of production: it has every row, instantly, and nothing is copied until one
# MAGIC side changes. The branch gets a 24-hour TTL so it deletes itself if you forget.

# COMMAND ----------

# DBTITLE 1,3. Branch it (seconds, no data copied)
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
created, ready = create_branch(BRANCH)
TIMES["branch_create"], TIMES["branch_ready"] = created, ready
print(f"✓ Branch {BRANCH}: created in {created} s from production ({STORES * SKUS:,} rows, nothing copied); "
      f"its compute was ready after {ready} s")
n = q(BRANCH, f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]
print(f"✓ {BRANCH} sees {n:,} rows")
show_view_link(f"Open branch {BRANCH} in Lakebase", lakebase_project_url(branch=BRANCH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The problem, reproduced on the branch
# MAGIC "Which stores carry this SKU, and how many do they have?" Without an index on `sku`, Postgres reads all two million
# MAGIC rows to answer it.

# COMMAND ----------

# DBTITLE 1,4. The slow lookup, on the branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
node, ms, plan = explain(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
rows = q(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
TIMES["before_ms"] = ms
print(f"Lookup on {BRANCH} before the change: {node} | {ms:.0f} ms | {len(rows):,} stores carry {LOOKUP_SKU}")
print("\n".join("   " + l for l in plan[:4]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The change, on the branch only
# MAGIC The fix is one statement. It runs on the branch, so production is not touched, and you can see what it does
# MAGIC against the full data before anyone else does.

# COMMAND ----------

# DBTITLE 1,5. Fix it on the branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
q(BRANCH, MIGRATION_SQL, fetch=False)
q(BRANCH, f"ANALYZE {TABLE}", fetch=False)
print(f"✓ Index built on {BRANCH} in {stamp('index_branch', started)} s: {MIGRATION_SQL}")
node, ms, plan = explain(BRANCH, LOOKUP_SQL, (LOOKUP_SKU,))
TIMES["after_ms"] = ms
print(f"Lookup on {BRANCH} after the change: {node} | {ms:.1f} ms (was {TIMES['before_ms']:.0f} ms)")
pause("after the change on the branch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Production never moved

# COMMAND ----------

# DBTITLE 1,6. Production is untouched
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
IDX_SQL = "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s ORDER BY indexname"
prod_idx = [r["indexname"] for r in q("production", IDX_SQL, (PG_SCHEMA, TABLE))]
branch_idx = [r["indexname"] for r in q(BRANCH, IDX_SQL, (PG_SCHEMA, TABLE))]
node, ms, _ = explain("production", LOOKUP_SQL, (LOOKUP_SKU,))
print(f"{BRANCH} indexes:    {branch_idx}")
print(f"production indexes: {prod_idx}")
assert INDEX in branch_idx and INDEX not in prod_idx, "expected the index on the branch only"
print(f"✓ Production untouched: the lookup there is still {node} | {ms:.0f} ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Promote the change
# MAGIC In a pull request this is the merge. You don't merge the branch's data into production; you run the same
# MAGIC statement against production. Only the change moves forward, never the branch's rows.

# COMMAND ----------

# DBTITLE 1,7. Promote: the same statement, now on production
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
started = time.monotonic()
q("production", MIGRATION_SQL, fetch=False)
q("production", f"ANALYZE {TABLE}", fetch=False)
print(f"✓ Promoted: the same statement ran on production in {stamp('index_prod', started)} s")
node, ms, _ = explain("production", LOOKUP_SQL, (LOOKUP_SKU,))
TIMES["prod_after_ms"] = ms
print(f"Lookup on production after promote: {node} | {ms:.1f} ms")
pause("after promote")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Diff the two
# MAGIC Columns and indexes on the branch versus production. After promote they should match.

# COMMAND ----------

# DBTITLE 1,8. Schema diff
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
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
else:
    print(f"✗ Schema diff: {set(on_branch['indexes']) ^ set(on_prod['indexes'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. The change merged. The branch goes away.

# COMMAND ----------

# DBTITLE 1,9. Delete the branch
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
delete_branch_if_exists(BRANCH)
print(f"✓ Branch {BRANCH} deleted; branches now: {branch_ids()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What this maps to in a pull request
# MAGIC
# MAGIC | In the pull request | In Lakebase |
# MAGIC |---|---|
# MAGIC | Open the PR | Create a branch from the stable branch, with a TTL |
# MAGIC | CI runs the migration and the tests | Run the migration file against the branch; query it |
# MAGIC | Review | Schema diff between the branch and the stable branch |
# MAGIC | Merge | Replay the same migration file against the stable branch, then production |
# MAGIC | Close the PR | Delete the branch |
# MAGIC
# MAGIC The branch never crosses into production. The migration file does. On a synced table, `CREATE INDEX` is an
# MAGIC allowed operation too, so this is the same move for a table that Databricks keeps in sync for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional: an agent breaks production, you fix it from a point in time
# MAGIC
# MAGIC Here's the one that matters, so let me make the problem dead simple.
# MAGIC
# MAGIC **What happened.** Someone asked an agent to clear the zero-stock rows for one store. It ran this against production,
# MAGIC with no store filter:
# MAGIC
# MAGIC ```sql
# MAGIC DELETE FROM store_inventory WHERE on_hand = 0;
# MAGIC ```
# MAGIC
# MAGIC About 20,000 store-and-SKU rows are gone, across every store.
# MAGIC
# MAGIC **What it looks like.** Nothing crashed. The app keeps answering. For 20,000 items it now says "not carried here."
# MAGIC And the stores keep working: a restock lands, a new SKU rolls out, somebody re-lists one of the items the agent
# MAGIC deleted. The damage and the day's real work are now mixed together in the same table.
# MAGIC
# MAGIC **What you want.** The deleted rows back, without losing anything the stores did since.
# MAGIC
# MAGIC Point-in-time restore gives you production as it was at any second in the history window, as a new branch. There
# MAGIC are two ways to use it:
# MAGIC
# MAGIC - **Re-point** the app at the recovered branch. Fast, and right when production is trashed. Wrong here: you'd throw
# MAGIC   away the restock, the new SKU, and the re-list.
# MAGIC - **Repair in place.** Pull the deleted rows out of the recovered branch and merge them back into production by
# MAGIC   primary key, with one rule: anything newer in production wins. Same endpoint, nobody reconnects, the day's work
# MAGIC   survives. That's what this cell does.
# MAGIC
# MAGIC ```
# MAGIC time ----------------------------------------------------------------------------------------->
# MAGIC
# MAGIC production  [2,000,000] . t0 . [agent DELETE] . [restock, new SKU, re-list] . [merge back] . [2,000,001]
# MAGIC                               |                                                   ^
# MAGIC                               '-- recovered (production as of t0) ----------------'
# MAGIC                                   the ~20,000 deleted rows come from here; production's newer rows win
# MAGIC ```
# MAGIC
# MAGIC How the repair knows what to put back: the agent's statement is in the query history, so its predicate (`on_hand = 0`)
# MAGIC picks the candidate rows out of `recovered`. Then `INSERT ... ON CONFLICT DO NOTHING` on the primary key does the
# MAGIC rest: a row that still exists in production, including one the store re-listed after the damage, is left alone.
# MAGIC
# MAGIC | Row | At t0 | After the agent | The day goes on | After the repair |
# MAGIC |---|---|---|---|---|
# MAGIC | A zero-stock row at some store | present | deleted | | back, from `recovered` |
# MAGIC | A zero-stock row the store re-listed | present, 0 on hand | deleted | re-inserted, 5 on hand | kept at 5; production is newer |
# MAGIC | Store 42's restock on 3 SKUs | present | present | on_hand + 12 | untouched |
# MAGIC | A brand-new SKU at store 42 | didn't exist | | inserted | untouched |
# MAGIC
# MAGIC Snapshots are the scheduled version of the same restore. A checkpoint branch, one you create before a risky change,
# MAGIC is the manual version.

# COMMAND ----------

# DBTITLE 1,10. Optional: an agent breaks production, you fix it from a point in time (about three minutes)
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
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

# 2. The agent's statement, against production.
q("production", AGENT_SQL, fetch=False)
rows1, stores1 = health("production")
deleted = rows0 - rows1
print(f"✗ After the agent's DELETE: {rows1:,} rows ({deleted:,} gone); {stores1:,} stores carry {LOOKUP_SKU}. "
      "Nothing crashed. The app is wrong.")

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
      f"re-listed {watched['sku']} with 5 on hand (one of the deleted rows, now newer than the backup)")

# 4. Point-in-time restore: production exactly as it was at t0, as a new branch.
r_created, r_ready = create_branch(RECOVERED, source="production", at=t0)
rows_r, stores_r = health(RECOVERED)
print(f"✓ {RECOVERED}: production as of {t0:%H:%M:%S} UTC, created in {r_created} s, ready after {r_ready} s: "
      f"{rows_r:,} rows; {stores_r:,} stores carry {LOOKUP_SKU}")

# 5. Repair: the deleted rows, copied out of recovered and merged back by primary key. Anything newer in production wins.
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
print(f"✓ Repaired production from {RECOVERED} in {stamp('repair', started)} s: {candidates:,} candidate rows, {len(merged):,} put back, "
      f"{candidates - len(merged):,} skipped because production had a newer version")
print(f"✓ Production now: {rows2:,} rows; {stores2:,} stores carry {LOOKUP_SKU}; store {watched['store_id']} still shows {kept} on hand "
      f"for {watched['sku']}; store {BUSY_STORE} keeps its {newer} newer rows ({NEW_SKU} included). Same endpoint the whole time.")

delete_branch_if_exists(RECOVERED)
print(f"✓ Cleaned up: branches now {branch_ids()}")

# COMMAND ----------

# DBTITLE 1,Wrap-up
if "q" not in globals() or "PROJECT_ID" not in globals():
    raise RuntimeError("Run the two cells above the steps first: %run ../_setup and Helpers. They define this lab's "
                       "functions, and a Python restart clears them.")
summary = (f"Branch per change: DONE | {PROJECT_ID} | rows {STORES * SKUS:,} | branch in {TIMES.get('branch_create')} s"
           f" | lookup before {TIMES.get('before_ms', 0):.0f} ms, after {TIMES.get('after_ms', 0):.1f} ms"
           f" | index on branch {TIMES.get('index_branch')} s, on production {TIMES.get('index_prod')} s"
           f" | branches now {branch_ids()}")
print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)
# MAGIC The branches are gone. The project stays so you can run the notebook again. To remove it entirely, uncomment and run:

# COMMAND ----------

# UNCOMMENT TO DELETE YOUR PROJECT:
# w.postgres.delete_project(name=f"projects/{PROJECT_ID}", purge=True).wait()
# print(f"✓ Project {PROJECT_ID} deleted")
