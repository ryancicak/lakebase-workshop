# Databricks notebook source
# MAGIC %md
# MAGIC # Session preflight: run this before the day
# MAGIC
# MAGIC Run this notebook a few days before the session, on **Serverless**, signed in as each person who will do the hands-on labs. It checks what the three labs need and prints exactly what to fix, and who can fix it.
# MAGIC
# MAGIC | Step | What it does | Time |
# MAGIC |---|---|---|
# MAGIC | 1. Quick check | Compute, network, and Lakebase access. Installs nothing and changes nothing | About 15 seconds |
# MAGIC | 2. Install | The same libraries the labs install | About 30 seconds |
# MAGIC | 3. Install the labs | Creates two Git folders from public GitHub under your own home folder, or reuses them if they're there, and confirms the lab notebooks are in them | About 30 seconds |
# MAGIC | 4. Full check | Creates one throwaway Lakebase project, connects to it, branches it, turns on Lakebase Search, then deletes it | About 2 to 5 minutes |
# MAGIC
# MAGIC At the end, copy the one-line report and send it to your Databricks contact. Two people can run this at the same time.
# MAGIC
# MAGIC **What this changes, and what it never touches.** Step 3 adds a folder named `lakebase-session-labs` under your own home folder with two Git folders in it (set `INSTALL_LABS = False` in that cell to skip it). Step 4 creates one small project named `lb-preflight-...` (1 CU, scales to zero after 5 idle minutes) and deletes it before it finishes. The notebook never touches, changes, or deletes any other project, folder, or table. The only projects it ever deletes are ones it created itself, which carry the display name "Lakebase session preflight (safe to delete)". If you only want the read-only check, stop after step 1.

# COMMAND ----------

# DBTITLE 1,1. Quick check (installs nothing, changes nothing)
"""Check what the labs need without installing or creating anything."""
import hashlib
import os
import re
import urllib.error
import urllib.request

from databricks.sdk import WorkspaceClient

BLOCKERS, NOTES = [], []


def report(ok, what, fix="", who="", kind="blocker"):
    """Print one check. A failed blocker gets ❌; a warning gets ⚠️."""
    if ok:
        print("✅ " + what)
        return True
    tail = (f"\n   Fix: {fix}" if fix else "") + (f" (who: {who})" if who else "")
    print(("⚠️ " if kind == "warning" else "❌ ") + what + tail)
    (NOTES if kind == "warning" else BLOCKERS).append(what)
    return False


def reachable(url):
    """True if this compute gets any HTTP answer from the site."""
    try:
        urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=15)
    except urllib.error.HTTPError:
        pass  # the site answered, so the network path is open
    except Exception:
        return False
    return True


def lakebase(method, path, **kwargs):
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if getattr(w.config, "workspace_id", None):
        headers["X-Databricks-Workspace-Id"] = str(w.config.workspace_id)
    return w.api_client.do(method, f"/api/2.0/postgres/{path}", headers=headers, **kwargs)


w = WorkspaceClient()
me = w.current_user.me()
print(f"Hi {getattr(me, 'display_name', None) or me.user_name}. This notebook takes about five minutes and never "
      f"touches your existing projects.\n\nQuick check as {me.user_name} in {w.config.host}\n")

runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
try:
    import importlib.metadata as _md
    import sys as _sys
    env_versions = (f"Python {_sys.version.split()[0]}, protobuf {_md.version('protobuf')}, "
                    f"databricks-sdk {_md.version('databricks-sdk')} preinstalled")
except Exception:
    env_versions = "versions unknown"
report(os.environ.get("IS_SERVERLESS", "").lower() in ("true", "1") or runtime.startswith("client."),
       f"Serverless compute ({runtime or 'unknown runtime'}; {env_versions})",
       "Pick Serverless in the compute menu at the top right. The labs were tested on serverless", "you",
       kind="warning")

for site, url, use in (
    ("pypi.org", "https://pypi.org/simple/psycopg/", "the %pip cell in every lab"),
    ("files.pythonhosted.org", "https://files.pythonhosted.org/", "the %pip cell in every lab"),
    ("github.com", "https://github.com/databricks/cli/releases", "the move lab's Databricks CLI download"),
    ("objects.githubusercontent.com", "https://objects.githubusercontent.com/", "the move lab's CLI download"),
    ("apt.postgresql.org", "https://apt.postgresql.org/pub/repos/apt/", "the move lab's pg_dump download"),
):
    report(reachable(url), f"Serverless can reach {site} ({use})",
           f"Let serverless compute reach {site} over HTTPS (serverless network policy)", "workspace admin")

projects, token = {}, None
try:
    while True:
        page = lakebase("GET", "projects", query={"page_token": token} if token else None)
        for p in page.get("projects", []):
            projects[p["name"].split("/", 1)[1]] = p.get("status", {})
        token = page.get("next_page_token")
        if not token:
            break
    report(True, f"Lakebase is on in this workspace ({len(projects)} project(s) visible to you)")
    report(len(projects) < 950, f"Room for new projects ({len(projects)} visible to you; the workspace limit is 1,000)",
           "Delete unused projects, or ask Databricks to raise the limit", "workspace admin", kind="warning")
except Exception as e:
    report(False, f"Lakebase API: {(str(e).splitlines() or [type(e).__name__])[0][:200]}",
           "Turn on Lakebase for this workspace, and confirm people can open Lakebase Postgres from the grid "
           "menu at the top right", "workspace admin")

canonical = me.user_name.strip().casefold()
local = canonical.split("@")[0]
slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", local)).strip("-")[:23].rstrip("-") or "user"
search_project = f"lakebase-retail-search-{slug}-{hashlib.sha256(canonical.encode()).hexdigest()[:16]}"
if search_project in projects:
    print(f"✅ Your Search lab project already exists ({search_project}); the lab will reuse it")
move_slug = re.sub(r"[^a-z0-9]+", "-", me.user_name.split("@")[0].lower()).strip("-")[:16].rstrip("-") or "user"
for kind_, label in (("old", "Lakebase move lab: old home"), ("new", "Lakebase move lab: new home")):
    pid = f"lb-move-{kind_}-{move_slug}-{me.id}"
    if pid in projects:
        ours = projects[pid].get("display_name") == label
        report(False, f"A move lab project from an earlier run is still here ({pid})" if ours
               else f"A project named {pid} exists, and the move lab didn't make it",
               "Open the move lab and run Module 7's two cleanup cells before the session" if ours
               else "Rename or delete it before the session", "you")

def is_git_folder(path):
    """True if the workspace path is a Git folder. Git folders report as DIRECTORY with is_git_folder set."""
    info = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": path})
    if (info.get("directory_info") or {}).get("is_git_folder") or info.get("object_type") == "REPO":
        return True
    if info.get("object_type") == "DIRECTORY" and info.get("object_id"):
        try:
            w.repos.get(repo_id=info["object_id"])  # older API shape: a Git folder's object id is its repo id
            return True
        except Exception:
            return False
    return False


try:
    path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    parts, in_git = path.split("/"), False
    for i in range(len(parts) - 1, 2, -1):
        if is_git_folder("/".join(parts[:i])):
            in_git = True
            break
    if in_git:
        report(True, "Git folders from GitHub work here (this notebook is in one)")
    else:
        print("ℹ️ This notebook isn't inside a Git folder. Step 3 tests Git folders by installing the labs.")
except Exception as e:
    print(f"ℹ️ Couldn't tell whether this notebook is in a Git folder ({str(e)[:120]}). Step 3 tests Git folders.")

if BLOCKERS:
    raise RuntimeError(f"Not ready: fix the {len(BLOCKERS)} item(s) marked ❌ above, then run this cell again.")
print("\nQuick check passed" + (", with the notes marked ⚠️." if NOTES else ".") +
      " Next, run steps 2 to 4.")

# COMMAND ----------

# DBTITLE 1,2. Install the libraries the labs use
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

# DBTITLE 1,3. Install the three labs as Git folders in your home folder (on by default)
"""Put the three labs where the session expects them: Git folders from public GitHub, under your own home folder.

Set INSTALL_LABS = False to skip this step. It creates or reuses folders under /Users/<you>/lakebase-session-labs
and never changes anything outside that folder. A ❌ here doesn't stop the full check in step 4.
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

INSTALL_LABS = True
LABS_FOLDER = "lakebase-session-labs"  # created under your home folder
REPOS = [  # (folder, GitHub URL, branch, what it is)
    ("lakebase-workshop", "https://github.com/ryancicak/lakebase-workshop", "feature/retail-search-context",
     "Lab 1 (branch per change) and Lab 2 (store-aware Search)"),
    ("lakebase-move-lab", "https://github.com/ryancicak/lakebase-move-lab", "main",
     "optional reading, moving a project between workspaces"),
]
LAB_INSTALL = {}
LAB_NOTEBOOKS = {  # notebooks that must be inside the lakebase-workshop folder
    "Lab 1": "labs/branch-per-change/Branch_Per_Change",
    "Lab 2": "labs/retail-search-context/Retail_Search_Context",
}
LAB_FILES_OK = True


def _brief(e):
    code = getattr(e, "error_code", None)
    first = (str(e).splitlines() or [type(e).__name__])[0][:200]
    return f"{code}: {first}" if code else f"{type(e).__name__}: {first}"


def _same_repo(a, b):
    tidy = lambda u: (u or "").strip().rstrip("/").removesuffix(".git").lower()
    return tidy(a) == tidy(b)


def say(ok, what, fix="", who=""):
    tail = (f"\n   Fix: {fix}" if fix else "") + (f" (who: {who})" if who else "")
    print(("✅ " if ok else "❌ ") + what + ("" if ok else tail))


w = WorkspaceClient()
me = w.current_user.me()
root = f"/Users/{me.user_name}/{LABS_FOLDER}"
LAB_PATHS = {}


def _git_folder(path):
    """(exists, is_git_folder, object_id) for a workspace path. Git folders report as DIRECTORY with is_git_folder set,
    and a Git folder's object id is its repo id."""
    try:
        info = w.api_client.do("GET", "/api/2.0/workspace/get-status", query={"path": path})
    except NotFound:
        return False, False, None
    oid = info.get("object_id")
    is_git = bool((info.get("directory_info") or {}).get("is_git_folder")) or info.get("object_type") == "REPO"
    if not is_git and info.get("object_type") == "DIRECTORY" and oid:
        try:
            w.repos.get(repo_id=oid)
            is_git = True
        except Exception:
            pass
    return True, is_git, oid


def _note_lab_paths(folder_path):
    """Record where the lab notebooks are, and confirm each one is really there."""
    global LAB_FILES_OK
    for lab, rel in LAB_NOTEBOOKS.items():
        full = f"{folder_path}/{rel}"
        exists, _, _ = _git_folder(full)
        LAB_PATHS[lab] = full
        if exists:
            say(True, f"{lab} notebook is there: {full}")
        else:
            LAB_FILES_OK = False
            say(False, f"{lab} notebook is missing: {full}",
                "Open the Git folder and click Pull to get the latest. If it's still missing, send your Databricks "
                "contact this line; the lab may not be published yet", "you")


def _enclosing_repo():
    """If this notebook was itself pulled from GitHub as a Git folder, find that folder."""
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        parts = path.split("/")
        for i in range(len(parts) - 1, 2, -1):
            parent = "/".join(parts[:i])
            exists, is_git, oid = _git_folder(parent)
            if exists and is_git:
                return parent, w.repos.get(repo_id=oid)
    except Exception:
        pass
    return None, None


here_path, here_repo = _enclosing_repo()

if not INSTALL_LABS:
    print("Skipped: INSTALL_LABS is False. The labs can be installed on the day (Workspace, Create, Git folder).")
else:
    try:
        w.workspace.mkdirs(root)
    except Exception as e:
        say(False, f"Create {root}: {_brief(e)}", "You need to be able to create folders in your own home folder",
            "workspace admin")
    for folder, url, branch, what in REPOS:
        path = f"{root}/{folder}"
        if here_repo is not None and _same_repo(here_repo.url, url):
            # This notebook came from that repository as a Git folder, so the lab is already here.
            LAB_INSTALL[folder] = True
            say(True, f"{folder}: this notebook is already in it at {here_path} "
                      f"({here_repo.branch} @ {(here_repo.head_commit_id or '')[:7]}). {what}")
            if folder == "lakebase-workshop":
                _note_lab_paths(here_path)
            continue
        try:
            exists, is_git, oid = _git_folder(path)
            if exists and not is_git:
                LAB_INSTALL[folder] = False
                say(False, f"{folder}: something that isn't a Git folder is already at {path}",
                    "Move or rename it, or set LABS_FOLDER in this cell to a new name", "you")
                continue
            if exists:
                repo = w.repos.get(repo_id=oid)
                if not _same_repo(repo.url, url):
                    LAB_INSTALL[folder] = False
                    say(False, f"{folder}: the Git folder at {path} points at {repo.url}, not {url}",
                        "Remove it, or set LABS_FOLDER in this cell to a new name", "you")
                    continue
                verb = "already installed"
            else:
                repo = w.repos.create(url=url, provider="gitHub", path=path)
                verb = "installed"
            if repo.branch != branch:
                w.repos.update(repo_id=repo.id, branch=branch)
                repo = w.repos.get(repo_id=repo.id)
            LAB_INSTALL[folder] = True
            say(True, f"{folder}: {verb} at {path} ({repo.branch} @ {(repo.head_commit_id or '')[:7]}). {what}")
            if folder == "lakebase-workshop":
                _note_lab_paths(path)
        except Exception as e:
            LAB_INSTALL[folder] = False
            say(False, f"{folder}: couldn't create a Git folder from {url}: {_brief(e)}",
                "Allow Git folders from github.com in this workspace's admin settings, or import the notebook "
                "files from the zip instead", "workspace admin")
    print(f"\nLabs installed: {sum(LAB_INSTALL.values())} of {len(REPOS)}.")
    for lab, full in LAB_PATHS.items():
        print(f"On the day, {lab} is {full}")

# COMMAND ----------

# DBTITLE 1,4. Full check (makes one throwaway project and deletes it)
"""Prove the three labs will work here: make one throwaway project, use it the way the labs do, delete it."""
import datetime as dt
import hashlib
import os
import re
import secrets
import time
import urllib.error
import urllib.request

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType

CATALOG = None  # None = this workspace's default catalog. Or name a catalog you can create schemas in.
KIT_VERSION = "2026-11-05d"
RESULTS, TIMES = {}, {}
if not CATALOG:
    try:
        _current = spark.catalog.currentCatalog()
    except Exception:
        _current = None
    CATALOG = _current if _current and _current not in ("hive_metastore", "spark_catalog", "system", "samples") else "main"


class Stop(Exception):
    """A blocker that makes the rest of the full check meaningless."""


def check(key, ok, what, fix="", who="", kind="blocker"):
    RESULTS[key] = (bool(ok), kind)
    if ok:
        print("✅ " + what)
    else:
        tail = (f"\n   Fix: {fix}" if fix else "") + (f" (who: {who})" if who else "")
        print(("⚠️ " if kind == "warning" else "❌ ") + what + tail)
    return bool(ok)


def brief(e):
    code = getattr(e, "error_code", None)
    first = (str(e).splitlines() or [type(e).__name__])[0][:200]
    return f"{code}: {first}" if code else f"{type(e).__name__}: {first}"


def reachable(url):
    try:
        urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=15)
    except urllib.error.HTTPError:
        pass
    except Exception:
        return False
    return True


def lakebase(method, path, **kwargs):
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if getattr(w.config, "workspace_id", None):
        headers["X-Databricks-Workspace-Id"] = str(w.config.workspace_id)
    return w.api_client.do(method, f"/api/2.0/postgres/{path}", headers=headers, **kwargs)


def wait_operation(started, timeout):
    name = (started or {}).get("name")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        op = w.postgres.get_operation(name=name)
        if op.done:
            if op.error:
                raise RuntimeError(f"{getattr(op.error, 'error_code', '')}: {getattr(op.error, 'message', '')}")
            return
        time.sleep(5)
    raise TimeoutError(f"{name} was still running after {timeout} s")


def connect(endpoint, dbname="databricks_postgres", timeout=600):
    """Connect with a fresh OAuth credential, retrying while the compute starts or restarts."""
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            ep = w.postgres.get_endpoint(name=endpoint)
            host = ep.status.hosts.host if ep.status and ep.status.hosts else None
            if host:
                cred = w.postgres.generate_database_credential(endpoint=endpoint)
                return psycopg.connect(host=host, dbname=dbname, user=me.user_name, password=cred.token,
                                       sslmode="require", connect_timeout=30)
        except (psycopg.OperationalError, NotFound) as e:
            last = e
        time.sleep(10)
    raise TimeoutError(f"couldn't connect to {endpoint} within {timeout} s: {last}")


def gone(path):
    try:
        lakebase("GET", path)
        return False
    except NotFound:
        return True


w = WorkspaceClient()
me = w.current_user.me()
canonical = me.user_name.strip().casefold()
fp = hashlib.sha256(canonical.encode()).hexdigest()[:8]
slug = re.sub(r"[^a-z0-9]+", "-", canonical.split("@")[0]).strip("-")[:20].rstrip("-") or "user"
PREFIX, LABEL = f"lb-preflight-{slug}-{fp}-", "Lakebase session preflight (safe to delete)"
# A fresh ID every run: a purge finishes in the background, so reusing an ID minutes later can collide.
# The random tail keeps two runs by the same person in the same second from colliding (seen 2026-11-04).
PID = f"{PREFIX}{dt.datetime.now(dt.timezone.utc):%m%d%H%M%S}-{secrets.token_hex(2)}"
PROJECT = f"projects/{PID}"
MAIN_EP = f"{PROJECT}/branches/production/endpoints/primary"
print(f"Full check as {me.user_name} in {w.config.host}, {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
check("serverless", os.environ.get("IS_SERVERLESS", "").lower() in ("true", "1") or runtime.startswith("client."),
      f"Serverless compute ({runtime or 'unknown runtime'})", "Pick Serverless in the compute menu", "you",
      kind="warning")
check("pypi", True, "Installed the labs' libraries from PyPI (step 2 worked)")
check("github", reachable("https://github.com/databricks/cli/releases"),
      "Serverless can reach github.com (the move lab's CLI download)",
      "Let serverless compute reach github.com and objects.githubusercontent.com over HTTPS", "workspace admin")
check("apt", reachable("https://apt.postgresql.org/pub/repos/apt/"),
      "Serverless can reach apt.postgresql.org (the move lab's pg_dump download)",
      "Let serverless compute reach apt.postgresql.org over HTTPS", "workspace admin")

conn, created = None, False
try:
    leftovers, token = [], None
    try:
        while True:
            page = lakebase("GET", "projects", query={"page_token": token} if token else None)
            for p in page.get("projects", []):
                status = p.get("status", {})
                if (p["name"].split("/", 1)[1].startswith(PREFIX) and status.get("display_name") == LABEL
                        and str(status.get("owner", "")).casefold() == canonical):
                    # Only tidy projects that are clearly abandoned. A check project made in the last
                    # 15 minutes is most likely another copy of this notebook that you're running now.
                    age_min = None
                    try:
                        made_at = dt.datetime.fromisoformat(str(p.get("create_time")).replace("Z", "+00:00"))
                        age_min = (dt.datetime.now(dt.timezone.utc) - made_at).total_seconds() / 60
                    except (TypeError, ValueError):
                        age_min = None
                    if age_min is not None and age_min < 15:
                        print(f"Another preflight run by you looks active ({p['name'].split('/', 1)[1]}, "
                              f"started {age_min:.0f} min ago); leaving it alone")
                        continue
                    leftovers.append(p["name"])
            token = page.get("next_page_token")
            if not token:
                break
        for name in leftovers:
            try:
                w.postgres.delete_project(name=name, purge=True).wait()
                print(f"Removed a leftover check project from an earlier, interrupted run: {name.split('/', 1)[1]}")
            except NotFound:
                pass
    except Exception as e:  # tidying is best effort; it never decides readiness
        print(f"Couldn't look for leftover check projects ({brief(e)}); continuing")

    t0 = time.monotonic()
    try:
        endpoint_settings = {"autoscaling_limit_min_cu": 1, "autoscaling_limit_max_cu": 1,
                             "suspend_timeout_duration": "300s"}
        started = lakebase("POST", "projects", query={"project_id": PID}, body={
            "spec": {"display_name": LABEL, "pg_version": 17,
                     "custom_tags": [{"key": "purpose", "value": "lakebase-preflight"}],
                     "default_endpoint_settings": endpoint_settings},
            "initial_endpoint_spec": {**endpoint_settings,
                                      "group": {"min": 1, "max": 1, "enable_readable_secondaries": False}},
        })
        created = True
        wait_operation(started, 600)
    except PermissionDenied as e:
        check("create", False, f"Create a Lakebase project: {brief(e)}",
              "Every workspace user can create projects by default. Check that Lakebase is on for this "
              "workspace and that no workspace or serverless usage policy blocks it", "workspace admin")
        raise Stop()
    except Exception as e:
        check("create", False, f"Create a Lakebase project: {brief(e)}",
              "Run this cell again. If it fails the same way, send this line to your Databricks contact", "you")
        raise Stop()
    TIMES["create"] = time.monotonic() - t0
    check("create", True, f"Create a Lakebase project ({TIMES['create']:.0f} s)")

    t0 = time.monotonic()
    try:
        conn = connect(MAIN_EP)
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, current_setting('server_version')")
            role, version = cur.fetchone()
    except Exception as e:
        check("connect", False, f"Connect to the project from this compute: {brief(e)}",
              "Serverless compute must reach the Lakebase endpoint. Check the workspace's serverless network "
              "policy and any Private Link setup for Lakebase", "workspace admin")
        raise Stop()
    TIMES["connect"] = time.monotonic() - t0
    check("connect", True, f"Connect with your Databricks identity (Postgres {version} as {role}, "
                           f"{TIMES['connect']:.0f} s)")

    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA preflight")
            cur.execute("CREATE TABLE preflight.notes (id int PRIMARY KEY, note text)")
            cur.execute("INSERT INTO preflight.notes VALUES (1, 'written before the branch')")
            cur.execute("SELECT note FROM preflight.notes WHERE id = 1")
            ok = cur.fetchone()[0] == "written before the branch"
        conn.commit()
        check("ddl", ok, "Create a schema and table, write a row, and read it back")
    except Exception as e:
        check("ddl", False, f"Create a schema and table: {brief(e)}",
              "Send this line to your Databricks contact", "you")
        raise Stop()

    t0 = time.monotonic()
    branch_name = f"{PROJECT}/branches/preflight-pr"
    try:
        w.postgres.create_branch(parent=PROJECT, branch_id="preflight-pr", branch=Branch(
            spec=BranchSpec(source_branch=f"{PROJECT}/branches/production", no_expiry=True))).wait()
        # A new branch normally gets its own read/write endpoint; make one only if it doesn't appear.
        deadline, endpoints = time.monotonic() + 180, []
        while not endpoints and time.monotonic() < deadline:
            endpoints = list(w.postgres.list_endpoints(parent=branch_name))
            if not endpoints:
                time.sleep(10)
        if not endpoints:
            w.postgres.create_endpoint(parent=branch_name, endpoint_id="primary", endpoint=Endpoint(
                spec=EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                                  autoscaling_limit_min_cu=1, autoscaling_limit_max_cu=1))).wait()
            endpoints = list(w.postgres.list_endpoints(parent=branch_name))
        branch_conn = connect(endpoints[0].name)
        with branch_conn.cursor() as cur:
            cur.execute("SELECT note FROM preflight.notes WHERE id = 1")
            inherited = cur.fetchone()[0] == "written before the branch"
        branch_conn.close()
        deleting = w.postgres.delete_branch(name=branch_name)
        if hasattr(deleting, "wait"):
            deleting.wait()
        TIMES["branch"] = time.monotonic() - t0
        check("branch", inherited, f"Create a branch, connect to it, see the parent's data, delete it "
                                   f"({TIMES['branch']:.0f} s)")
    except Exception as e:
        check("branch", False, f"Branch the project: {brief(e)}",
              "The branch-per-change and move labs need to create and delete branches on their own projects. "
              "Send this line to your Databricks contact", "you")

    conn.close()
    conn = None
    t0 = time.monotonic()
    try:
        started = lakebase("POST", f"{PROJECT}/search-extensions", body={})
        wait_operation(started, 900)
        deadline, preload = time.monotonic() + 600, ""
        while time.monotonic() < deadline:
            try:
                conn = connect(MAIN_EP)
                with conn.cursor() as cur:
                    cur.execute("SHOW shared_preload_libraries")
                    preload = cur.fetchone()[0]
                if {"lakebase_vector", "lakebase_text"} <= {p.strip() for p in preload.split(",")}:
                    break
                conn.close()
                conn = None
            except Exception:
                conn = None
            time.sleep(10)
        if conn is None:
            raise TimeoutError("the compute restart after turning on Search didn't settle within 10 minutes")
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS lakebase_text")
            cur.execute("SELECT count(*) FROM pg_extension WHERE extname IN ('lakebase_vector', 'lakebase_text')")
            loaded = cur.fetchone()[0] == 2
        conn.rollback()
        TIMES["search"] = time.monotonic() - t0
        check("search", loaded, f"Turn on Lakebase Search and load its extensions ({TIMES['search']:.0f} s)")
    except PermissionDenied as e:
        check("search", False, f"Turn on Lakebase Search: {brief(e)}",
              "The Search lab turns on Lakebase Search for each person's own project. Check that the workspace "
              "allows it", "workspace admin")
    except Exception as e:
        check("search", False, f"Turn on Lakebase Search: {brief(e)}",
              "Run this cell again. If it fails the same way, send this line to your Databricks contact", "you")

    uc_schema = f"lb_preflight_{slug.replace('-', '_')}_{fp}"
    try:
        if spark.sql(f"SHOW SCHEMAS IN `{CATALOG}` LIKE '{uc_schema}'").count() == 0:
            spark.sql(f"CREATE SCHEMA `{CATALOG}`.`{uc_schema}`")
            spark.sql(f"DROP SCHEMA `{CATALOG}`.`{uc_schema}` CASCADE")
        check("uc", True, f"Create a schema in catalog {CATALOG} (the move lab's optional synced table)",
              kind="warning")
    except Exception as e:
        check("uc", False, f"Create a schema in catalog {CATALOG}: {brief(e)}",
              f"Grant USE CATALOG and CREATE SCHEMA on {CATALOG}, or set CATALOG at the top of this cell to one "
              "you can use. The move lab skips its optional synced table without it", "catalog owner",
              kind="warning")
except Stop:
    pass
finally:
    if conn is not None:
        conn.close()
    if created:
        try:
            w.postgres.delete_project(name=PROJECT, purge=True).wait()
        except Exception as e:
            print(f"Couldn't delete {PID}: {brief(e)}")
        check("cleanup", gone(PROJECT), f"Deleted the throwaway project {PID}",
              f"Delete {PID} in Lakebase Postgres. It's safe to remove", "you", kind="warning")

LABS = {
    "Branch-per-change lab": ("create", "connect", "ddl", "branch"),
    "Retail Search lab": ("pypi", "create", "connect", "ddl", "search"),
    "Move lab": ("pypi", "github", "apt", "create", "connect", "ddl", "branch"),
}
verdicts = {}
for lab, needs in LABS.items():
    failed = [k for k in needs if k in RESULTS and not RESULTS[k][0]]
    skipped = [k for k in needs if k not in RESULTS]  # never ran because an earlier check stopped the cell
    if not failed and not skipped:
        verdicts[lab] = "READY"
    else:
        verdicts[lab] = "NOT READY (" + ", ".join(failed or ["stopped early"]) + (
            "; later checks skipped" if skipped and failed else "") + ")"
notes = [k for k, (ok, kind) in RESULTS.items() if not ok and kind == "warning"]
installed = globals().get("LAB_INSTALL") or {}  # from step 3, when it ran
if installed and not all(installed.values()):
    notes.append("gitfolders")
if globals().get("LAB_FILES_OK") is False:
    notes.append("labfiles")

print("\n" + "\n".join(f"{lab}: {v}" for lab, v in verdicts.items()))
line = (f"Lakebase preflight {KIT_VERSION} run {dt.date.today()} | {me.user_name} | {w.config.host} | "
        + " | ".join(f"{lab}: {v}" for lab, v in verdicts.items())
        + (f" | labs installed {sum(installed.values())}/{len(installed)}" if installed else "")
        + (f" | notes: {', '.join(notes)}" if notes else "")
        + " | " + ", ".join(f"{k} {v:.0f}s" for k, v in TIMES.items()))
print("\nCopy this line and send it to your Databricks contact:\n" + line)
if any(v != "READY" for v in verdicts.values()):
    raise RuntimeError("Not ready for every lab: fix the items marked ❌ above, then run this cell again.")
