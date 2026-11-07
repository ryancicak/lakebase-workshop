# Databricks notebook source
# MAGIC %md
# MAGIC <!-- Modified by Ryan Cicak, 2026-11-06: verified downloads, complete lab runs, and preparation checks. -->
# MAGIC # Get your two labs ready
# MAGIC
# MAGIC Run this before the session, signed in as the person who'll do the exercises. Pick **Serverless**, set **Standard version 6** in the Environment panel, and click **Run all**.
# MAGIC
# MAGIC What it does: it downloads the session versions of both labs, runs each one end to end as you, and leaves your two lab projects loaded and ready. That includes the branching recovery exercise and all eight Search checks, so if anything in your workspace is going to get in the way, we find out now and not in the room.
# MAGIC
# MAGIC **READY means** both notebooks ran, their results passed, and their starting data checked out. You'll get links to the two notebooks to open on the day.
# MAGIC
# MAGIC A few things to know. It creates a new folder under your home folder and two one-time job runs. Each lab creates its own Lakebase project (or reuses it), and those projects and their data stay afterward with a one-hour idle timeout. The exercises change only their own lab data. The optional project-move lab isn't part of this.
# MAGIC
# MAGIC Run one preparation at a time per person, and don't work in the lab projects while it's running. Different people can run it at the same time.
# MAGIC
# MAGIC If something fails, keep the error and the job link and send them to your Databricks contact. A failed or partial check won't print READY. One more thing: this proves the labs run in your workspace; it doesn't replace the walkthrough we'll do together.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Choose the libraries
# MAGIC
# MAGIC **What:** this cell writes the Python library list: the Databricks client, and protobuf kept on your environment's current major version.
# MAGIC
# MAGIC **Why:** we've seen an install replace a core package and kill the notebook. Pinning protobuf to what you already have avoids that.
# MAGIC
# MAGIC **Look for:** the selected libraries. The next cell installs them.

# COMMAND ----------

# DBTITLE 1,1. Choose the libraries
import importlib.metadata as _md

_reqs = ["databricks-sdk>=0.81.0"]
try:
    _pb = _md.version("protobuf")
    _reqs.append(f"protobuf>={_pb},<{int(_pb.split('.')[0]) + 1}")
except _md.PackageNotFoundError:
    pass
with open("/tmp/lakebase_ready_requirements.txt", "w") as _f:
    _f.write("\n".join(_reqs) + "\n")
print("Libraries selected: " + ", ".join(_reqs))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install the libraries
# MAGIC
# MAGIC **What:** `%pip install` from that list.
# MAGIC
# MAGIC **Why:** the client is what downloads the notebooks into your workspace and starts the lab runs.
# MAGIC
# MAGIC **Look for:** it finishes cleanly. Then run the restart.

# COMMAND ----------

# DBTITLE 1,Install the libraries
# MAGIC %pip install --quiet -r /tmp/lakebase_ready_requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restart Python
# MAGIC
# MAGIC **What:** we restart Python so it loads the client we installed.
# MAGIC
# MAGIC **Why:** the running session still has the earlier version loaded.
# MAGIC
# MAGIC **Look for:** the restart completes. Then run the remaining cells in order.

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load the preparation steps
# MAGIC
# MAGIC **What:** this defines the download, run, and result checks. It doesn't run anything yet.
# MAGIC
# MAGIC **Why:** both labs have to use the session version and pass under your identity. A job that finished isn't enough; each lab also has to confirm its data is ready for another run.
# MAGIC
# MAGIC **Look for:** the release and environment version. The next cell runs the steps.

# COMMAND ----------

# DBTITLE 1,2. Load the preparation steps
import base64
import hashlib
import json
import re
import secrets
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, JobRunAs, NotebookTask, SubmitTask
from databricks.sdk.service.workspace import ImportFormat, Language

KIT_VERSION = "2026-11-06d"
RELEASE_TAG = "retail-session-2026-11-06d"
SESSION_ENVIRONMENT = "6"
SOURCE_BASE = f"https://raw.githubusercontent.com/ryancicak/lakebase-workshop/{RELEASE_TAG}/"
# These fingerprints are filled from the exact release files before packaging.
SOURCES = {
    'labs/_setup.py': '5b2e750083662d685b8e4cd567c4df17ac07fdf15be2e91383ff8c767e4599c9',
    'labs/branch-per-change/Branch_Per_Change.py': 'd1eb7484b438dab5c899744499283d29ce7e8b4ac19c47733782a198c1b4b324',
    'labs/retail-search-context/Retail_Search_Context.py': 'c3e2e0c73d52fd1d07e0861ae11d997dc1928195ffa6d8834de966bd68023072',
}
LABS = {
    "branch": {
        "label": "Branching",
        "path": "labs/branch-per-change/Branch_Per_Change",
        "checks": {"isolation", "index", "schema", "recovery", "reset"},
    },
    "search": {
        "label": "Retail Search",
        "path": "labs/retail-search-context/Retail_Search_Context",
        "checks": {"exact", "hybrid", "store_rules", "no_result", "restock", "reset"},
    },
}


def enum_text(value):
    return str(getattr(value, "value", value) or "").rsplit(".", 1)[-1]


def rehearsal_project_suffix(user):
    """Optional job-only isolation for instructor validation; participants leave it unset."""
    try:
        suffix = dbutils.widgets.get("project_suffix")
    except Exception:
        return ""
    if suffix == "":
        return ""
    if not isinstance(suffix, str) or not re.fullmatch(r"[a-z0-9]{1,8}", suffix):
        raise RuntimeError("Rehearsal project_suffix must contain 1 to 8 lowercase letters or digits.")
    slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", user.split("@")[0].lower())).strip("-")
    search_slug = slug[:23].rstrip("-") or "user"
    names = (f"lakebase-lab-{slug}-{suffix}", f"lakebase-retail-search-{search_slug}-{'0' * 16}-{suffix}")
    if any(len(name) > 63 for name in names):
        raise RuntimeError("Rehearsal project_suffix makes this identity's project name too long.")
    return suffix


def download_sources():
    """Verify every download before importing any notebook."""
    files = {}
    for relative, expected in SOURCES.items():
        if len(expected) != 64:
            raise RuntimeError("This release is missing its file fingerprints. Download the published READY notebook.")
        request = urllib.request.Request(SOURCE_BASE + relative, headers={"User-Agent": "lakebase-session-ready"})
        with urllib.request.urlopen(request, timeout=45) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != expected:
            raise RuntimeError(f"The downloaded {relative} doesn't match release {KIT_VERSION}. Nothing was imported.")
        files[relative] = data
    return files


def import_source(w, path, data):
    w.workspace.import_(
        path=path, format=ImportFormat.SOURCE, language=Language.PYTHON,
        content=base64.b64encode(data).decode("ascii"), overwrite=False,
    )


def install_sources(w, user, run_id, files):
    """Use a new folder so earlier lab copies and edits are left in place."""
    if set(files) != set(SOURCES):
        raise RuntimeError("A required notebook or shared setup file is missing.")
    for relative, data in files.items():
        if hashlib.sha256(data).hexdigest() != SOURCES[relative]:
            raise RuntimeError(f"Source verification failed before import: {relative}")
    root = f"/Users/{user}/lakebase-session-labs/{KIT_VERSION}-{run_id}"
    paths, links = {}, {}
    for relative, data in files.items():
        path = root + "/" + relative.removesuffix(".py")
        w.workspace.mkdirs(path.rsplit("/", 1)[0])
        import_source(w, path, data)
    for lab, spec in LABS.items():
        paths[lab] = root + "/" + spec["path"]
        info = w.workspace.get_status(paths[lab])
        if enum_text(info.object_type) != "NOTEBOOK" or info.object_id is None:
            raise RuntimeError(f"The imported {spec['label']} notebook is missing.")
        links[lab] = w.config.host.rstrip("/") + f"/#notebook/{info.object_id}"
    return paths, links


def validate_receipt(text, lab, user, run_id):
    """Reject stale, missing, partial, or wrong-identity completion evidence."""
    try:
        receipt = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{LABS[lab]['label']} didn't return its completion check.") from exc
    if not isinstance(receipt, dict):
        raise RuntimeError("The lab returned an invalid completion check.")
    expected = {
        "status": "PASSED", "lab": lab, "release_version": KIT_VERSION,
        "validation_run_id": run_id, "session_environment": SESSION_ENVIRONMENT,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise RuntimeError(f"{LABS[lab]['label']} completion check doesn't match {key}.")
    if str(receipt.get("user", "")).strip().casefold() != user.strip().casefold():
        raise RuntimeError("The lab ran under a different identity.")
    if receipt.get("prepared") is not True:
        raise RuntimeError(f"{LABS[lab]['label']} didn't verify its starting data.")
    checks = receipt.get("checks")
    if not isinstance(checks, list) or not all(isinstance(c, str) for c in checks):
        raise RuntimeError("The lab returned invalid check results.")
    if not LABS[lab]["checks"].issubset(set(checks)):
        raise RuntimeError(f"{LABS[lab]['label']} did not complete every required exercise.")
    if not isinstance(receipt.get("project_id"), str) or not receipt["project_id"]:
        raise RuntimeError("The lab didn't identify its prepared project.")
    if not isinstance(receipt.get("schema"), str) or not receipt["schema"]:
        raise RuntimeError("The lab didn't identify its prepared schema.")
    return receipt


def run_lab(w, lab, path, user, run_id, state, timeout_seconds=2400, project_suffix=""):
    """Submit as this participant, pin environment 6, and require task completion evidence."""
    label = LABS[lab]["label"]
    parameters = {
        "prepare_for_session": "true", "release_version": KIT_VERSION,
        "validation_run_id": run_id, "session_environment": SESSION_ENVIRONMENT,
    }
    if project_suffix:
        parameters["project_suffix"] = project_suffix
    task = SubmitTask(
        task_key=lab, environment_key="session",
        notebook_task=NotebookTask(notebook_path=path, base_parameters=parameters),
        timeout_seconds=timeout_seconds,
    )
    # If submission fails after the service accepted it, keep the lock until someone checks the run.
    state["may_be_running"] = True
    state.pop("run_id", None)
    state["run_name"] = f"Lakebase session {KIT_VERSION}: {label} ({run_id})"
    submitted = w.jobs.submit(
        run_name=state["run_name"],
        tasks=[task], run_as=JobRunAs(user_name=user),
        environments=[JobEnvironment(environment_key="session", spec=Environment(client=SESSION_ENVIRONMENT))],
        idempotency_token=f"lb-{run_id}-{lab}", timeout_seconds=timeout_seconds,
    )
    parent_id = submitted.run_id
    if parent_id is None:
        raise RuntimeError(f"{label} submission didn't return a run ID. Check Jobs & Pipelines before retrying.")
    state["run_id"] = parent_id
    link = w.config.host.rstrip("/") + f"/#joblist?run={parent_id}"
    print(f"Running {label} as {user}, serverless environment {SESSION_ENVIRONMENT}.")
    deadline = time.monotonic() + timeout_seconds + 180
    last = None
    while time.monotonic() < deadline:
        current = w.jobs.get_run(run_id=parent_id)
        if current.run_page_url and current.run_page_url != link:
            link = current.run_page_url
            print(f"Run details: {link}")
        lifecycle = enum_text(getattr(current.state, "life_cycle_state", None))
        if lifecycle != last:
            print(f"  {label}: {lifecycle.lower().replace('_', ' ')}")
            last = lifecycle
        if lifecycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            state["may_be_running"] = False
            result = enum_text(getattr(current.state, "result_state", None))
            if result != "SUCCESS":
                detail = getattr(current.state, "state_message", "") or result
                raise RuntimeError(f"{label} failed: {detail}. Open {link}")
            tasks = [t for t in (current.tasks or []) if t.task_key == lab]
            if not tasks:
                raise RuntimeError(f"{label} is missing its notebook task result. Open {link}")
            if len(tasks) > 1:
                attempts = [getattr(t, "attempt_number", None) for t in tasks]
                if not all(type(attempt) is int and attempt >= 0 for attempt in attempts):
                    raise RuntimeError(f"{label} has ambiguous notebook task attempts. Open {link}")
                latest_attempt = max(attempts)
                tasks = [t for t in tasks if t.attempt_number == latest_attempt]
            if len(tasks) != 1 or tasks[0].run_id is None:
                raise RuntimeError(f"{label} has no unique latest notebook task result. Open {link}")
            completed = tasks[0]
            if enum_text(getattr(completed.state, "result_state", None)) != "SUCCESS":
                raise RuntimeError(f"{label}'s notebook task didn't succeed. Open {link}")
            output = w.jobs.get_run_output(run_id=completed.run_id)
            notebook_output = output.notebook_output
            if output.error or notebook_output is None or notebook_output.truncated:
                raise RuntimeError(f"{label} has missing or incomplete result evidence. Open {link}")
            receipt = validate_receipt(notebook_output.result, lab, user, run_id)
            if project_suffix and not receipt["project_id"].endswith("-" + project_suffix):
                raise RuntimeError(f"{label} did not use the isolated rehearsal project.")
            print(f"✓ {label}: all exercises passed; starting data verified.")
            return receipt, link
        time.sleep(10)
    try:
        w.jobs.cancel_run(run_id=parent_id)
    except Exception as exc:
        raise RuntimeError(
            f"{label} exceeded the wait limit, and cancellation couldn't be requested. Check {link} before retrying."
        ) from exc
    # Cancellation is asynchronous. Leave the lock rather than risk a second run on the same projects.
    raise RuntimeError(f"{label} exceeded the wait limit. Cancellation requested; check {link} before retrying.")


def prepare_session():
    """The only path that prints READY; no cached results are accepted."""
    w = WorkspaceClient()
    user = w.current_user.me().user_name
    if not user or "/" in user:
        raise RuntimeError("Couldn't identify a participant user. Run this signed in with your own account.")
    project_suffix = rehearsal_project_suffix(user)
    run_id = secrets.token_hex(8)
    home = f"/Users/{user}/lakebase-session-labs"
    lock_path = home + "/READY_in_progress"
    state = {"may_be_running": False}
    receipts, run_links = {}, {}
    lock_created = False
    print(f"Preparing release {KIT_VERSION} for {user} in {w.config.host}.")
    try:
        files = download_sources()
        print("✓ All three source files match the session release.")
        w.workspace.mkdirs(home)
        marker = (
            "# Databricks notebook source\n# Preparation lock. Do not run this notebook.\n"
            f"# User: {user}\n# Release: {KIT_VERSION}\n# Run: {run_id}\n"
            f"# Started UTC: {datetime.now(timezone.utc).isoformat()}\n"
            "# Remove only after confirming all preparation jobs for this user have stopped.\n"
        ).encode("utf-8")
        try:
            import_source(w, lock_path, marker)
            lock_created = True
        except Exception as exc:
            raise RuntimeError(
                f"Couldn't reserve your preparation run. If {lock_path} already exists, another run may be active. "
                "Check Jobs & Pipelines. After any earlier run has stopped, delete only that marker notebook and retry. "
                "If no marker exists, check permission to create notebooks in your home folder."
            ) from exc
        paths, links = install_sources(w, user, run_id, files)
        for lab in LABS:
            print(f"Installed {LABS[lab]['label']}: {links[lab]}")
        for lab in LABS:
            receipts[lab], run_links[lab] = run_lab(w, lab, paths[lab], user, run_id, state, project_suffix=project_suffix)
        if set(receipts) != set(LABS):
            raise RuntimeError("Both lab checks must complete before READY.")
        result = {
            "status": "READY", "release_version": KIT_VERSION, "user": user,
            "workspace": w.config.host, "session_environment": SESSION_ENVIRONMENT,
            "validation_run_id": run_id, "labs": receipts,
            "notebooks": links, "runs": run_links,
        }
    except Exception as exc:
        print(f"\nNOT READY: {exc}")
        raise
    finally:
        if lock_created and not state["may_be_running"]:
            try:
                w.workspace.delete(path=lock_path, recursive=False)
            except Exception as exc:
                raise RuntimeError(f"The checks stopped, but the preparation marker couldn't be removed: {lock_path}") from exc
        elif lock_created:
            print(f"Preparation may still be running. Marker retained: {lock_path}")
            if state.get("run_id"):
                print(f"Check job run {state['run_id']} before deleting the marker or trying again.")
            elif state.get("run_name"):
                print(f"Look in Jobs & Pipelines for: {state['run_name']}")
    # Print success only after both lab receipts and lock cleanup succeeded.
    print("\nREADY: both labs passed, and your lab projects are prepared.")
    for lab in LABS:
        print(f"{LABS[lab]['label']}: {links[lab]}")
        print(f"  Project: {receipts[lab]['project_id']}")
    print(f"On the day, open these notebooks and choose Serverless, Standard version {SESSION_ENVIRONMENT}.")
    print("Run each notebook from the top. The projects and data will be reused.")
    print(
        f"\nCopy back: Lakebase READY {KIT_VERSION} | {user} | {w.config.host} | "
        f"Branching PASSED | Search PASSED | starting data VERIFIED | environment {SESSION_ENVIRONMENT} | run {run_id}"
    )
    return result


print(f"Preparation steps loaded. Release {KIT_VERSION}; serverless environment {SESSION_ENVIRONMENT}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Download, run, and prepare both labs
# MAGIC
# MAGIC **What:** download the verified files, run both labs as you, check their results and starting data.
# MAGIC
# MAGIC **Why:** I'd rather find a setup or permission problem now than with everyone watching. The job links show progress and any error.
# MAGIC
# MAGIC **Look for:** READY, both notebook links, and the line to copy back to us. The projects stay for the session. If you see NOT READY, send the error and the job link to your contact.

# COMMAND ----------

# DBTITLE 1,3. Prepare both labs
READY_RESULT = None
READY_RESULT = prepare_session()
