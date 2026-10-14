# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Setup (shared)
# MAGIC This notebook is `%run` by each lab to provide common utilities.
# MAGIC **Do not run this notebook directly.**

# COMMAND ----------

import hashlib
import os
import re
import time
import psycopg
from psycopg.rows import dict_row
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
user_email = w.current_user.me().user_name

def _sanitize(email):
    name = email.split("@")[0]
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", name.lower())).strip("-")

PROJECT_ID = f"lakebase-lab-{_sanitize(user_email)}"
PG_SCHEMA  = f"lakebase_lab_{_sanitize(user_email).replace('-', '_')}"


def _retail_search_identity(email):
    """Return one bounded, workspace-unique identity for the standalone lab."""
    canonical = email.strip().casefold()
    slug = _sanitize(canonical)[:23].rstrip("-") or "user"
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return slug, fingerprint


def retail_search_project_id_for(email):
    slug, fingerprint = _retail_search_identity(email)
    return f"lakebase-retail-search-{slug}-{fingerprint}"


def retail_search_schema_for(email):
    slug, fingerprint = _retail_search_identity(email)
    return f"retail_search_{slug.replace('-', '_')}_{fingerprint}"


RETAIL_SEARCH_PROJECT_ID = retail_search_project_id_for(user_email)
RETAIL_SEARCH_PG_SCHEMA = retail_search_schema_for(user_email)

_REQUIRED_TABLES = {"products", "events", "agent_sessions", "agent_messages", "agent_memory_store", "audit_log"}

# An endpoint is usable once it leaves INIT. IDLE means it scaled to zero, which is
# the normal resting state for a non-production branch and is not something to wait
# out: an idle endpoint only wakes when a client connects, so waiting for ACTIVE here
# would block until the timeout and then fail a branch that is perfectly healthy.
_READY_ENDPOINT_STATES = ("ACTIVE", "IDLE", "DEGRADED")


def wait_for_endpoint(branch="production", max_attempts=60, delay=5):
    """Poll until the branch's primary endpoint is past provisioning, then return it.

    Newly created branches (dev, checkpoint, recovery) need a few seconds to a few
    minutes for their compute to spin up. Poll for readiness instead of guessing
    with a fixed sleep, which can flake on slower endpoint creation."""
    for attempt in range(max_attempts):
        try:
            endpoints = list(w.postgres.list_endpoints(
                parent=f"projects/{PROJECT_ID}/branches/{branch}"
            ))
            if endpoints:
                ep = w.postgres.get_endpoint(name=endpoints[0].name)
                state = str(getattr(ep.status, "current_state", "")).upper()
                if any(ready in state for ready in _READY_ENDPOINT_STATES):
                    return ep
        except Exception:
            pass
        time.sleep(delay)
    raise TimeoutError(
        f"Endpoint for branch '{branch}' is still provisioning after "
        f"{max_attempts * delay // 60} minutes. Check the Lakebase UI."
    )

def get_connection(branch="production", connect_retries=3):
    """Connect to a Lakebase branch. Returns a psycopg connection with dict_row.

    Uses Databricks SDK OAuth (`generate_database_credential`) + sslmode=require —
    the recommended path for notebooks and apps that can mint short-lived tokens.
    For interactive clients (psql/pgAdmin/DBeaver), prefer a native Postgres password
    role from the Lakebase App Connect dialog instead.

    Sets search_path to PG_SCHEMA so table references don't need schema qualifiers.
    Retries the connect a few times: a scale-to-zero branch may take a moment to
    wake on the first connection (non-production branches suspend when idle)."""
    endpoints = list(w.postgres.list_endpoints(
        parent=f"projects/{PROJECT_ID}/branches/{branch}"
    ))
    ep = w.postgres.get_endpoint(name=endpoints[0].name)
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
    # connect_timeout matters as much as the retry: without it a handshake against
    # a waking endpoint can hang instead of raising, which takes the kernel with it.
    params = {"host": host, "dbname": "databricks_postgres",
              "user": user_email, "password": cred.token, "sslmode": "require",
              "connect_timeout": 15,
              "options": f"-c search_path={PG_SCHEMA},public"}
    last_err = None
    for attempt in range(max(1, connect_retries)):
        try:
            conn = psycopg.connect(**params, row_factory=dict_row)
            _ensure_schema(conn, branch)
            return conn
        except psycopg.OperationalError as e:
            last_err = e
            time.sleep(3)  # brief pause for scale-to-zero wake / transient network
    raise last_err


def prepare_retail_search_lab():
    """Create or resume the dedicated project, enable Search, and validate it.

    This is the one workshop path that does not depend on the foundation
    notebook. It creates a PostgreSQL 17 project for the current user when the
    exact project ID is missing, enables Lakebase Search on that dedicated
    project, creates only the participant schema, and then runs the functional
    readiness probe. Partial progress is deliberately kept so rerunning this
    cell resumes instead of creating a second project.
    """
    from databricks.sdk.errors import (
        AlreadyExists,
        NotFound,
        PermissionDenied,
        ResourceAlreadyExists,
        ResourceConflict,
    )
    from psycopg import sql as pg_sql

    project_id = RETAIL_SEARCH_PROJECT_ID
    pg_schema = RETAIL_SEARCH_PG_SCHEMA
    expected_project_name = f"projects/{project_id}"
    expected_branch_name = f"{expected_project_name}/branches/production"
    expected_endpoint_name = f"{expected_branch_name}/endpoints/primary"
    identity_suffix = project_id.removeprefix("lakebase-retail-search-")
    expected_display_name = f"Lakebase Workshop: Retail Search: {identity_suffix}"
    expected_tags = {
        ("purpose", "lakebase-workshop"),
        ("lab", "retail-search-context"),
    }
    required_preloads = {"lakebase_vector", "lakebase_text"}
    bootstrap_conn = None

    class PreparationError(RuntimeError):
        pass

    class OperationStillRunning(PreparationError):
        pass

    def brief_error(exc):
        code = getattr(exc, "error_code", None) or getattr(exc, "status_code", None)
        lines = str(exc).splitlines()
        first_line = (lines[0] if lines else type(exc).__name__)[:240]
        return f"{code}: {first_line}" if code else f"{type(exc).__name__}: {first_line}"

    def postgres_headers():
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        workspace_id = getattr(w.config, "workspace_id", None)
        if workspace_id:
            headers["X-Databricks-Workspace-Id"] = str(workspace_id)
        return headers

    def raw_get_project():
        return w.api_client.do(
            "GET",
            f"/api/2.0/postgres/{expected_project_name}",
            headers=postgres_headers(),
        )

    def wait_for_raw_project(timeout_seconds=120):
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                return raw_get_project()
            except NotFound:
                time.sleep(3)
        raise PreparationError(
            f"{project_id} was created but did not become visible in time. "
            "The project was kept; rerun this cell."
        )

    def wait_for_operation(started, label, timeout_seconds):
        operation_name = (started or {}).get("name")
        expected_prefix = f"{expected_project_name}/operations/"
        if not operation_name or not operation_name.startswith(expected_prefix):
            raise PreparationError(
                f"{label} returned an unexpected operation name: {operation_name!r}"
            )

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            operation = w.postgres.get_operation(name=operation_name)
            if operation.done:
                if operation.error:
                    code = getattr(operation.error, "error_code", None)
                    message = getattr(operation.error, "message", None)
                    detail = f"{code}: {message}" if code else str(operation.error)
                    raise PreparationError(f"{label} failed: {detail}")
                if operation.response is None:
                    raise PreparationError(
                        f"{label} completed without a project response"
                    )
                response_name = operation.response.get("name")
                if response_name != expected_project_name:
                    raise PreparationError(
                        f"{label} completed for {response_name!r}, not "
                        f"{expected_project_name!r}"
                    )
                return operation.response
            time.sleep(5)

        raise OperationStillRunning(
            f"{label} is still running after {timeout_seconds // 60} minutes. "
            "The project was kept. Wait a few minutes, then rerun this cell."
        )

    def project_value(project, key):
        status = project.get("status") or {}
        spec = project.get("spec") or {}
        return status.get(key) if status.get(key) is not None else spec.get(key)

    def project_tags(project):
        tags = []
        for source in (project.get("status") or {}, project.get("spec") or {}):
            for tag in source.get("custom_tags") or []:
                if isinstance(tag, dict):
                    tags.append((tag.get("key"), tag.get("value")))
        return set(tags)

    def cu_value(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def duration_seconds(value):
        if value is None:
            return None
        if hasattr(value, "total_seconds"):
            return float(value.total_seconds())
        seconds = getattr(value, "seconds", None)
        if seconds is not None:
            try:
                nanos = float(getattr(value, "nanos", 0) or 0)
                return float(seconds) + nanos / 1_000_000_000
            except (TypeError, ValueError):
                return None
        if isinstance(value, dict):
            try:
                return float(value.get("seconds", 0)) + float(
                    value.get("nanos", 0)
                ) / 1_000_000_000
            except (TypeError, ValueError):
                return None
        text = str(value).strip().lower()
        if text.endswith("s"):
            text = text[:-1]
        try:
            return float(text)
        except ValueError:
            return None

    def validate_project_safety(project):
        resource_name = project.get("name")
        resource_id = project.get("project_id")
        owner = project_value(project, "owner")
        display_name = project_value(project, "display_name")
        pg_version = project_value(project, "pg_version")
        tags = project_tags(project)

        if resource_name != expected_project_name:
            raise PreparationError(
                f"refusing to use unexpected project resource {resource_name!r}"
            )
        if resource_id is not None and resource_id != project_id:
            raise PreparationError(
                f"refusing to use project ID {resource_id!r}; expected {project_id!r}"
            )
        if not owner or str(owner).casefold() != user_email.casefold():
            raise PreparationError(
                "refusing to enable Search because the exact project is not owned "
                f"by {user_email}. Search is permanent and restarts project computes."
            )
        required_tags_match = all(
            {value for tag_key, value in tags if tag_key == key} == {expected_value}
            for key, expected_value in expected_tags
        )
        if display_name != expected_display_name or not required_tags_match:
            raise PreparationError(
                "refusing to enable Search because this is not verified as the "
                "current user's dedicated retail Search project. The exact display "
                "name and required lab tags must all match."
            )
        try:
            pg_major = int(pg_version)
        except (TypeError, ValueError):
            raise PreparationError(
                f"project PostgreSQL version is missing or invalid: {pg_version!r}"
            )
        if pg_major != 17:
            raise PreparationError(
                f"this lab requires its dedicated PostgreSQL 17 project; found {pg_major}"
            )

        settings = project_value(project, "default_endpoint_settings") or {}
        min_cu = cu_value(settings.get("autoscaling_limit_min_cu"))
        max_cu = cu_value(settings.get("autoscaling_limit_max_cu"))
        suspend_seconds = duration_seconds(settings.get("suspend_timeout_duration"))
        if min_cu != 1.0 or max_cu != 1.0:
            raise PreparationError(
                "the existing project is not the expected 1/1 CU dedicated lab "
                "project. No project settings were changed."
            )
        if settings.get("no_suspension") is True or not suspend_seconds or suspend_seconds <= 0:
            raise PreparationError(
                "the existing project does not have scale-to-zero configured. "
                "No project settings were changed."
            )

        return pg_major

    def wait_for_exact_branch(timeout_seconds=300):
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                branch = w.postgres.get_branch(name=expected_branch_name)
            except NotFound:
                time.sleep(5)
                continue
            except PermissionDenied as exc:
                raise PreparationError(
                    f"{user_email} cannot access the exact production branch"
                ) from exc
            if branch.name != expected_branch_name:
                raise PreparationError(
                    f"branch lookup returned {branch.name!r}, not {expected_branch_name!r}"
                )
            return branch
        raise PreparationError("the exact production branch was not created in time")

    def wait_for_exact_endpoint(timeout_seconds=600):
        deadline = time.monotonic() + timeout_seconds
        last_state = "NOT_FOUND"
        while time.monotonic() < deadline:
            try:
                endpoint = w.postgres.get_endpoint(name=expected_endpoint_name)
            except NotFound:
                time.sleep(5)
                continue
            except PermissionDenied as exc:
                raise PreparationError(
                    f"{user_email} cannot access {expected_endpoint_name}"
                ) from exc

            if endpoint.name != expected_endpoint_name:
                raise PreparationError(
                    f"endpoint lookup returned {endpoint.name!r}, not the exact primary endpoint"
                )
            last_state = str(
                getattr(endpoint.status, "current_state", "UNKNOWN")
            ).split(".")[-1].upper()
            host = getattr(getattr(endpoint.status, "hosts", None), "host", None)
            if last_state in {"ACTIVE", "IDLE"} and host:
                min_cu = cu_value(
                    getattr(endpoint.status, "autoscaling_limit_min_cu", None)
                )
                max_cu = cu_value(
                    getattr(endpoint.status, "autoscaling_limit_max_cu", None)
                )
                suspend_seconds = duration_seconds(
                    getattr(endpoint.status, "suspend_timeout_duration", None)
                )
                if min_cu != 1.0 or max_cu != 1.0:
                    raise PreparationError(
                        f"production/primary is {min_cu}/{max_cu} CU, not the "
                        "expected 1/1 CU"
                    )
                if not suspend_seconds or suspend_seconds <= 0:
                    raise PreparationError(
                        "production/primary does not have scale-to-zero configured"
                    )
                return endpoint, last_state
            time.sleep(5)

        raise PreparationError(
            f"production/primary did not reach ACTIVE or IDLE; last state was {last_state}"
        )

    def connect_fresh(timeout_seconds=180):
        deadline = time.monotonic() + timeout_seconds
        last_error = None
        while time.monotonic() < deadline:
            endpoint, _ = wait_for_exact_endpoint(timeout_seconds=60)
            try:
                credential = w.postgres.generate_database_credential(
                    endpoint=expected_endpoint_name
                )
                if not getattr(credential, "token", None):
                    raise RuntimeError("credential response did not include a token")
                return psycopg.connect(
                    host=endpoint.status.hosts.host,
                    dbname="databricks_postgres",
                    user=user_email,
                    password=credential.token,
                    sslmode="require",
                    connect_timeout=15,
                    autocommit=True,
                    row_factory=dict_row,
                    options=f"-c search_path={pg_schema},public",
                )
            except psycopg.OperationalError as exc:
                last_error = exc
                time.sleep(5)
        raise PreparationError(
            "OAuth and TLS could not connect to production/primary: "
            + (brief_error(last_error) if last_error else "connection timed out")
        )

    def read_preloads(conn):
        with conn.cursor() as cur:
            cur.execute("SHOW shared_preload_libraries")
            preload_value = cur.fetchone()["shared_preload_libraries"] or ""
        return {
            item.strip().strip("'\"").rsplit("/", 1)[-1].lower()
            for item in preload_value.split(",")
            if item.strip()
        }

    def wait_for_search_connection(timeout_seconds=600):
        deadline = time.monotonic() + timeout_seconds
        missing = required_preloads
        while time.monotonic() < deadline:
            remaining = max(1, int(deadline - time.monotonic()))
            wait_for_exact_endpoint(timeout_seconds=remaining)
            candidate = connect_fresh(timeout_seconds=min(180, remaining))
            keep_candidate = False
            try:
                missing = required_preloads - read_preloads(candidate)
                if not missing:
                    keep_candidate = True
                    return candidate
            except (psycopg.OperationalError, psycopg.InterfaceError):
                pass
            finally:
                if not keep_candidate:
                    candidate.close()
            time.sleep(5)

        detail = "missing preload: " + ", ".join(sorted(missing))
        raise PreparationError(
            "Search enablement or the compute restart is still settling. "
            f"{detail}. The project was kept; rerun this cell."
        )

    try:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", project_id):
            raise PreparationError(
                f"derived project ID {project_id!r} is invalid or longer than 63 characters"
            )
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", pg_schema):
            raise PreparationError(
                f"derived schema {pg_schema!r} is invalid or longer than 63 characters"
            )

        created_here = False
        try:
            project = raw_get_project()
        except PermissionDenied as exc:
            raise PreparationError(
                f"{user_email} cannot inspect {project_id}. The cell will not treat "
                "a permission denial as a missing project."
            ) from exc
        except NotFound:
            print(f"Creating dedicated Lakebase project: {project_id}")
            payload = {
                "spec": {
                    "display_name": expected_display_name,
                    "pg_version": 17,
                    "custom_tags": [
                        {"key": key, "value": value}
                        for key, value in sorted(expected_tags)
                    ],
                    "default_endpoint_settings": {
                        "autoscaling_limit_min_cu": 1,
                        "autoscaling_limit_max_cu": 1,
                        "suspend_timeout_duration": "3600s",
                    },
                },
                "initial_endpoint_spec": {
                    "autoscaling_limit_min_cu": 1,
                    "autoscaling_limit_max_cu": 1,
                    "suspend_timeout_duration": "3600s",
                    "group": {
                        "min": 1,
                        "max": 1,
                        "enable_readable_secondaries": False,
                    },
                },
            }
            try:
                started = w.api_client.do(
                    "POST",
                    "/api/2.0/postgres/projects",
                    query={"project_id": project_id},
                    headers=postgres_headers(),
                    body=payload,
                )
                wait_for_operation(started, "project creation", timeout_seconds=600)
                created_here = True
            except OperationStillRunning:
                raise
            except PermissionDenied as exc:
                raise PreparationError(
                    "the workspace or current identity cannot create the per-user "
                    "Lakebase project. Project creation is required for this lab. "
                    "Ask a workspace admin to verify Lakebase availability and "
                    "workspace policy, then rerun this cell."
                ) from exc
            except (AlreadyExists, ResourceAlreadyExists, ResourceConflict):
                print("Project was created concurrently. Re-reading the exact project.")
            except PreparationError as exc:
                raise PreparationError(
                    "the workspace or current identity could not create the per-user "
                    "Lakebase project. Ask a workspace admin to verify Lakebase "
                    f"availability and workspace policy. Detail: {exc}"
                ) from exc
            except Exception as exc:
                raise PreparationError(
                    "the workspace or current identity could not create the per-user "
                    "Lakebase project. Ask a workspace admin to verify Lakebase "
                    f"availability and workspace policy. Detail: {brief_error(exc)}"
                ) from exc
            project = wait_for_raw_project()
        except Exception as exc:
            raise PreparationError(
                f"project lookup failed for {project_id}: {brief_error(exc)}"
            ) from exc

        pg_major = validate_project_safety(project)
        action = "created" if created_here else "reused"
        print(
            f"✓ Dedicated project: {action} {project_id} "
            f"(PostgreSQL {pg_major}, purpose=lakebase-workshop, "
            "lab=retail-search-context)"
        )

        wait_for_exact_branch()

        _endpoint, endpoint_state = wait_for_exact_endpoint()
        print(f"✓ Production endpoint: {endpoint_state} at 1/1 CU")

        bootstrap_conn = connect_fresh()
        with bootstrap_conn.cursor() as cur:
            cur.execute("SHOW server_version_num")
            server_version_num = int(cur.fetchone()["server_version_num"])
            cur.execute("SELECT current_user AS role")
            database_role = cur.fetchone()["role"]
        if not 170000 <= server_version_num < 180000:
            raise PreparationError(
                f"live PostgreSQL is {server_version_num}; this lab requires PostgreSQL 17"
            )
        if str(database_role).casefold() != user_email.casefold():
            raise PreparationError(
                f"OAuth connected as {database_role!r}, not {user_email!r}"
            )

        missing_preloads = required_preloads - read_preloads(bootstrap_conn)
        if missing_preloads:
            print(
                "Enabling Lakebase Search on this dedicated project. This cannot be "
                "undone, restarts every project compute, and drops active connections."
            )
            bootstrap_conn.close()
            bootstrap_conn = None
            started = None
            try:
                started = w.api_client.do(
                    "POST",
                    f"/api/2.0/postgres/{expected_project_name}/search-extensions",
                    headers=postgres_headers(),
                    body={},
                )
            except PermissionDenied as exc:
                raise PreparationError(
                    f"{user_email} cannot enable Search on the dedicated project"
                ) from exc
            except NotFound as exc:
                raise PreparationError(
                    "the Search enablement API is unavailable in this workspace. "
                    "Verify Lakebase Search availability, or enable it from the "
                    "project Settings page, then rerun this cell."
                ) from exc
            except (AlreadyExists, ResourceAlreadyExists, ResourceConflict):
                print("Search enablement is already running. Waiting for the restart.")

            if started is not None:
                wait_for_operation(started, "Search enablement", timeout_seconds=900)
            bootstrap_conn = wait_for_search_connection(timeout_seconds=600)
            print("✓ Lakebase Search: enabled and verified")
        else:
            print("✓ Lakebase Search: already enabled and verified")

        schema_created = False
        with bootstrap_conn.cursor() as cur:
            cur.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner "
                "FROM pg_namespace WHERE nspname = %s",
                (pg_schema,),
            )
            schema_row = cur.fetchone()
            schema_created = schema_row is None
            cur.execute(
                pg_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    pg_sql.Identifier(pg_schema)
                )
            )
            cur.execute(
                "SELECT pg_get_userbyid(nspowner) AS owner "
                "FROM pg_namespace WHERE nspname = %s",
                (pg_schema,),
            )
            schema_row = cur.fetchone()

            cur.execute(
                "SELECT current_user AS role, "
                "has_schema_privilege(current_user, %s, 'USAGE') AS can_use, "
                "has_schema_privilege(current_user, %s, 'CREATE') AS can_create",
                (pg_schema, pg_schema),
            )
            privileges = cur.fetchone()

        if not schema_row or str(schema_row["owner"]).casefold() != str(
            privileges["role"]
        ).casefold():
            raise PreparationError(
                f"participant schema {pg_schema} is not owned by the current database role"
            )
        if not privileges["can_use"] or not privileges["can_create"]:
            raise PreparationError(
                f"current role needs USAGE and CREATE on participant schema {pg_schema}"
            )
        schema_action = "created" if schema_created else "reused"
        print(f"✓ Participant schema: {schema_action} {pg_schema} (no foundation tables seeded)")

        bootstrap_conn.close()
        bootstrap_conn = None

    except PreparationError as exc:
        if bootstrap_conn is not None:
            bootstrap_conn.close()
        print(f"\n✗ NOT READY: {exc}")
        print(
            "Any project, Search, or schema state already created was kept. "
            "Fix the stated issue, then rerun this same cell."
        )
        raise RuntimeError(f"Retail Search preparation failed: {exc}") from exc
    except Exception as exc:
        if bootstrap_conn is not None:
            bootstrap_conn.close()
        detail = brief_error(exc)
        print(f"\n✗ NOT READY: unexpected preparation error: {detail}")
        print(
            "Any project, Search, or schema state already created was kept. "
            "Fix the stated issue, then rerun this same cell."
        )
        raise RuntimeError(f"Retail Search preparation failed: {detail}") from exc

    return _validate_retail_search_runtime(project_id, pg_schema)


def _validate_retail_search_runtime(project_id, pg_schema):
    """Fail fast unless the prepared retail Search project can run every exercise.

    This validation is read-only outside one rolled-back SQL transaction. It
    leaves no probe objects, rows, indexes, or extension-state changes behind.
    """
    import secrets

    from databricks.sdk.errors import NotFound, PermissionDenied
    from psycopg import sql as pg_sql

    readiness = []

    def passed(label, detail):
        readiness.append(("PASS", label, detail))

    def failed(label, detail):
        readiness.append(("FAIL", label, detail))

    def skipped(label, detail):
        readiness.append(("SKIP", label, detail))

    def brief_error(exc):
        code = getattr(exc, "error_code", None) or getattr(exc, "status_code", None)
        lines = str(exc).splitlines()
        first_line = (lines[0] if lines else type(exc).__name__)[:240]
        return f"{code}: {first_line}" if code else f"{type(exc).__name__}: {first_line}"

    project = None
    branch = None
    endpoint = None
    credential = None
    preflight_conn = None
    schema_ready = False
    schema_writable = False
    search_preloaded = False
    extensions_available = False

    passed("Identity", user_email)

    try:
        project = w.postgres.get_project(name=f"projects/{project_id}")
        passed("Project access", project_id)
    except PermissionDenied:
        failed(
            "Project access",
            f"{user_email} cannot access {project_id}. Ask the facilitator to grant access.",
        )
    except NotFound:
        failed(
            "Project access",
            f"{project_id} is missing after preparation. Rerun the prepare cell.",
        )
    except Exception as exc:
        failed("Project access", brief_error(exc))

    if project is not None:
        try:
            branch = w.postgres.get_branch(
                name=f"projects/{project_id}/branches/production"
            )
            passed("Production branch", branch.name)
        except Exception as exc:
            failed("Production branch", brief_error(exc))
    else:
        skipped("Production branch", "project check failed")

    if branch is not None:
        try:
            endpoint = w.postgres.get_endpoint(
                name=f"projects/{project_id}/branches/production/endpoints/primary"
            )
            endpoint_state = str(
                getattr(endpoint.status, "current_state", "UNKNOWN")
            ).split(".")[-1].upper()
            endpoint_host = getattr(getattr(endpoint.status, "hosts", None), "host", None)
            if endpoint_state in {"ACTIVE", "IDLE"} and endpoint_host:
                passed("Production primary endpoint", endpoint_state)
            else:
                failed(
                    "Production primary endpoint",
                    f"state is {endpoint_state}; wait for ACTIVE or IDLE before the session",
                )
                endpoint = None
        except Exception as exc:
            failed("Production primary endpoint", brief_error(exc))
    else:
        skipped("Production primary endpoint", "production branch check failed")

    if endpoint is not None:
        try:
            credential = w.postgres.generate_database_credential(endpoint=endpoint.name)
            if not getattr(credential, "token", None):
                raise RuntimeError("credential response did not include a token")
            passed("OAuth database credential", "generated for the current user")
        except Exception as exc:
            failed("OAuth database credential", brief_error(exc))

        if credential is not None:
            try:
                preflight_conn = psycopg.connect(
                    host=endpoint.status.hosts.host,
                    dbname="databricks_postgres",
                    user=user_email,
                    password=credential.token,
                    sslmode="require",
                    connect_timeout=15,
                    autocommit=True,
                    row_factory=dict_row,
                    options=f"-c search_path={pg_schema},public",
                )
                passed("PostgreSQL connection", "OAuth and TLS connection succeeded")
            except Exception as exc:
                failed("PostgreSQL connection", brief_error(exc))
        else:
            skipped("PostgreSQL connection", "credential check failed")
    else:
        skipped("OAuth database credential", "endpoint check failed")
        skipped("PostgreSQL connection", "endpoint check failed")

    if preflight_conn is not None:
        try:
            with preflight_conn.cursor() as cur:
                cur.execute("SHOW server_version_num")
                server_version_num = int(cur.fetchone()["server_version_num"])
            if 170000 <= server_version_num < 180000:
                passed("Live PostgreSQL version", str(server_version_num))
            else:
                failed(
                    "Live PostgreSQL version",
                    f"expected PostgreSQL 17, found {server_version_num}",
                )
        except Exception as exc:
            failed("Live PostgreSQL version", brief_error(exc))

        try:
            with preflight_conn.cursor() as cur:
                cur.execute("SHOW shared_preload_libraries")
                preload_value = cur.fetchone()["shared_preload_libraries"] or ""
            preloaded = {
                item.strip().strip("'\"").rsplit("/", 1)[-1].lower()
                for item in preload_value.split(",")
                if item.strip()
            }
            missing_preloads = {"lakebase_vector", "lakebase_text"} - preloaded
            if missing_preloads:
                failed(
                    "Lakebase Search enabled",
                    "missing preload: "
                    + ", ".join(sorted(missing_preloads))
                    + ". Preparation did not finish cleanly; rerun the same cell.",
                )
            else:
                search_preloaded = True
                passed(
                    "Lakebase Search enabled",
                    "lakebase_vector and lakebase_text are preloaded",
                )
        except Exception as exc:
            failed("Lakebase Search enabled", brief_error(exc))

        try:
            with preflight_conn.cursor() as cur:
                cur.execute(
                    "SELECT name, default_version FROM pg_available_extensions "
                    "WHERE name IN ('lakebase_vector', 'lakebase_text') ORDER BY name"
                )
                available = {row["name"]: row["default_version"] for row in cur.fetchall()}
            missing_extensions = {"lakebase_vector", "lakebase_text"} - set(available)
            if missing_extensions:
                failed(
                    "Search extensions available",
                    "missing: " + ", ".join(sorted(missing_extensions)),
                )
            else:
                extensions_available = True
                versions = ", ".join(
                    f"{name} {available[name]}" for name in sorted(available)
                )
                passed("Search extensions available", versions)
        except Exception as exc:
            failed("Search extensions available", brief_error(exc))

        try:
            with preflight_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_get_userbyid(nspowner) AS owner "
                    "FROM pg_namespace WHERE nspname = %s",
                    (pg_schema,),
                )
                schema_row = cur.fetchone()
                cur.execute("SELECT current_user AS role")
                current_role = cur.fetchone()["role"]
            if schema_row is None:
                failed("Participant schema", f"{pg_schema} does not exist")
            elif str(schema_row["owner"]).casefold() != str(current_role).casefold():
                failed(
                    "Participant schema",
                    f"{pg_schema} is owned by {schema_row['owner']}, not {current_role}",
                )
            else:
                schema_ready = True
                passed(
                    "Participant schema",
                    f"{pg_schema} exists and is owned by the current role",
                )
        except Exception as exc:
            failed("Participant schema", brief_error(exc))

        if schema_ready:
            try:
                lab_table_names = {
                    "retail_product_search",
                    "retail_store_product",
                }
                lab_index_names = {
                    "retail_product_search_ann",
                    "retail_product_search_bm25",
                    "retail_product_search_attributes",
                    "retail_store_product_eligibility",
                }
                with preflight_conn.cursor() as cur:
                    cur.execute(
                        "SELECT "
                        "has_schema_privilege(current_user, %s, 'USAGE') AS can_use, "
                        "has_schema_privilege(current_user, %s, 'CREATE') AS can_create, "
                        "current_user AS role",
                        (pg_schema, pg_schema),
                    )
                    privilege_row = cur.fetchone()
                    cur.execute(
                        "SELECT c.relname, c.relkind, "
                        "pg_get_userbyid(c.relowner) AS owner, "
                        "pg_has_role(current_user, c.relowner, 'USAGE') AS can_manage, "
                        "parent.relname AS indexed_table "
                        "FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "LEFT JOIN pg_index ix ON ix.indexrelid = c.oid "
                        "LEFT JOIN pg_class parent ON parent.oid = ix.indrelid "
                        "WHERE n.nspname = %s "
                        "AND c.relname IN ("
                        "'retail_product_search', 'retail_store_product', "
                        "'retail_product_search_ann', 'retail_product_search_bm25', "
                        "'retail_product_search_attributes', "
                        "'retail_store_product_eligibility')",
                        (pg_schema,),
                    )
                    existing_lab_objects = cur.fetchall()
                existing_lab_tables = [
                    row for row in existing_lab_objects
                    if row["relname"] in lab_table_names
                ]
                wrong_relation_kind = [
                    row["relname"]
                    for row in existing_lab_tables
                    if row["relkind"] not in {"r", "p"}
                ]
                unmanageable_tables = [
                    row["relname"]
                    for row in existing_lab_tables
                    if row["relkind"] in {"r", "p"} and not row["can_manage"]
                ]
                index_collisions = [
                    row["relname"]
                    for row in existing_lab_objects
                    if row["relname"] in lab_index_names
                    and not (
                        row["relkind"] in {"i", "I"}
                        and row["indexed_table"] in lab_table_names
                        and row["can_manage"]
                    )
                ]
                if not privilege_row["can_use"] or not privilege_row["can_create"]:
                    failed(
                        "Lab object permissions",
                        f"{privilege_row['role']} needs USAGE and CREATE on {pg_schema}",
                    )
                elif wrong_relation_kind:
                    failed(
                        "Lab object permissions",
                        "lab table names are occupied by the wrong object type: "
                        + ", ".join(sorted(wrong_relation_kind)),
                    )
                elif unmanageable_tables:
                    failed(
                        "Lab object permissions",
                        "current role cannot replace: "
                        + ", ".join(sorted(unmanageable_tables)),
                    )
                elif index_collisions:
                    failed(
                        "Lab object permissions",
                        "fixed index names collide with other objects: "
                        + ", ".join(sorted(index_collisions)),
                    )
                else:
                    schema_writable = True
                    detail = "schema is writable"
                    if existing_lab_tables:
                        detail += "; existing lab tables are replaceable by the current role"
                    passed("Lab object permissions", detail)
            except Exception as exc:
                failed("Lab object permissions", brief_error(exc))
        else:
            skipped("Lab object permissions", "participant schema check failed")

        if search_preloaded and extensions_available and schema_writable:
            preflight_conn.autocommit = False
            probe_suffix = secrets.token_hex(4)
            probe_table = f"_retail_search_readiness_{probe_suffix}"
            exact_index = f"{probe_table}_exact"
            ann_index = f"{probe_table}_ann"
            bm25_index = f"{probe_table}_bm25"
            probe_phase = "extension installation"
            probe_ok = False
            extensions_before = None
            try:
                with preflight_conn.cursor() as cur:
                    cur.execute("SELECT extname FROM pg_extension ORDER BY extname")
                    extensions_before = {row["extname"] for row in cur.fetchall()}
                    cur.execute(
                        pg_sql.SQL("SET LOCAL search_path TO {}, public").format(
                            pg_sql.Identifier(pg_schema)
                        )
                    )
                    cur.execute("CREATE EXTENSION IF NOT EXISTS lakebase_vector CASCADE")
                    cur.execute("CREATE EXTENSION IF NOT EXISTS lakebase_text")

                    probe_phase = "table creation"
                    cur.execute(
                        pg_sql.SQL(
                            "CREATE TABLE {}.{} ("
                            "id INTEGER PRIMARY KEY, sku TEXT NOT NULL, "
                            "embedding VECTOR(8) NOT NULL, search_tsv TSVECTOR NOT NULL)"
                        ).format(
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )
                    cur.execute(
                        pg_sql.SQL(
                            "INSERT INTO {}.{} (id, sku, embedding, search_tsv) VALUES "
                            "(1, 'READY-1', '[1,0,0,0,0,0,0,0]'::vector, "
                            "to_tsvector('simple', 'deck impact driver')), "
                            "(2, 'READY-2', '[0,1,0,0,0,0,0,0]'::vector, "
                            "to_tsvector('simple', 'garden hose'))"
                        ).format(
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )

                    probe_phase = "exact index"
                    cur.execute(
                        pg_sql.SQL("CREATE UNIQUE INDEX {} ON {}.{} (sku)").format(
                            pg_sql.Identifier(exact_index),
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )
                    probe_phase = "vector index"
                    cur.execute(
                        pg_sql.SQL(
                            "CREATE INDEX {} ON {}.{} USING lakebase_ann "
                            "(embedding vector_cosine_ops)"
                        ).format(
                            pg_sql.Identifier(ann_index),
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )
                    probe_phase = "BM25 index"
                    cur.execute(
                        pg_sql.SQL(
                            "CREATE INDEX {} ON {}.{} USING lakebase_bm25 (search_tsv)"
                        ).format(
                            pg_sql.Identifier(bm25_index),
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )

                    probe_phase = "exact query"
                    cur.execute(
                        pg_sql.SQL(
                            "SELECT sku FROM {}.{} WHERE sku = 'READY-1'"
                        ).format(
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )
                    if cur.fetchone()["sku"] != "READY-1":
                        raise RuntimeError("exact probe returned the wrong row")

                    probe_phase = "vector query"
                    cur.execute(
                        pg_sql.SQL(
                            "SELECT sku FROM {}.{} "
                            "ORDER BY embedding <=> '[1,0,0,0,0,0,0,0]'::vector LIMIT 1"
                        ).format(
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        )
                    )
                    if cur.fetchone()["sku"] != "READY-1":
                        raise RuntimeError("vector probe returned the wrong row")

                    probe_phase = "BM25 query"
                    cur.execute(
                        pg_sql.SQL(
                            "SELECT sku FROM {}.{} "
                            "ORDER BY search_tsv <@> to_bm25query("
                            "to_tsvector('simple', 'deck driver'), %s) LIMIT 1"
                        ).format(
                            pg_sql.Identifier(pg_schema),
                            pg_sql.Identifier(probe_table),
                        ),
                        (bm25_index,),
                    )
                    if cur.fetchone()["sku"] != "READY-1":
                        raise RuntimeError("BM25 probe returned the wrong row")
                    probe_ok = True
            except Exception as exc:
                if getattr(exc, "sqlstate", None) == "42501":
                    detail = f"permission denied during {probe_phase}"
                else:
                    detail = f"{probe_phase} failed: {brief_error(exc)}"
                failed("Transactional Search probe", detail)
            finally:
                preflight_conn.rollback()

            cleanup_ok = False
            try:
                with preflight_conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass(%s) AS probe_object",
                        (f"{pg_schema}.{probe_table}",),
                    )
                    probe_object = cur.fetchone()["probe_object"]
                    cur.execute("SELECT extname FROM pg_extension ORDER BY extname")
                    extensions_after = {row["extname"] for row in cur.fetchall()}
                preflight_conn.commit()
                cleanup_ok = (
                    probe_object is None
                    and extensions_before is not None
                    and extensions_after == extensions_before
                )
                if cleanup_ok:
                    passed(
                        "Preflight cleanup",
                        "probe table, indexes, rows, and extension state rolled back",
                    )
                else:
                    failed(
                        "Preflight cleanup",
                        "probe rollback did not restore the original database state",
                    )
            except Exception as exc:
                preflight_conn.rollback()
                failed("Preflight cleanup", brief_error(exc))

            if probe_ok and cleanup_ok:
                passed(
                    "Transactional Search probe",
                    "exact, vector, and BM25 paths succeeded",
                )
        else:
            blockers = []
            if not search_preloaded:
                blockers.append("Search preload")
            if not extensions_available:
                blockers.append("extension availability")
            if not schema_writable:
                blockers.append("schema permissions")
            skipped(
                "Transactional Search probe",
                "blocked by " + ", ".join(blockers),
            )
            skipped("Preflight cleanup", "no probe objects were created")

    else:
        for label in (
            "Live PostgreSQL version",
            "Lakebase Search enabled",
            "Search extensions available",
            "Participant schema",
            "Lab object permissions",
            "Transactional Search probe",
            "Preflight cleanup",
        ):
            skipped(label, "PostgreSQL connection check failed")

    print("\nLakebase Search lab readiness")
    for status, label, detail in readiness:
        marker = {"PASS": "✓", "FAIL": "✗", "SKIP": "○"}[status]
        print(f"{marker} {label}: {detail}")

    failed_checks = [label for status, label, _ in readiness if status == "FAIL"]
    if failed_checks:
        print(
            f"\n✗ NOT READY: {len(failed_checks)} check(s) failed. "
            "No fixture tables were created."
        )
        if preflight_conn is not None:
            preflight_conn.close()
        raise RuntimeError(
            "Lakebase Search lab readiness failed: " + ", ".join(failed_checks)
        )

    passed_checks = sum(status == "PASS" for status, _, _ in readiness)
    print(
        f"\n✓ READY: all {passed_checks} checks passed. "
        "The dedicated project is prepared, and no probe objects remain."
    )
    return preflight_conn


def connection_was_closed(conn, exc):
    """True when exc means the server closed conn, not that the SQL failed."""
    return isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)) and (
        conn.closed or conn.broken
    )


def reconnect_retail_search_lab(timeout_seconds=180):
    """Open a fresh connection to the participant's prepared retail Search project.

    When the project scales to zero, it closes open connections. Connecting
    again wakes the compute. Each attempt mints a fresh OAuth credential.
    Nothing in the project is created or changed.
    """
    endpoint_name = (
        f"projects/{RETAIL_SEARCH_PROJECT_ID}/branches/production/endpoints/primary"
    )
    deadline = time.monotonic() + timeout_seconds
    last_error = None
    while time.monotonic() < deadline:
        endpoint = w.postgres.get_endpoint(name=endpoint_name)
        credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
        try:
            return psycopg.connect(
                host=endpoint.status.hosts.host,
                dbname="databricks_postgres",
                user=user_email,
                password=credential.token,
                sslmode="require",
                connect_timeout=15,
                row_factory=dict_row,
                options=f"-c search_path={RETAIL_SEARCH_PG_SCHEMA},public",
            )
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(5)
    detail = (str(last_error).splitlines() or [""])[0][:200] if last_error else "timed out"
    raise RuntimeError(
        f"Could not reconnect to the retail Search project: {detail}. "
        "Rerun the Prepare and validate cell."
    )


def _find_seed_sql():
    """Locate bootstrap/seed.sql by walking up from this file or the calling notebook."""
    candidates = []
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        ws = f"/Workspace{nb_path}"
        # Calling notebook is labs/<lab>/<Notebook> — walk up 3 levels to project root
        candidates.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(ws))), "bootstrap", "seed.sql"))
        # _setup.py is labs/_setup — walk up 2 levels
        candidates.append(os.path.join(os.path.dirname(os.path.dirname(ws)), "bootstrap", "seed.sql"))
    except Exception:
        pass
    try:
        candidates.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bootstrap", "seed.sql"))
    except NameError:
        pass
    for p in candidates:
        if os.path.isfile(p):
            return p
    return None

_REPAIR_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    category VARCHAR(100),
    tags TEXT[],
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {schema}.events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    source VARCHAR(100),
    payload JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {schema}.agent_sessions (
    session_id VARCHAR(64) PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL,
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {schema}.agent_messages (
    message_id SERIAL PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL REFERENCES {schema}.agent_sessions(session_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {schema}.agent_memory_store (
    memory_id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    memory TEXT NOT NULL,
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, topic)
);

CREATE TABLE IF NOT EXISTS {schema}.audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id INTEGER,
    old_data JSONB,
    new_data JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE INDEX IF NOT EXISTS idx_events_type ON {schema}.events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_created ON {schema}.events(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_session ON {schema}.agent_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_memory_user ON {schema}.agent_memory_store(user_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON {schema}.products(category);
CREATE INDEX IF NOT EXISTS idx_products_tags ON {schema}.products USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_audit_table ON {schema}.audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_record ON {schema}.audit_log(record_id);
"""

def _ensure_schema(conn, branch):
    """Verify required tables exist; create any missing ones."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (PG_SCHEMA,),
        )
        existing = {r["table_name"] for r in cur.fetchall()}

    missing = _REQUIRED_TABLES - existing
    if not missing:
        return

    print(f"⚠ Missing tables in {PG_SCHEMA}: {missing}")
    print(f"  Creating missing tables...")

    seed_path = _find_seed_sql()
    if seed_path:
        with open(seed_path) as f:
            sql = f.read().replace("{schema}", PG_SCHEMA)
    else:
        sql = _REPAIR_SQL.format(schema=PG_SCHEMA)

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"✓ Schema {PG_SCHEMA} repaired — all tables now exist")

def get_endpoint_name(branch="production"):
    """Get the full endpoint resource name for a branch."""
    return f"projects/{PROJECT_ID}/branches/{branch}/endpoints/primary"

WORKSPACE_HOST = w.config.host.rstrip("/") if w.config.host else ""
APP_NAME = "lakebase-lab-console"
APP_URL = f"{WORKSPACE_HOST}/apps/{APP_NAME}" if WORKSPACE_HOST else ""

def show_app_link(page_id, label=None):
    """Render a banner linking to the corresponding Lab Console app page."""
    if not APP_URL:
        return
    url = f"{APP_URL}#{page_id}"
    title = label or page_id.replace("-", " ").title()
    displayHTML(f"""
    <div style="padding:10px 16px;margin:8px 0;border-radius:8px;background:#e8f0fe;border:1px solid #aecbfa;display:flex;align-items:center;gap:12px;font-family:Inter,sans-serif">
      <span style="font-size:20px">🖥️</span>
      <div style="flex:1">
        <strong style="color:#1a73e8">Try it in the Lab Console</strong>
        <span style="color:#3c4043;margin-left:8px">This lab is also available as an interactive UI.</span>
      </div>
      <a href="{url}" target="_blank" style="background:#1a73e8;color:#fff;padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none">
        Open {title} →
      </a>
    </div>
    """)

# ---------------------------------------------------------------------------
# Databricks UI deep links
#
# Labs create durable artifacts (UC tables, Lakebase projects/branches/tables,
# sync pipelines). These helpers build per-user, per-workspace URLs so a
# participant can click straight through to what they just created. Everything
# resolves dynamically from the current identity/workspace — nothing hardcoded.
# ---------------------------------------------------------------------------

# The Lakebase UI addresses projects/branches by their system-generated UID
# (e.g. projects/5fd496e7-… / branches/br-round-tree-…), not the human-readable
# resource id the SDK uses in resource paths. Resolve + cache the UIDs once.
_project_uid_cache = {}
_branch_uid_cache = {}


def _workspace_org_id():
    """Resolve the numeric workspace/org id used by the `?o=` deep-link param.

    Works on all clouds: AWS/GCP hosts (e.g. e2-demo-field-eng.cloud.databricks.com)
    don't embed the id, so we ask the SDK (`get_workspace_id`). Azure hosts embed it
    as `adb-<digits>`, used as a fallback if the API call is unavailable.
    """
    try:
        wid = w.get_workspace_id()
        if wid:
            return str(wid)
    except Exception:
        pass
    if WORKSPACE_HOST:
        m = re.search(r"adb-(\d+)\.", WORKSPACE_HOST)
        if m:
            return m.group(1)
    return None


_ORG_ID = _workspace_org_id()


def _with_org(url):
    """Append the Azure `?o=<org>` param when we can derive it."""
    if _ORG_ID:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}o={_ORG_ID}"
    return url


def uc_table_url(catalog, schema, table):
    """Catalog Explorer deep link for a Unity Catalog table."""
    if not WORKSPACE_HOST:
        return ""
    return _with_org(f"{WORKSPACE_HOST}/explore/data/{catalog}/{schema}/{table}")


def _project_uid(project_id=None):
    """Resolve (and cache) the system UID for a Lakebase project."""
    project_id = project_id or PROJECT_ID
    if project_id in _project_uid_cache:
        return _project_uid_cache[project_id]
    uid = None
    try:
        proj = w.postgres.get_project(name=f"projects/{project_id}")
        uid = getattr(proj, "uid", None)
    except Exception:
        uid = None
    _project_uid_cache[project_id] = uid
    return uid


def _branch_uid(branch="production", project_id=None):
    """Resolve (and cache) the system UID for a Lakebase branch."""
    project_id = project_id or PROJECT_ID
    key = f"{project_id}/{branch}"
    if key in _branch_uid_cache:
        return _branch_uid_cache[key]
    uid = None
    try:
        b = w.postgres.get_branch(name=f"projects/{project_id}/branches/{branch}")
        uid = getattr(b, "uid", None)
    except Exception:
        uid = None
    _branch_uid_cache[key] = uid
    return uid


def lakebase_project_url(branch=None, project_id=None):
    """Lakebase project UI link, optionally focused on a branch.

    Falls back to the human-readable project id if the UID lookup fails."""
    if not WORKSPACE_HOST:
        return ""
    project_id = project_id or PROJECT_ID
    puid = _project_uid(project_id) or project_id
    url = f"{WORKSPACE_HOST}/lakebase/projects/{puid}"
    if branch:
        url = f"{url}?branchId={branch}"
    return url


def lakebase_tables_url(branch="production", project_id=None):
    """Lakebase Tables UI link for a branch.

    Prefers the UID path (/projects/{uid}/branches/{uid}/tables). If a UID can't
    be resolved, falls back to the project URL focused on the branch."""
    if not WORKSPACE_HOST:
        return ""
    project_id = project_id or PROJECT_ID
    puid = _project_uid(project_id)
    buid = _branch_uid(branch, project_id)
    if puid and buid:
        return f"{WORKSPACE_HOST}/lakebase/projects/{puid}/branches/{buid}/tables"
    return lakebase_project_url(branch=branch, project_id=project_id)


def pipeline_url(pipeline_id):
    """Lakeflow/DLT pipeline UI link for a sync or publish pipeline."""
    if not WORKSPACE_HOST or not pipeline_id:
        return ""
    return f"{WORKSPACE_HOST}/pipelines/{pipeline_id}"


def show_view_link(label, url):
    """Render a clickable 'View … →' banner linking to a Databricks UI page.

    Falls back to a plain-text print when displayHTML isn't available (e.g.
    running outside a notebook)."""
    if not url:
        return
    try:
        displayHTML(f"""
    <div style="padding:10px 16px;margin:8px 0;border-radius:8px;background:#e6f4ea;border:1px solid #a8dab5;display:flex;align-items:center;gap:12px;font-family:Inter,sans-serif">
      <div style="flex:1;color:#137333;font-weight:600">{label}</div>
      <a href="{url}" target="_blank" style="background:#137333;color:#fff;padding:6px 16px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none">
        View →
      </a>
    </div>
    """)
    except Exception:
        print(f"{label}: {url}")

print(f"Shared-lab project:          {PROJECT_ID}")
print(f"Shared-lab schema:           {PG_SCHEMA}")
print(f"Standalone Search project:   {RETAIL_SEARCH_PROJECT_ID}")
print(f"Standalone Search schema:    {RETAIL_SEARCH_PG_SCHEMA}")
print(f"User:                        {user_email}")
if APP_URL:
    print(f"Lab App: {APP_URL}")
