from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import lab_manifest as manifest  # noqa: E402
import harness_common as h  # noqa: E402
import reset_lab_state  # noqa: E402
import run_labs_live  # noqa: E402
import validate_all  # noqa: E402


def args(*, only=None, skip=None, from_lab=None):
    return SimpleNamespace(only=only, skip=skip, from_lab=from_lab)


class FakeCtx:
    def placeholders(self):
        return {
            "schema": "lakebase_lab_test",
            "catalog": "main",
            "uc_schema": "lakebase_lab_test",
            "fed_catalog": "lb_fed_test",
            "project": "lakebase-lab-test",
            "user": "test@example.com",
        }


class StandaloneRoutingTests(unittest.TestCase):
    def base_ctx(self):
        return h.Ctx(
            w=object(),
            profile="TEST",
            user_email="test@example.com",
            project_id="local-project-override",
            pg_schema="lakebase_lab_test",
            uc_catalog="main",
            uc_schema="lakebase_lab_test",
            app_name="lakebase-lab-console",
            host="https://example.cloud.databricks.com",
            bundle_target="dev",
        )

    def test_retail_identity_is_bounded_and_domain_safe(self):
        email = " Very.Long.User-Name_With+Symbols@example.COM "
        self.assertEqual(
            h.retail_search_project_id_for(email),
            "lakebase-retail-search-very-long-user-name-wit-dd17f52e65bf743c",
        )
        self.assertEqual(
            h.retail_search_pg_schema_for(email),
            "retail_search_very_long_user_name_wit_dd17f52e65bf743c",
        )
        self.assertLessEqual(len(h.retail_search_project_id_for(email)), 63)
        self.assertNotEqual(
            h.retail_search_project_id_for("user@example.com"),
            h.retail_search_project_id_for("user@other.example"),
        )

    def test_retail_context_ignores_base_project_override(self):
        base = self.base_ctx()
        routed = h.with_retail_search_project(base)
        self.assertEqual(
            routed.project_id,
            h.retail_search_project_id_for(base.user_email),
        )
        self.assertEqual(
            routed.pg_schema,
            h.retail_search_pg_schema_for(base.user_email),
        )
        self.assertEqual(routed.uc_schema, base.uc_schema)
        self.assertEqual(base.project_id, "local-project-override")

    def test_default_and_explicit_live_selection(self):
        default = run_labs_live.select_labs(args())
        self.assertTrue(default)
        self.assertTrue(all(not lab.standalone for lab in default))

        explicit = run_labs_live.select_labs(
            args(only="retail_search_context")
        )
        self.assertEqual([lab.id for lab in explicit], ["retail_search_context"])

    def test_mixed_and_from_selection_are_rejected(self):
        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as mixed:
            run_labs_live.select_labs(
                args(only="setup,retail_search_context")
            )
        self.assertEqual(mixed.exception.code, 2)

        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as from_lab:
            run_labs_live.select_labs(
                args(from_lab="retail_search_context")
            )
        self.assertEqual(from_lab.exception.code, 2)

        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as redundant:
            run_labs_live.select_labs(
                args(
                    only="retail_search_context",
                    from_lab="retail_search_context",
                )
            )
        self.assertEqual(redundant.exception.code, 2)

    def test_default_reset_excludes_standalone(self):
        actions = reset_lab_state.planned_actions(FakeCtx())
        self.assertNotIn(("pg_table", "retail_product_search"), actions)
        self.assertNotIn(("pg_table", "retail_store_product"), actions)

    def test_explicit_standalone_reset_is_selectable(self):
        actions = reset_lab_state.planned_actions(
            FakeCtx(), "retail_search_context"
        )
        self.assertIn(("pg_table", "retail_product_search"), actions)
        self.assertIn(("pg_table", "retail_store_product"), actions)

    def test_validate_all_scopes_standalone_only(self):
        calls = []

        def fake_stage(_title, argv):
            calls.append(argv)
            return 0, 0.0

        argv = [
            "validate_all.py",
            "--skip-static",
            "--only",
            "retail_search_context",
        ]
        with (
            patch.object(validate_all, "stage", side_effect=fake_stage),
            patch.object(validate_all, "latest", return_value=None),
            patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(validate_all.main(), 0)

        reset = [call for call in calls if "reset_lab_state.py" in call[0]]
        live = [call for call in calls if "run_labs_live.py" in call[0]]
        app = [call for call in calls if "validate_app.py" in call[0]]
        self.assertEqual(len(reset), 1)
        self.assertEqual(reset[0][-2:], ["--only", "retail_search_context"])
        self.assertEqual(len(live), 1)
        self.assertIn("retail_search_context", live[0])
        self.assertEqual(app, [])

    def test_validate_all_keeps_shared_app_validation(self):
        calls = []

        def fake_stage(_title, argv):
            calls.append(argv)
            return 0, 0.0

        argv = [
            "validate_all.py",
            "--skip-static",
            "--skip-reset",
            "--only",
            "setup",
        ]
        with (
            patch.object(validate_all, "stage", side_effect=fake_stage),
            patch.object(validate_all, "latest", return_value=None),
            patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(validate_all.main(), 0)

        app = [call for call in calls if "validate_app.py" in call[0]]
        self.assertEqual(app, [["scripts/validate_app.py"]])

    def test_live_preflight_uses_retail_context(self):
        seen = []
        pf = SimpleNamespace(facts={}, gates={}, warnings=[], failures=[])
        argv = [
            "run_labs_live.py",
            "--only",
            "retail_search_context",
            "--preflight-only",
        ]
        with (
            patch.object(run_labs_live.h, "require_sdk"),
            patch.object(run_labs_live.h, "build_ctx", return_value=self.base_ctx()),
            patch.object(
                run_labs_live.h,
                "run_preflight",
                side_effect=lambda ctx: seen.append(ctx) or pf,
            ),
            patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(run_labs_live.main(), 0)

        self.assertEqual(len(seen), 1)
        self.assertEqual(
            seen[0].project_id,
            h.retail_search_project_id_for("test@example.com"),
        )
        self.assertEqual(
            seen[0].pg_schema,
            h.retail_search_pg_schema_for("test@example.com"),
        )

    def test_explicit_reset_uses_retail_context(self):
        seen = []
        argv = [
            "reset_lab_state.py",
            "--only",
            "retail_search_context",
            "--dry-run",
        ]
        with (
            patch.object(reset_lab_state.h, "require_sdk"),
            patch.object(reset_lab_state.h, "build_ctx", return_value=self.base_ctx()),
            patch.object(
                reset_lab_state,
                "planned_actions",
                side_effect=lambda ctx, _only: seen.append(ctx) or [],
            ),
            patch.object(reset_lab_state.h, "write_report"),
            patch.object(sys, "argv", argv),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(reset_lab_state.main(), 0)

        self.assertEqual(len(seen), 1)
        self.assertEqual(
            seen[0].project_id,
            h.retail_search_project_id_for("test@example.com"),
        )
        self.assertEqual(
            seen[0].pg_schema,
            h.retail_search_pg_schema_for("test@example.com"),
        )

    def test_retail_project_contract_checks_sdk_state(self):
        ctx = h.with_retail_search_project(self.base_ctx())
        duration = SimpleNamespace(seconds=600, nanos=0)
        settings = SimpleNamespace(
            autoscaling_limit_min_cu=1.0,
            autoscaling_limit_max_cu=1.0,
            no_suspension=False,
            suspend_timeout_duration=duration,
        )
        tags = [
            SimpleNamespace(key="purpose", value="lakebase-workshop"),
            SimpleNamespace(key="lab", value="retail-search-context"),
        ]
        status = SimpleNamespace(
            owner=ctx.user_email,
            project_id=ctx.project_id,
            pg_version=17,
            display_name=(
                "Lakebase Workshop: Retail Search: "
                + ctx.project_id.removeprefix("lakebase-retail-search-")
            ),
            custom_tags=tags,
            default_endpoint_settings=settings,
        )
        project = SimpleNamespace(
            name=ctx.project_name(),
            project_id=ctx.project_id,
            status=status,
            spec=None,
        )
        endpoint_name = f"{ctx.branch_name('production')}/endpoints/primary"
        endpoint = SimpleNamespace(
            name=endpoint_name,
            status=SimpleNamespace(
                autoscaling_limit_min_cu=1.0,
                autoscaling_limit_max_cu=1.0,
                suspend_timeout_duration=duration,
            ),
        )
        postgres = SimpleNamespace(
            get_project=lambda **_kwargs: project,
            get_endpoint=lambda **_kwargs: endpoint,
        )
        ctx = replace(ctx, w=SimpleNamespace(postgres=postgres))

        ok, detail = run_labs_live._check_retail_project_contract(ctx, "")
        self.assertTrue(ok, detail)

        status.owner = "someone-else@example.com"
        ok, detail = run_labs_live._check_retail_project_contract(ctx, "")
        self.assertFalse(ok)
        self.assertIn("owner is", detail)

        status.owner = ctx.user_email
        tags.append(SimpleNamespace(key="purpose", value="other-purpose"))
        ok, detail = run_labs_live._check_retail_project_contract(ctx, "")
        self.assertFalse(ok)
        self.assertIn("conflicting values for tag purpose", detail)

        tags.pop()
        status.display_name = "wrong display"
        ok, detail = run_labs_live._check_retail_project_contract(ctx, "")
        self.assertFalse(ok)
        self.assertIn("display name is", detail)

    def test_manifest_and_lifecycle_invariants(self):
        self.assertEqual(manifest.validate_manifest(), [])
        self.assertEqual(
            [lab.id for lab in manifest.LABS if lab.standalone],
            ["retail_search_context"],
        )

        standalone_requires_shared = tuple(
            replace(lab, requires=("setup",))
            if lab.id == "retail_search_context"
            else lab
            for lab in manifest.LABS
        )
        self.assertTrue(
            any(
                "standalone lab cannot require shared lab" in problem
                for problem in manifest.validate_manifest(standalone_requires_shared)
            )
        )

        shared_requires_standalone = tuple(
            replace(
                lab,
                requires=lab.requires + ("retail_search_context",),
            )
            if lab.id == "branches"
            else lab
            for lab in manifest.LABS
        )
        self.assertTrue(
            any(
                "shared lab cannot require standalone lab" in problem
                for problem in manifest.validate_manifest(shared_requires_standalone)
            )
        )


if __name__ == "__main__":
    unittest.main()
