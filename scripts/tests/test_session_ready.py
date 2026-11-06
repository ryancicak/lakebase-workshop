"""Offline behavioral tests against the READY notebook's own functions.

No Databricks credentials, HTTP requests, or database connections are used.
Install databricks-sdk>=0.81.0 to run these tests; they skip when it is unavailable.
"""
import ast
import base64
import contextlib
import hashlib
import io
import json
import types
import unittest
from pathlib import Path

try:
    from databricks.sdk.service.compute import Environment as _Environment
    from databricks.sdk.service.jobs import JobEnvironment as _JobEnvironment, JobRunAs as _JobRunAs
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'labs/session-preflight/Session_Preflight.py'
USER = 'participant@example.test'
RUN_ID = '0123456789abcdef'


def obj(**kwargs):
    return types.SimpleNamespace(**kwargs)


def load_functions():
    cells = SOURCE.read_text().split('# COMMAND ----------')
    matches = [cell for cell in cells if 'def download_sources():' in cell]
    if len(matches) != 1:
        raise AssertionError('Expected one preparation definitions cell')
    namespace = {'__name__': 'ready_notebook_test'}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(matches[0], str(SOURCE), 'exec'), namespace)
    return namespace


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Workspace:
    def __init__(self, world):
        self.world = world
        self.objects = {}
        self.imports = []
        self.deletes = []
        self.mkdirs_calls = []

    def mkdirs(self, path):
        self.mkdirs_calls.append(path)

    def import_(self, **kwargs):
        self.imports.append(kwargs)
        if self.world.get('lock_collision') and kwargs['path'].endswith('/READY_in_progress'):
            raise RuntimeError('RESOURCE_ALREADY_EXISTS')
        if self.world.get('import_failure') and '/labs/' in kwargs['path']:
            raise RuntimeError('PERMISSION_DENIED')
        if kwargs['path'] in self.objects and not kwargs['overwrite']:
            raise RuntimeError('RESOURCE_ALREADY_EXISTS')
        self.objects[kwargs['path']] = base64.b64decode(kwargs['content'])

    def get_status(self, path):
        if self.world.get('missing_notebook') and path.endswith(self.world['missing_notebook']):
            raise RuntimeError('RESOURCE_DOES_NOT_EXIST')
        if path not in self.objects:
            raise RuntimeError('RESOURCE_DOES_NOT_EXIST')
        return obj(object_type=self.world.get('object_type', 'NOTEBOOK'), object_id=100 + len(path))

    def delete(self, **kwargs):
        self.deletes.append(kwargs)
        if self.world.get('delete_failure'):
            raise RuntimeError('PERMISSION_DENIED')
        self.objects.pop(kwargs['path'], None)


class Jobs:
    def __init__(self, namespace, world):
        self.n = namespace
        self.world = world
        self.submissions = []
        self.runs = {}
        self.output_ids = []
        self.cancelled = []

    def submit(self, **kwargs):
        self.submissions.append(kwargs)
        lab = kwargs['tasks'][0].task_key
        if self.world.get('submit_failure') == lab:
            raise TimeoutError('response lost after possible submission')
        parent = 100 + len(self.submissions)
        self.runs[parent] = kwargs
        return obj(run_id=None if self.world.get('missing_run_id') == lab else parent)

    def get_run(self, run_id):
        submission = self.runs[run_id]
        lab = submission['tasks'][0].task_key
        if self.world.get('poll_failure') == lab:
            raise ConnectionError('status unavailable')
        lifecycle = 'RUNNING' if self.world.get('timeout') == lab else 'TERMINATED'
        result = 'FAILED' if self.world.get('parent_failed') == lab else 'SUCCESS'
        task_result = 'FAILED' if self.world.get('task_failed') == lab else 'SUCCESS'
        tasks = [obj(task_key=lab, run_id=run_id + 1000, state=obj(result_state=task_result))]
        if self.world.get('missing_task') == lab:
            tasks = []
        if self.world.get('duplicate_task') == lab:
            tasks *= 2
        return obj(
            run_page_url=f'https://workspace.example.test/run/{run_id}',
            state=obj(life_cycle_state=lifecycle, result_state=result, state_message='injected failure'),
            tasks=tasks,
        )

    def get_run_output(self, run_id):
        self.output_ids.append(run_id)
        if run_id < 1000:
            raise AssertionError('Parent output requested instead of notebook task output')
        submission = self.runs[run_id - 1000]
        task = submission['tasks'][0]
        lab = task.task_key
        receipt = valid_receipt(self.n, lab, submission['run_as'].user_name,
                                task.notebook_task.base_parameters['validation_run_id'])
        suffix = task.notebook_task.base_parameters.get('project_suffix', '')
        if suffix:
            receipt['project_id'] += '-' + suffix
        receipt.update(self.world.get('receipt_changes', {}).get(lab, {}))
        text = json.dumps(receipt)
        if self.world.get('bad_json') == lab:
            text = 'not a receipt'
        output = obj(result=text, truncated=self.world.get('truncated') == lab)
        if self.world.get('missing_output') == lab:
            output = None
        return obj(error='notebook error' if self.world.get('output_error') == lab else None,
                   notebook_output=output)

    def cancel_run(self, run_id):
        self.cancelled.append(run_id)
        if self.world.get('cancel_failure'):
            raise ConnectionError('cancellation request did not reach the service')


def valid_receipt(namespace, lab='branch', user=USER, run_id=RUN_ID):
    return {
        'status': 'PASSED', 'lab': lab, 'release_version': namespace['KIT_VERSION'],
        'user': user, 'validation_run_id': run_id,
        'session_environment': namespace['SESSION_ENVIRONMENT'], 'prepared': True,
        'checks': sorted(namespace['LABS'][lab]['checks']),
        'project_id': f'lab-{lab}-participant', 'schema': 'lab_participant',
    }


@unittest.skipUnless(SDK_AVAILABLE, 'Install databricks-sdk>=0.81.0 to run READY behavior tests')
class ReadyTests(unittest.TestCase):
    def setUp(self):
        self.n = load_functions()
        self.world = {}
        self.workspace = Workspace(self.world)
        self.jobs = Jobs(self.n, self.world)
        self.client = obj(
            workspace=self.workspace, jobs=self.jobs,
            config=obj(host='https://workspace.example.test'),
            current_user=obj(me=lambda: obj(user_name=USER)),
        )
        self.n['WorkspaceClient'] = lambda: self.client
        self.n['time'] = Clock()
        self.n['secrets'] = obj(token_hex=lambda length: RUN_ID)
        self.n['urllib'] = obj(request=obj(Request=lambda url, headers: obj(full_url=url, headers=headers),
                                           urlopen=self.urlopen))
        self.files = {relative: ('# Databricks notebook source\n# ' + relative).encode()
                      for relative in self.n['SOURCES']}
        self.n['SOURCES'] = {relative: hashlib.sha256(data).hexdigest() for relative, data in self.files.items()}
        self.downloads = []

    def urlopen(self, request, timeout):
        self.downloads.append(request.full_url)
        relative = request.full_url.removeprefix(self.n['SOURCE_BASE'])
        if self.world.get('download_failure') == relative:
            raise ConnectionError('blocked')
        data = self.files[relative]
        if self.world.get('bad_download') == relative:
            data += b'changed'
        return io.BytesIO(data)

    def prepare(self, should_fail=False):
        output = io.StringIO()
        error = None
        result = None
        try:
            with contextlib.redirect_stdout(output):
                result = self.n['prepare_session']()
        except Exception as exc:
            error = exc
        if should_fail:
            self.assertIsNotNone(error, output.getvalue())
            self.assertNotIn('\nREADY:', output.getvalue())
        else:
            if error:
                raise error
            self.assertIn('\nREADY:', output.getvalue())
        return result, output.getvalue(), error

    def test_fresh_complete_preparation(self):
        result, output, _ = self.prepare()
        self.assertEqual(result['status'], 'READY')
        self.assertEqual(set(result['labs']), {'branch', 'search'})
        self.assertEqual(len(self.downloads), 3)
        self.assertEqual(len(self.workspace.imports), 4)
        self.assertEqual(len(self.jobs.submissions), 2)
        self.assertEqual(self.jobs.output_ids, [1101, 1102])
        self.assertEqual(len(self.workspace.deletes), 1)
        self.assertTrue(all(not call['overwrite'] for call in self.workspace.imports))
        self.assertTrue(all('lakebase-move' not in url for url in self.downloads))
        self.assertNotIn('Move lab', output)

    def test_job_identity_environment_and_parameters(self):
        self.prepare()
        for submission in self.jobs.submissions:
            self.assertEqual(submission['run_as'].user_name, USER)
            env = submission['environments'][0]
            self.assertEqual(env.environment_key, 'session')
            self.assertEqual(env.spec.client, '6')
            task = submission['tasks'][0]
            self.assertEqual(task.environment_key, env.environment_key)
            self.assertEqual(task.notebook_task.base_parameters, {
                'prepare_for_session': 'true', 'release_version': self.n['KIT_VERSION'],
                'validation_run_id': RUN_ID, 'session_environment': '6',
            })
            self.assertEqual(submission['idempotency_token'], f'lb-{RUN_ID}-{task.task_key}')

    def test_download_mismatch_imports_nothing(self):
        self.world['bad_download'] = list(self.files)[-1]
        self.prepare(should_fail=True)
        self.assertEqual(self.workspace.imports, [])
        self.assertEqual(self.jobs.submissions, [])

    def test_rehearsal_suffix_default_absent_leaves_participant_parameters_unchanged(self):
        self.prepare()
        for submission in self.jobs.submissions:
            self.assertNotIn('project_suffix', submission['tasks'][0].notebook_task.base_parameters)

    def test_rehearsal_suffix_passed_to_both_labs(self):
        self.n['dbutils'] = obj(widgets=obj(get=lambda name: 'test1106'))
        result, _, _ = self.prepare()
        for submission in self.jobs.submissions:
            self.assertEqual(submission['tasks'][0].notebook_task.base_parameters['project_suffix'], 'test1106')
        self.assertTrue(all(receipt['project_id'].endswith('-test1106') for receipt in result['labs'].values()))

    def test_invalid_rehearsal_suffix_fails_before_mutations(self):
        for suffix in ('../other', 'TEST', ' test', 'a' * 9, None, 1, 'a-b'):
            with self.subTest(suffix=suffix):
                self.setUp()
                self.n['dbutils'] = obj(widgets=obj(get=lambda name: suffix))
                self.prepare(should_fail=True)
                self.assertEqual(self.downloads, [])
                self.assertEqual(self.workspace.mkdirs_calls, [])
                self.assertEqual(self.workspace.imports, [])
                self.assertEqual(self.jobs.submissions, [])

    def test_rehearsal_suffix_receipt_must_identify_isolated_project(self):
        self.n['dbutils'] = obj(widgets=obj(get=lambda name: 'test1106'))
        self.world['receipt_changes'] = {'branch': {'project_id': 'original-project'}}
        self.prepare(should_fail=True)
        self.assertEqual(len(self.jobs.submissions), 1)

    def test_rehearsal_suffix_rejects_overlong_project_before_mutations(self):
        self.n['dbutils'] = obj(widgets=obj(get=lambda name: 'test1106'))
        self.client.current_user.me = lambda: obj(user_name='a' * 23 + '@example.test')
        self.prepare(should_fail=True)
        self.assertEqual(self.workspace.imports, [])
        self.assertEqual(self.jobs.submissions, [])

    def test_download_failure_imports_nothing(self):
        self.world['download_failure'] = list(self.files)[-1]
        self.prepare(should_fail=True)
        self.assertEqual(self.workspace.imports, [])

    def test_missing_file_before_import(self):
        for omitted in self.files:
            with self.subTest(omitted=omitted):
                partial = {k: v for k, v in self.files.items() if k != omitted}
                with self.assertRaises(RuntimeError):
                    self.n['install_sources'](self.client, USER, RUN_ID, partial)
                self.assertEqual(self.workspace.imports, [])

    def test_changed_bytes_before_import(self):
        files = dict(self.files)
        files[list(files)[-1]] += b'local edit'
        with self.assertRaises(RuntimeError):
            self.n['install_sources'](self.client, USER, RUN_ID, files)
        self.assertEqual(self.workspace.imports, [])

    def test_fresh_folder_preserves_existing_edited_copy(self):
        old_path = f'/Users/{USER}/lakebase-session-labs/older/labs/branch-per-change/Branch_Per_Change'
        self.workspace.objects[old_path] = b'participant edits'
        self.prepare()
        self.assertEqual(self.workspace.objects[old_path], b'participant edits')
        self.assertNotIn(old_path, [x['path'] for x in self.workspace.imports])

    def test_install_failure_releases_only_own_lock(self):
        self.world['import_failure'] = True
        self.prepare(should_fail=True)
        self.assertEqual(self.jobs.submissions, [])
        self.assertEqual(self.workspace.deletes,
                         [{'path': f'/Users/{USER}/lakebase-session-labs/READY_in_progress', 'recursive': False}])

    def test_missing_imported_notebook_blocks_jobs(self):
        self.world['missing_notebook'] = 'Retail_Search_Context'
        self.prepare(should_fail=True)
        self.assertEqual(self.jobs.submissions, [])

    def test_wrong_import_object_type_blocks_jobs(self):
        self.world['object_type'] = 'DIRECTORY'
        self.prepare(should_fail=True)
        self.assertEqual(self.jobs.submissions, [])

    def test_lock_collision_does_not_run_or_remove_other_lock(self):
        self.world['lock_collision'] = True
        self.prepare(should_fail=True)
        self.assertEqual(len(self.workspace.imports), 1)
        self.assertEqual(self.workspace.deletes, [])
        self.assertEqual(self.jobs.submissions, [])

    def test_uncertain_submission_keeps_lock(self):
        self.world['submit_failure'] = 'branch'
        _, output, _ = self.prepare(should_fail=True)
        self.assertEqual(self.workspace.deletes, [])
        self.assertIn('Marker retained:', output)

    def test_second_uncertain_submission_does_not_point_to_first_run(self):
        self.world['submit_failure'] = 'search'
        _, output, _ = self.prepare(should_fail=True)
        self.assertEqual(self.workspace.deletes, [])
        self.assertNotIn('Check job run 101 before deleting', output)

    def test_missing_parent_run_id_keeps_lock(self):
        self.world['missing_run_id'] = 'branch'
        self.prepare(should_fail=True)
        self.assertEqual(self.workspace.deletes, [])

    def test_poll_failure_keeps_lock(self):
        self.world['poll_failure'] = 'branch'
        self.prepare(should_fail=True)
        self.assertEqual(self.workspace.deletes, [])

    def test_timeout_requests_cancel_and_keeps_lock(self):
        self.world['timeout'] = 'branch'
        self.prepare(should_fail=True)
        self.assertEqual(self.jobs.cancelled, [101])
        self.assertEqual(self.workspace.deletes, [])

    def test_failed_cancel_is_reported_without_claiming_success(self):
        self.world['timeout'] = 'branch'
        self.world['cancel_failure'] = True
        _, output, error = self.prepare(should_fail=True)
        self.assertEqual(self.workspace.deletes, [])
        self.assertNotIn('Cancellation requested;', str(error))
        self.assertIn('cancel', str(error).lower())

    def test_terminal_failures_release_lock_and_never_ready(self):
        for fault in ('parent_failed', 'task_failed', 'missing_task', 'duplicate_task',
                      'missing_output', 'output_error', 'truncated', 'bad_json'):
            with self.subTest(fault=fault):
                self.setUp()
                self.world[fault] = 'branch'
                self.prepare(should_fail=True)
                self.assertEqual(len(self.workspace.deletes), 1)
                self.assertEqual(len(self.jobs.submissions), 1)

    def test_second_lab_failure_cannot_reuse_first_lab_success(self):
        self.world['parent_failed'] = 'search'
        self.prepare(should_fail=True)
        self.assertEqual(len(self.jobs.submissions), 2)
        self.assertEqual(len(self.workspace.deletes), 1)

    def test_receipt_rejects_stale_wrong_or_partial_results(self):
        changes = {
            'status': 'READY', 'lab': 'search', 'release_version': 'old',
            'validation_run_id': 'old-run', 'session_environment': '1', 'user': 'other@example.test',
            'prepared': False, 'checks': ['isolation', 'index'], 'project_id': '', 'schema': '',
        }
        for field, bad in changes.items():
            with self.subTest(field=field):
                receipt = valid_receipt(self.n)
                receipt[field] = bad
                with self.assertRaises(RuntimeError):
                    self.n['validate_receipt'](json.dumps(receipt), 'branch', USER, RUN_ID)

    def test_receipt_rejects_wrong_types_and_missing_values(self):
        for text in (None, '', 'null', '[]', 'true', '{}', 'not json'):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                self.n['validate_receipt'](text, 'branch', USER, RUN_ID)
        for prepared in ('true', 1, None):
            receipt = valid_receipt(self.n)
            receipt['prepared'] = prepared
            with self.subTest(prepared=prepared), self.assertRaises(RuntimeError):
                self.n['validate_receipt'](json.dumps(receipt), 'branch', USER, RUN_ID)
        for checks in ('isolation,index,schema,recovery,reset', None, [1, 2]):
            receipt = valid_receipt(self.n)
            receipt['checks'] = checks
            with self.subTest(checks=checks), self.assertRaises(RuntimeError):
                self.n['validate_receipt'](json.dumps(receipt), 'branch', USER, RUN_ID)

    def test_full_path_receipt_failure_releases_lock(self):
        self.world['receipt_changes'] = {'branch': {'prepared': False}}
        self.prepare(should_fail=True)
        self.assertEqual(len(self.workspace.deletes), 1)

    def test_lock_cleanup_failure_never_prints_ready(self):
        self.world['delete_failure'] = True
        self.prepare(should_fail=True)

    def test_failed_rerun_does_not_print_cached_ready(self):
        self.prepare()
        self.n['READY_RESULT'] = {'status': 'READY'}
        self.world['download_failure'] = list(self.files)[0]
        self.prepare(should_fail=True)
        final_cell = SOURCE.read_text().split('# COMMAND ----------')[-1]
        self.assertLess(final_cell.index('READY_RESULT = None'), final_cell.index('READY_RESULT = prepare_session()'))

    def test_real_release_hashes_and_no_move_dependencies(self):
        actual = load_functions()
        self.assertEqual(set(actual['SOURCES']), set(self.files))
        for relative, expected in actual['SOURCES'].items():
            data = (ROOT / relative).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected, relative)
        self.assertEqual(set(actual['LABS']), {'branch', 'search'})
        text = SOURCE.read_text()
        self.assertNotIn('apt.postgresql.org', text)
        self.assertNotIn('objects.githubusercontent.com', text)
        self.assertIn(actual['RELEASE_TAG'], actual['SOURCE_BASE'])

    def test_each_python_cell_compiles_and_has_what_why(self):
        last = ''
        count = 0
        for cell in SOURCE.read_text().split('# COMMAND ----------'):
            if '# MAGIC %md' in cell:
                last = cell
            elif cell.strip():
                ast.parse(cell)
                self.assertTrue(all(label in last for label in ('**What:**', '**Why:**', '**Look for:**')))
                count += 1
        self.assertEqual(count, 5)



if __name__ == '__main__':
    unittest.main(verbosity=2)
