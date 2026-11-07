"""Keep the public root notebook identical to the validated preparation source.

Run without arguments to refresh it, or with --check before publishing.
The root copy can also be published on main without merging other lab changes.
"""
import argparse
from pathlib import Path

root = Path(__file__).resolve().parents[1]
source = root / 'labs/session-preflight/Session_Preflight.py'
target = root / 'START_HERE.py'
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check', action='store_true')
args = parser.parse_args()
if args.check:
    if not target.exists() or target.read_bytes() != source.read_bytes():
        raise SystemExit('START_HERE.py differs from the preparation source. Run scripts/sync_start_here.py.')
    print('START_HERE.py matches the preparation source byte for byte.')
else:
    target.write_bytes(source.read_bytes())
    print('Updated START_HERE.py from the preparation source.')
