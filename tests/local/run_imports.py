"""Import smoke test: every module in the repo loads in the py39/pyspark env."""
import sys
sys.path.insert(0, 'tests/local')
import bootstrap
import importlib, pathlib, traceback

mods = []
root = pathlib.Path('.')
for p in sorted(root.rglob('*.py')):
    if any(part in ('tests', 'photos', 'framework') for part in p.parts):
        continue
    mods.append('.'.join(p.with_suffix('').parts))

failed = []
for m in mods:
    try:
        importlib.import_module(m)
        print('OK  ', m)
    except Exception as e:
        failed.append((m, f'{type(e).__name__}: {e}'))
        print('FAIL', m, '->', e)
print()
print(f'{len(mods)-len(failed)}/{len(mods)} modules imported cleanly')
for m, e in failed:
    print(' ', m, e)
