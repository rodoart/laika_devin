"""Resume/crash-recovery test for the dynamic datapull pipeline.

Verifies that interrupting `main_dynamic.py --local` at different points
produces byte-identical *logical* outputs to an uninterrupted run:

1. Baseline: clean run of all stages -> snapshot hashes of every parquet.
2. For each prefix P of the source's ``steps_list``: clean + seed extract,
   run ``--steps <P>`` (partial), then run the FULL pipeline (it should
   reload/recompute the rest) -> compare vs baseline.
3. Hard kill mid-run: start the full pipeline, SIGKILL it after the first
   stage finishes (mid second stage), rerun full -> compare.

Equality is checked at the DataFrame level (rows sorted by all columns,
hashed) so parquet file naming/metadata differences don't matter.

Run:
    C:/Users/Usuario/miniconda3/envs/laika_datapull_py310/python.exe \\
        tests/local/resume_test.py
"""
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

TESTS_LOCAL = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_LOCAL))
import bootstrap  # noqa: E402  (PosixPath shim + HADOOP_HOME)

PROJECT_ROOT = TESTS_LOCAL.parents[1]
WORKDIR_DYN = Path(bootstrap.POSIX_WORKDIR).parent / "workdir_dyn"
SEED_EXTRACT = Path(bootstrap.POSIX_WORKDIR) / "extract"
SOURCE_DIR = WORKDIR_DYN / "202601" / "payroll_streaks"
BASELINE_JSON = TESTS_LOCAL / "resume_baseline.json"

ENV = dict(os.environ)
ENV.update({
    "LAIKA_CONFIG_DIR": "tests/local/config_dyn",
    "LAIKA_DISABLE_EMAIL": "1",
    "LAIKA_FS_MODE": "local",
})
CMD = [sys.executable, str(PROJECT_ROOT / "main_dynamic.py"),
       "--local", "--no-email"]

STAGES = ["extract", "weeks", "mt", "groupby", "pivoting", "fe", "va"]


def clean_and_seed() -> None:
    """Borra ``workdir_dyn`` y re-siembra los ``extract/*_historic``
    (las queries Hive del extract no corren en local)."""
    if WORKDIR_DYN.exists():
        shutil.rmtree(WORKDIR_DYN)
    dst = SOURCE_DIR / "extract"
    dst.mkdir(parents=True)
    for table in SEED_EXTRACT.iterdir():
        shutil.copytree(table, dst / table.name)


def run_pipeline(steps=None, timeout=600, kill_after_marker=None):
    """Lanza main_dynamic y espera. Si ``kill_after_marker`` se da, mata
    el proceso en cuanto aparece ese texto en stdout (kill a mitad)."""
    cmd = CMD + (["--steps"] + steps if steps else [])
    proc = subprocess.Popen(
        cmd, cwd=PROJECT_ROOT, env=ENV,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace")
    out_lines = []
    killed = False
    t0 = time.time()
    while proc.poll() is None:
        line = proc.stdout.readline()
        if line:
            out_lines.append(line)
        if kill_after_marker and any(
                kill_after_marker in l for l in out_lines) and not killed:
            time.sleep(2)  # deja que avance un poco dentro de la etapa
            proc.kill()
            killed = True
            break
        if time.time() - t0 > timeout:
            proc.kill()
            break
    out_lines += proc.stdout.readlines() if proc.stdout else []
    proc.wait()
    return proc.returncode, "".join(out_lines)


def snapshot_hashes():
    """relpath -> hash estable del contenido lógico de cada parquet."""
    spark = bootstrap.get_spark("resume_test_hash")
    spark.sparkContext.setLogLevel("ERROR")
    hashes = {}
    for p in sorted(SOURCE_DIR.rglob("*")):
        if not p.is_dir() or "tmp" in p.parts:
            continue
        if not any(f.suffix == ".parquet" for f in p.iterdir()):
            continue
        rel = p.relative_to(SOURCE_DIR).as_posix()
        try:
            df = spark.read.parquet(p.as_posix())
        except Exception as e:  # parquet parcial/truncado por el kill
            hashes[rel] = f"UNREADABLE:{type(e).__name__}"
            continue
        # Column order is non-deterministic across runs (list(set(...)) in
        # the pipeline) -> compare rows sorted AND with sorted columns.
        df = df.select(sorted(df.columns))
        rows = df.orderBy(*df.columns).rdd.map(repr).collect()
        hashes[rel] = hashlib.sha256("\n".join(rows).encode()).hexdigest()
    return hashes


def compare(name: str, hashes: dict, baseline: dict) -> bool:
    if hashes == baseline:
        print(f"[PASS] {name}: outputs idénticos al baseline")
        return True
    keys = set(hashes) | set(baseline)
    for k in sorted(keys):
        a, b = baseline.get(k), hashes.get(k)
        if a != b:
            print(f"  diff {k}: baseline={a} actual={b}")
    print(f"[FAIL] {name}: outputs difieren del baseline")
    return False


def main() -> None:
    results = {}

    # ---------- baseline ----------
    print("=" * 60)
    print("BASELINE: run completo sin interrupciones")
    clean_and_seed()
    rc, out = run_pipeline()
    if rc != 0:
        print(out[-3000:])
        sys.exit("baseline run failed")
    baseline = snapshot_hashes()
    BASELINE_JSON.write_text(json.dumps(baseline, indent=1))
    for k, v in baseline.items():
        print(f"  {k}: {v[:16]}...")

    # ---------- resume after each stage boundary ----------
    for i in range(1, len(STAGES) - 1):
        prefix = STAGES[:i + 1]  # e.g. ['extract','weeks']
        case = f"resume after {prefix[-1]}"
        print("=" * 60)
        print(f"CASE: {case} (primero --steps {' '.join(prefix)})")
        clean_and_seed()
        rc, out = run_pipeline(steps=prefix)
        if rc != 0:
            print(out[-2000:])
            results[case] = f"partial run failed rc={rc}"
            continue
        rc, out = run_pipeline()
        if rc != 0:
            print(out[-2000:])
            results[case] = f"full run failed rc={rc}"
            continue
        results[case] = compare(case, snapshot_hashes(), baseline)

    # ---------- hard kill mid-stage ----------
    case = "hard kill mid-stage (durante weeks)"
    print("=" * 60)
    print(f"CASE: {case}")
    clean_and_seed()
    rc, out = run_pipeline(kill_after_marker="Step finished")
    print(f"  killed rc={rc}; log:\n{out[-1200:]}")
    rc, out = run_pipeline()
    if rc != 0:
        print(out[-2000:])
        results[case] = f"rerun failed rc={rc}"
    else:
        results[case] = compare(case, snapshot_hashes(), baseline)

    print("=" * 60)
    print("RESUMEN")
    for k, v in results.items():
        print(f"  {k}: {'PASS' if v is True else v}")
    sys.exit(0 if all(v is True for v in results.values()) else 1)


if __name__ == "__main__":
    main()
