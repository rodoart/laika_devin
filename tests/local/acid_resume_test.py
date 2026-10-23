"""Acid test de reanudación: kills simulados a mitad de pipeline.

Para cada fuente del regression harness, mata el proceso en distintas
etapas **a mitad de una escritura parquet** — dejando un directorio
parcial corrupto (sin ``_SUCCESS``), como quedaría tras un kill real de
plataformas — reanuda, y exige que el workdir final sea idéntico al
``golden_regression.json`` (una corrida limpia de una sola vez).

Mecanismo: se envuelve ``DataFrameWriter.parquet`` — el embudo único por
el que pasan todos los writes del datapull — y en el punto marcado se
crea el dir destino con un fichero basura y se lanza RuntimeError.

    python tests/local/acid_resume_test.py

Requiere ``golden_regression.json`` (corre primero
``regression_e2e.py`` + ``snapshot`` si no existe).
"""
import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "regression_e2e", Path(__file__).parent / "regression_e2e.py")
re_e2e = importlib.util.module_from_spec(_spec)
sys.modules["regression_e2e"] = re_e2e
_spec.loader.exec_module(re_e2e)

SOURCES = re_e2e.SOURCES
GOLDEN = re_e2e.GOLDEN
YEAR_ID = re_e2e.YEAR_ID
CONFIG_DIR = re_e2e.CONFIG_DIR

# (descripcion, substring del path normalizado, ordinal de escritura)
# ordenadas según el flujo real del pipeline por fuente.
KILLS = [
    ("pivot_prepared (extract de la 1a fuente)", "/pivot_prepared", 1),
    ("extract payroll_streaks — mes asigna (tfrom weekly+spill)",
     "/payroll_streaks/extract/asigna", 3),
    ("extract payroll_streaks — ctrl de mes (_meta)",
     "/payroll_streaks/extract/_meta/", 2),
    ("extract payroll_streaks — mes nomina (tfrom weekly+spill)",
     "/payroll_streaks/extract/nomina", 2),
    ("mt payroll_streaks", "/payroll_streaks/missing_treatment/", 2),
    ("groupby payroll_streaks", "/payroll_streaks/groupby", 1),
    ("pivoting payroll_streaks", "/payroll_streaks/pivot", 1),
    ("groupby payroll_income", "/payroll_income/groupby", 1),
    ("fe payroll_income (raw)", "/payroll_income/features/raw_features", 1),
    ("mobs payroll_income (1a imputacion)", "/payroll_income/mobs/", 2),
    ("va payroll_income (write final)", "/payroll_income/vector_assembler", 1),
    ("extract bureau_pull (mes)",
     "/bureau_pull/extract/adv_colocacion", 2),
    ("mt bureau_pull (checkpoint por variable)",
     "/bureau_pull/missing_treatment/", 1),
    ("groupby bureau_pull", "/bureau_pull/groupby", 1),
    ("pivoting bureau_pull", "/bureau_pull/pivot", 1),
    ("mobs_preparation (mini-extract auxiliar)", "/mobs_preparation/", 3),
    ("mobs bureau_pull (imputacion por mes)", "/bureau_pull/mobs/", 4),
    ("extract liabilities", "/liabilities/extract/onus", 2),
    ("groupby liabilities", "/liabilities/groupby", 1),
    ("pivoting liabilities", "/liabilities/pivot", 1),
    ("mobs liabilities (imputacion)", "/liabilities/mobs/", 2),
    ("extract rev_balance", "/rev_balance/extract/ccp", 2),
    ("groupby rev_balance", "/rev_balance/groupby", 1),
    ("fe rev_balance", "/rev_balance/features/", 3),
    ("va rev_balance (write final)", "/rev_balance/vector_assembler", 1),
]

MAX_RESUMES_PER_SOURCE = 8


def _norm(path) -> str:
    return str(path).replace("\\", "/")


class KillSwitch:
    """Envuelve ``DataFrameWriter.parquet`` y mata en el punto armado.

    Cada kill pendiente lleva su propio contador: un kill cuyo ordinal
    resulte inalcanzable no bloquea a los posteriores — el primero de la
    lista que complete su ordinal dispara (orden de la lista en empates).
    """

    def __init__(self, kills):
        from libs.hive_path import mkdir, touch
        self._mkdir, self._touch = mkdir, touch
        self._pending = [[label, needle, ordinal, 0]
                         for label, needle, ordinal in kills]
        self.fired = []
        self._orig = None

    def _check(self, path):
        for kill in self._pending:
            _label, needle, ordinal, _count = kill
            if needle not in _norm(path):
                continue
            kill[3] += 1
            if kill[3] < ordinal:
                return
            # kill: deja el dir destino con basura (write a medias) y
            # lanza. El junk va en ``_temporary/`` — lo que deja un kill
            # real de Spark (los readers ignoran prefijo ``_``).
            self._pending.remove(kill)
            self._mkdir(_norm(path) + "/_temporary", "-p")
            self._touch(_norm(path) + "/_temporary/part-0000-CORRUPTED")
            self.fired.append((kill[0], path))
            raise RuntimeError(f"simulated kill: {kill[0]} @ {path}")

    def install(self):
        from pyspark.sql.readwriter import DataFrameWriter
        kill = self
        orig = DataFrameWriter.parquet
        self._orig = orig

        def patched(self, path, *args, **kwargs):
            kill._check(path)
            return orig(self, path, *args, **kwargs)

        DataFrameWriter.parquet = patched

    def restore(self):
        if self._orig is not None:
            from pyspark.sql.readwriter import DataFrameWriter
            DataFrameWriter.parquet = self._orig


def run_all(workdir: Path, kill: KillSwitch) -> None:
    """Corre las fuentes una a una reanudando tras cada kill."""
    from datapull import DataPull
    from libs.paths import PathsConfig
    from main_dynamic import build_session_builder, run_source

    real_build = build_session_builder(local=True)

    def builder():
        spark = real_build()
        re_e2e.register_views(spark)
        return spark

    rigid = json.loads(
        (CONFIG_DIR / "configuration.json").read_text(encoding="utf-8"))
    custom = json.loads(
        (CONFIG_DIR / "config_custom.json").read_text(encoding="utf-8"))
    custom["working_path"] = workdir.as_posix()
    datapull = DataPull(rigid, custom, builder)
    re_e2e._prepare_aux_dirs(workdir)

    for source in SOURCES:
        resumes = 0
        while True:
            ok = run_source(datapull, source, paths=PathsConfig(),
                            is_dynamic=True, step_filter=None,
                            cleanup=False, send_email=False)
            if ok:
                break
            resumes += 1
            if resumes > MAX_RESUMES_PER_SOURCE:
                sys.exit(f"{source}: {resumes} reanudaciones sin"
                         " converger — resume roto")


def compare(workdir: Path) -> int:
    """Diff de parquets vs golden (misma regla passthrough que verify)."""
    spark = bootstrap.get_spark("acid_compare")
    gold = json.loads(GOLDEN.read_text(encoding="utf-8"))
    root = workdir / YEAR_ID
    seen = {}
    for d in re_e2e._parquet_dirs(root):
        rel = f"{YEAR_ID}/" + d.relative_to(root).as_posix()
        seen[rel] = re_e2e._df_fingerprint(spark, d)
    spark.stop()

    checks, fails = 0, []
    for rel, ref in gold.items():
        equiv = re_e2e._equiv_golden_rel(rel)
        if equiv is None:
            continue
        checks += 1
        cur = seen.get(equiv)
        if cur is None:
            if "/mobs/imputate_mobs/" in rel:
                twin = rel.replace("/mobs/imputate_mobs/", "/features/")
                if (twin in gold and twin in seen
                        and seen[twin] == gold[twin] and gold[twin] == ref):
                    continue
            fails.append(f"FALTA {rel}")
        elif cur != ref:
            diffs = [k for k in ref if ref[k] != cur[k]]
            fails.append(f"DIFF {rel} ({','.join(diffs)})")
    for rel in seen:
        if rel not in gold:
            if any(f"/{src}/extract/" in rel
                   for src in re_e2e.WEEKLY_INTEGRATED_SOURCES):
                continue
            checks += 1
            fails.append(f"EXTRA {rel}")
    print(f"{checks - len(fails)}/{checks} parquets identicos")
    for f in fails:
        print("  ", f)
    return len(fails)


def main() -> None:
    if not GOLDEN.exists():
        sys.exit("no hay golden_regression.json — corre "
                 "regression_e2e.py primero")
    if not re_e2e.FIXTURES.exists():
        spark = bootstrap.get_spark("acid_fixtures")
        try:
            re_e2e.write_fixtures(spark)
        finally:
            spark.stop()
    re_e2e.make_config()

    tmp = Path(tempfile.mkdtemp(prefix="acid_resume_"))
    kill = KillSwitch(KILLS)
    kill.install()
    try:
        run_all(tmp, kill)
    finally:
        kill.restore()

    print(f"Kills ejecutados: {len(kill.fired)}/{len(KILLS)}")
    for label, path in kill.fired:
        print(f"   {label}")
    for label, _needle, _ordinal, count in kill._pending:
        print(f"   NO DISPARO: {label} (writes vistos: {count})")

    fails = compare(tmp)
    shutil.rmtree(tmp, ignore_errors=True)
    if fails:
        sys.exit("ACID TEST FAILED — resume no preserva outputs")
    print("ACID TEST OK — mismo resultado tras kills que en una sola pasada")


if __name__ == "__main__":
    main()
