"""Prueba de features para adobe_bancanet con ``n_mis=4`` y ``period=12``.

Corre la cadena extract -> weeks -> groupby -> pivoting -> fe de
``adobe_bancanet`` con esos parámetros y luego verifica las
transformaciones (incl. incremental*, rmeans*, ratiosmod* y ``mean``
solo). Con period=12 los dominios cortos (quincenal/dseq/ld) producen
pocas o ninguna ventana — el objetivo es que nada rompa.

Run:
    C:/Users/Usuario/miniconda3/envs/laika_datapull_py310/python.exe \\
        tests/local/fe_n4p12_test.py
"""
import json
import os
import shutil
import sys
from pathlib import Path

TESTS_LOCAL = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_LOCAL))
sys.path.insert(0, str(TESTS_LOCAL.parents[1]))

import bootstrap  # noqa: E402  (shims + HADOOP_HOME)

os.environ["LAIKA_FS_MODE"] = "local"
os.environ.setdefault("LAIKA_DISABLE_EMAIL", "1")

import adobe_e2e  # noqa: E402  (reusa fixtures/vistas del e2e)

FIXTURES = adobe_e2e.FIXTURES
YEAR_ID = adobe_e2e.YEAR_ID
WORKDIR = TESTS_LOCAL / "workdir_adobe_n4p12"
FE_DICT = {"ignore_this_value_when_calculating_mt_features": -99999,
           "frequency_sufix": "w"}

TRANS = ["incremental", "incremental_mt", "rmeans", "rmeans_mt",
         "ratiosmod", "ratiosmod_mt",
         "min", "max", "mean", "std", "sum",
         "min_mt", "max_mt", "mean_mt", "std_mt", "sum_mt"]


def run_pipeline() -> None:
    """extract->...->fe de adobe_bancanet con n_mis=4, period=12."""
    from datapull import DataPull
    from libs.paths import PathsConfig
    from main_dynamic import build_session_builder, run_source

    real_build = build_session_builder(local=True)

    def builder():
        spark = real_build()
        adobe_e2e.register_views(spark)
        return spark

    rigid = json.loads(
        (adobe_e2e.CONFIG_DIR / "configuration.json").read_text(
            encoding="utf-8"))
    custom = {
        "sources": ["adobe_bancanet"],
        "working_path": WORKDIR.as_posix(),
        "year_id": YEAR_ID,
        "n_mis": "4",
        "period": "12",
        "pivot_path": (FIXTURES / "pivot").as_posix(),
        "lista_trans": TRANS,
        "email_sender": "local",
        "email_receivers": ["local"],
        "n_threads": "2",
        "write_mode": "overwrite",
    }
    if WORKDIR.exists():
        shutil.rmtree(WORKDIR)
    datapull = DataPull(rigid, custom, builder)
    ok = run_source(datapull, "adobe_bancanet", paths=PathsConfig(),
                    is_dynamic=True,
                    step_filter=["extract", "weeks", "groupby",
                                 "pivoting", "fe"],
                    cleanup=False, send_email=False)
    if not ok:
        sys.exit("pipeline n4p12 falló")


def _nan_count(spark, df) -> int:
    import pyspark.sql.functions as F
    # solo columnas numericas: raw_features trae numcliente (string) y
    # vintage_date (date), donde isnan no aplica
    cols = [c for c, t in df.dtypes if t in ("double", "float")
            and c != "id_msf_tools_v2_12315"]
    if not cols:
        return 0
    exprs = [F.sum(F.when(F.isnan(F.col(c)) | F.col(c).isNull(), 1)
                   .otherwise(0))
             for c in cols]
    return sum(v or 0 for v in df.agg(*exprs).collect()[0].asDict().values())


def verify() -> None:
    """Lista los parquets de features y chequea NaN/nulls."""
    spark = bootstrap.get_spark("fe_n4p12_verify")
    spark.sparkContext.setLogLevel("ERROR")
    feats_dir = WORKDIR / YEAR_ID / "adobe_bancanet" / "features"
    assert feats_dir.is_dir(), f"no existe {feats_dir}"
    checks = []

    def check(name, cond):
        checks.append((name, bool(cond)))

    for p in sorted(feats_dir.iterdir()):
        if not (p / "_SUCCESS").exists():
            continue
        df = spark.read.parquet(str(p))
        ncols = len(df.columns) - 1
        nans = _nan_count(spark, df)
        check(f"{p.name}: {ncols} cols, 0 NaN", ncols > 0 and nans == 0)

    # mean solo, sobre el pivot de este run
    from libs.feature_engineering import FeatureEngineering
    solo_dir = WORKDIR / "solo_mean"
    if solo_dir.exists():
        shutil.rmtree(solo_dir)
    df_pivot = spark.read.parquet(
        str(WORKDIR / YEAR_ID / "adobe_bancanet" / "pivot"))
    fe = FeatureEngineering(df=df_pivot, working_path=str(solo_dir),
                            period=12, sqlContext=spark, number_threads=2,
                            fe_dict=FE_DICT, mode="overwrite",
                            is_dynamic=False)
    fe.run(["mean"])
    mean_dir = solo_dir / "features" / "mean_features"
    check("solo mean: mean_features escrito",
          (mean_dir / "_SUCCESS").exists())
    if (mean_dir / "_SUCCESS").exists():
        dfm = spark.read.parquet(str(mean_dir))
        check("solo mean: 0 NaN", _nan_count(spark, dfm) == 0)

    print("=" * 60)
    failed = [n for n, ok in checks if not ok]
    for n, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {n}")
    print("=" * 60)
    spark.stop()
    if failed:
        sys.exit(f"{len(failed)} checks failed: {failed}")
    print(f"FE N4P12 OK — {len(checks)} checks")


def main() -> None:
    if not (FIXTURES / "pivot").exists() \
            or not (adobe_e2e.CONFIG_DIR / "configuration.json").exists():
        sys.exit("faltan fixtures/config: corre tests/local/adobe_e2e.py "
                 "primero")
    run_pipeline()
    verify()


if __name__ == "__main__":
    main()
