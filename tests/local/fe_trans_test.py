"""Prueba enfocada de transformaciones de `fe` sobre el wide de adobe.

Corre `FeatureEngineering` directamente (misma llamada que
`DataPull._perform_fe`) sobre el parquet `pivot` que deja el e2e en
`tests/local/workdir_adobe_dyn`, para iterar rápido sobre las
transformaciones `incremental*`, `rmeans*`, `ratiosmod*` y `mean`.

Run:
    C:/Users/Usuario/miniconda3/envs/laika_datapull_py310/python.exe \\
        tests/local/fe_trans_test.py
"""
import os
import shutil
import sys
from pathlib import Path

TESTS_LOCAL = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_LOCAL))
sys.path.insert(0, str(TESTS_LOCAL.parents[1]))

import bootstrap  # noqa: E402  (shims + HADOOP_HOME)

os.environ.setdefault("LAIKA_DISABLE_EMAIL", "1")
os.environ["LAIKA_FS_MODE"] = "local"

PIVOT = (TESTS_LOCAL / "workdir_adobe_dyn" / "202601" /
         "adobe_bancanet" / "pivot")
WORKDIR_FE = TESTS_LOCAL / "workdir_fe_test"
FE_DICT = {"ignore_this_value_when_calculating_mt_features": -99999,
           "frequency_sufix": "w"}
PERIOD = 4


def run_case(spark, tag, trans):
    """Corre `FeatureEngineering.run(trans)` en un workdir limpio."""
    from libs.feature_engineering import FeatureEngineering
    wd = WORKDIR_FE / tag
    if wd.exists():
        shutil.rmtree(wd)
    wd.mkdir(parents=True)
    df = spark.read.parquet(str(PIVOT))
    fe = FeatureEngineering(df=df, working_path=str(wd), period=PERIOD,
                            sqlContext=spark, number_threads=2,
                            fe_dict=FE_DICT, mode="overwrite",
                            is_dynamic=False)
    fe.run(list(trans))
    feats_dir = wd / "features"
    feats = sorted(p.name for p in feats_dir.iterdir()
                   if (p / "_SUCCESS").exists())
    print(f"[{tag}] {len(feats)} parquets:", feats)
    for name in sorted(set(trans) | {"raw"}):
        check = feats_dir / f"{name}_features"
        if check.exists() and (check / "_SUCCESS").exists():
            n = len(spark.read.parquet(str(check)).columns)
            print(f"    {name}_features: {n} cols")
    return feats


def main() -> None:
    spark = bootstrap.get_spark("fe_trans_test")
    spark.sparkContext.setLogLevel("ERROR")
    assert PIVOT.exists(), f"corre primero adobe_e2e.py (falta {PIVOT})"

    # 1) las pedidas + las elementales de las que dependen ratiosmod*
    full = ["incremental", "incremental_mt", "rmeans", "rmeans_mt",
            "ratiosmod", "ratiosmod_mt",
            "min", "max", "mean", "std", "sum",
            "min_mt", "max_mt", "mean_mt", "std_mt", "sum_mt"]
    run_case(spark, "full", full)

    # 2) una sola feature: mean
    run_case(spark, "solo_mean", ["mean"])

    # 3) ratiosmod sin dependencias (debe omitirse con aviso, no romper)
    run_case(spark, "ratiosmod_solo", ["ratiosmod", "ratiosmod_mt"])

    spark.stop()
    print("FE TRANS TEST OK")


if __name__ == "__main__":
    main()
