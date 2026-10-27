# -*- coding: utf-8 -*-
"""Reproduccion del escenario de los mails en dos fases (codigo actual).

Fase 1: corre ``channels`` (sin enriquecimiento de pivot) con la config
prod-fiel — ``credit_card_processed_data`` sin ``fec_apertura``
(REG_CREDIT_CARD_FEC=0). Escribe un ``pivot_prepared`` plano
[numcliente, vintage] y ejercita el path ``mob + tfrom`` de
``credit_card_mobs`` (el crash bureau_pull/channels del mail).

Fase 2: en el MISMO workdir corre ``giros_credito`` + ``liabilities``.
El pivot_prepared plano carece de ``crd_acct_nbr``/``num_contrato_*``
— el crash extract del mail. El codigo actual debe detectar el schema
incompleto, re-preparar y completar ambas fuentes.

Ademas el fixture del pivot usa ``vintage`` STRING (prod-fiel,
REG_PIVOT_VINTAGE_STR=1): la re-preparacion pasa por
``_get_range_dates``, que con vintages str tronaba con
``TypeError`` — el segundo crash del mail.

Uso:
    python tests/local/mail_repro.py [workdir]
"""
import json
import os
import sys
from pathlib import Path

os.environ["REG_CREDIT_CARD_FEC"] = "0"
os.environ["REG_PIVOT_VINTAGE_STR"] = "1"

REPO = Path(r"D:\projects\laika_devin")
TESTS = REPO / "tests" / "local"
WORKDIR = Path(sys.argv[1]) if len(sys.argv) > 1 else \
    TESTS / "workdir_mail_repro"

sys.path.insert(0, str(TESTS))
sys.path.insert(0, str(REPO))
import bootstrap  # noqa: E402
import regression_e2e as reg  # noqa: E402

assert reg.CREDIT_CARD_FEC is False, "REG_CREDIT_CARD_FEC no aplicado"

from datapull import DataPull  # noqa: E402
from libs.paths import PathsConfig  # noqa: E402
from main_dynamic import build_session_builder, run_source  # noqa: E402


def builder():
    spark = bootstrap.get_spark("mail_repro")
    reg.register_views(spark)
    return spark


def run_phase(sources, working_path):
    rigid = json.loads(
        (reg.CONFIG_DIR / "configuration.json").read_text(encoding="utf-8"))
    custom = json.loads(
        (reg.CONFIG_DIR / "config_custom.json").read_text(encoding="utf-8"))
    custom["working_path"] = working_path.as_posix()
    custom["sources"] = list(sources)
    datapull = DataPull(rigid, custom, builder)
    reg._prepare_aux_dirs(working_path)
    ok = True
    for source in sources:
        ok &= run_source(datapull, source, paths=PathsConfig(),
                         is_dynamic=True, step_filter=None,
                         cleanup=False, send_email=False)
    return ok


def pivot_cols(working_path):
    spark = bootstrap.get_spark("mail_repro_check")
    try:
        df = spark.read.parquet(
            (working_path / reg.YEAR_ID / "pivot_prepared").as_posix())
        return sorted(df.columns)
    finally:
        spark.stop()


def main():
    spark = bootstrap.get_spark("mail_repro_fixtures")
    try:
        reg.write_fixtures(spark)
    finally:
        spark.stop()
    reg.make_config()

    # ---- fase 1: channels solo -> pivot_prepared plano + mob+tfrom ----
    print("=== FASE 1: channels (pivot plano, cc sin fec_apertura) ===")
    assert run_phase(["channels"], WORKDIR), "FASE 1 fallo"
    cols1 = pivot_cols(WORKDIR)
    print("pivot_prepared tras fase 1:", cols1)
    assert cols1 == ["numcliente", "vintage"], cols1
    # la mobs de channels debio calcularse via mob+tfrom
    spark = bootstrap.get_spark("mail_repro_check2")
    try:
        cc_hist = spark.read.parquet(
            (WORKDIR / reg.YEAR_ID / "mobs_preparation"
             / "credit_card_processed_data_historic").as_posix())
        assert "fec_apertura" not in cc_hist.columns, cc_hist.columns
        m = spark.read.parquet(
            (WORKDIR / reg.YEAR_ID / "channels" / "mobs"
             / "calculate_mobs").as_posix())
        print("calculate_mobs cols:", sorted(m.columns))
        assert "mobs_credit_card" in m.columns
        assert "max_liab_cc" in m.columns
    finally:
        spark.stop()
    print("FASE 1 OK — mobs credit_card sin fec_apertura calculado")

    # ---- fase 2: giros_credito + liabilities sobre pivot envenenado --
    print("=== FASE 2: giros_credito + liabilities (re-preparacion) ===")
    assert run_phase(["giros_credito", "liabilities"], WORKDIR), \
        "FASE 2 fallo"
    cols2 = pivot_cols(WORKDIR)
    print("pivot_prepared tras fase 2:", cols2)
    assert "crd_acct_nbr" in cols2 and \
        "num_contrato_maestra" in cols2, cols2
    va = WORKDIR / reg.YEAR_ID / "giros_credito" / "vector_assembler"
    assert (va / "_SUCCESS").exists(), "giros_credito va falta"
    print("FASE 2 OK — pivot_prepared re-preparado, extract funciona")

    # ---- fase 3: la property valida aunque _path_new_pivot exista ----
    # En el mail, una preparacion que muere a medias deja
    # ``_path_new_pivot`` seteado al parquet malo y la siguiente fuente
    # lo recarga sin validar -> crash en cascada. Se simula ese estado:
    # pivot_prepared envenenado + attr seteado a mano.
    print("=== FASE 3: property valida pivot_prepared envenenado ===")
    import shutil as _sh
    spark = bootstrap.get_spark("mail_repro_p3")
    poison = WORKDIR / reg.YEAR_ID / "pivot_poisoned"
    spark.read.parquet(
        (WORKDIR / reg.YEAR_ID / "pivot_prepared").as_posix()) \
        .select("numcliente", "vintage").distinct() \
        .write.mode("overwrite").parquet(poison.as_posix())
    spark.stop()
    _sh.rmtree(WORKDIR / reg.YEAR_ID / "pivot_prepared")
    os.rename(poison, WORKDIR / reg.YEAR_ID / "pivot_prepared")

    rigid = json.loads(
        (reg.CONFIG_DIR / "configuration.json").read_text(encoding="utf-8"))
    custom = json.loads(
        (reg.CONFIG_DIR / "config_custom.json").read_text(encoding="utf-8"))
    custom["working_path"] = WORKDIR.as_posix()
    custom["sources"] = ["giros_credito"]
    dp = DataPull(rigid, custom, builder)
    # estado post-crash: el attr apunta al parquet plano
    dp._path_new_pivot = (WORKDIR / reg.YEAR_ID
                          / "pivot_prepared").as_posix()
    piv = dp.pivot
    assert "crd_acct_nbr" in piv.columns, sorted(piv.columns)
    print("FASE 3 OK — la property detecto el schema malo y re-preparo")
    print("\nMAIL REPRO OK — todos los escenarios del mail resueltos")


if __name__ == "__main__":
    main()
