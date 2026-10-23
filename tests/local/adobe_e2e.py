"""E2E simulado de las fuentes Adobe en el datapull.

Corre el pipeline completo (extract -> weeks -> groupby -> pivoting ->
fe -> mobs -> va) de ``adobe_bancanet`` y ``adobe_mobile`` sobre datos
simulados, **in-process** (el catálogo in-memory de Spark local no
persiste entre procesos; por eso las tablas fuente se exponen como
*global temp views* que el builder re-registra tras cada
``reset_spark_session``):

- ``global_temp.adobe_t`` / ``global_temp.adobe_mob_t`` — tabla de hits
  **diaria** (un parquet particionado por ``file_date``, como el real).
- ``global_temp.hash_snap`` — tabla hash ``numcliente <-> hash/salti``
  con snapshots ``proc_dt`` (incluye uno "futuro" para probar la
  regla de no-fuga del special treatment).
- ``<fixtures>/pivot`` — pivot ``(numcliente, vintage)``.

El config se genera de ``config/configuration.json`` real (los bloques
``adobe_*`` de producción) solo cambiando libname/table/hash a las
vistas globales.

Run:
    C:/Users/Usuario/miniconda3/envs/laika_datapull_py310/python.exe \\
        tests/local/adobe_e2e.py
"""
import json
import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

TESTS_LOCAL = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_LOCAL))
sys.path.insert(0, str(TESTS_LOCAL.parents[1]))

import bootstrap  # noqa: E402  (shims + HADOOP_HOME)

os.environ.setdefault("LAIKA_DISABLE_EMAIL", "1")
os.environ["LAIKA_FS_MODE"] = "local"
# el mail real se escribe como .eml (dry-run) en vez de abrir SMTP
os.environ["LAIKA_EMAIL_DRY_RUN_DIR"] = str(
    TESTS_LOCAL / "workdir_adobe" / "emails")

PROJECT_ROOT = TESTS_LOCAL.parents[1]
FIXTURES = TESTS_LOCAL / "workdir_adobe" / "fixtures"
WORKDIR_DYN = TESTS_LOCAL / "workdir_adobe_dyn"
CONFIG_DIR = TESTS_LOCAL / "config_adobe"
YEAR_ID = "202601"
VINTAGE = date(2025, 8, 31)

BNT_COLS = ["post_visid_high", "post_visid_low", "visit_num",
            "post_evar5", "date_time", "file_date", "post_evar1",
            "post_evar3", "pagename", "prop11",
            "post_clickmaplinkbyregion", "evar25", "post_evar25",
            "visit_page_num"]
MOB_COLS = ["post_visid_high", "post_visid_low", "visit_num",
            "post_evar5", "date_time", "file_date", "pagename",
            "post_evar3", "post_evar1", "prop11", "evar25", "evar30",
            "post_evar63", "post_evar25", "visit_page_num"]

PIL_VIEW = "MX|BNP|Dashboard|Overlay|Logra mas|PERSONAL|ConoceMas"
LOP_VIEW = "MX|BNP|Dashboard|Overlay|Con Disponible Banamex|ConoceMas"


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
def _bnt_hit(vh, vl, vn, evar5, day, hhmm, evar25=""):
    """Un hit bancanet (pagename pasa el filtro de grupo 1)."""
    return (str(vh), str(vl), vn, evar5, f"{day} {hhmm}", day,
            "", "", "MX|BNP3|SESION|FINAL", "", "", evar25, "", "1")


def _mob_hit(vh, vl, vn, evar5, day, hhmm, evar63="MX|DIS|promo"):
    return (str(vh), str(vl), vn, evar5, f"{day} {hhmm}", day,
            "MX|Mobile|Dashboard|Productos|MOB-X",
            "OFERTASPARAMETRICAS", "", "", "", "", evar63, "", "1")


def write_fixtures(spark) -> None:
    """Genera pivot + tabla de hits diaria + tabla hash simulados."""
    if FIXTURES.parent.exists():
        shutil.rmtree(FIXTURES.parent)
    FIXTURES.mkdir(parents=True)

    # --- pivot: clientes con y sin actividad ---------------------------
    clients = ["c1", "c2", "c3", "c4", "m1"]
    spark.createDataFrame(
        [(c, VINTAGE) for c in clients], ["numcliente", "vintage"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "pivot"))

    # --- tabla hash: snapshot viejo + uno FUTURO (no debe usarse) ------
    spark.createDataFrame(
        [("c1", "H1", "S1", "2025-01-01"),
         ("c2", "H2", "S2", "2025-01-01"),
         ("c3", "H3", "S3", "2025-01-01"),
         ("m1", "HM1", "SM1", "2025-01-01"),
         ("c9", "HF", "SF", "2025-08-20")],   # solo en snapshot futuro
        ["numcliente", "numcliente_hash", "numcliente_salti", "proc_dt"],
    ).write.mode("overwrite").parquet(str(FIXTURES / "hash_snap"))

    # --- hits bancanet (diarios, particionados por file_date) ----------
    rows = []
    # c1: hit mensual con flag PIL_view (feb-ago) + sesión de 2 hits
    # donde el primero NO trae evar5 (ejercita el backfill)
    for m in range(2, 9):
        rows.append(_bnt_hit(11, 111, 1, "H1", f"2025-0{m}-10",
                             "10:00:00", PIL_VIEW))
    rows.append(_bnt_hit(11, 111, 7, "", "2025-08-15", "11:00:00",
                         PIL_VIEW))
    rows.append(_bnt_hit(11, 111, 7, "H1", "2025-08-15", "11:05:00"))
    # c2: hits semanales con flag LOP_view (mar-ago)
    d = date(2025, 3, 3)
    while d <= date(2025, 8, 25):
        rows.append(_bnt_hit(22, 222, 2, "H2", d.isoformat(),
                             "12:00:00", LOP_VIEW))
        d += timedelta(days=7)
    # c3: actividad diaria solo en los últimos 12 días (dseq/rachas,
    # mobs_adobe = 1 -> features de ventana larga a -99999)
    for dd in range(20, 32):
        rows.append(_bnt_hit(33, 333, 3, "H3", f"2025-08-{dd}",
                             "09:00:00", PIL_VIEW))
    # hash que solo existe en el snapshot FUTURO -> nunca resuelve
    rows.append(_bnt_hit(99, 999, 9, "HF", "2025-08-05", "09:00:00",
                         PIL_VIEW))
    # hits fuera de los filtros de página -> caen en el special treatment
    rows.append(("55", "555", 5, "H1", "2025-08-06 09:00:00",
                 "2025-08-06", "", "", "MX|OTRA|PAGINA", "", "", "", "",
                 "1"))
    spark.createDataFrame(rows, BNT_COLS)\
        .write.mode("overwrite").partitionBy("file_date")\
        .parquet(str(FIXTURES / "adobe_t"))

    # --- hits mobile (diarios) -----------------------------------------
    mrows = []
    for m in range(3, 9):
        mrows.append(_mob_hit(44, 444, 4, "HM1", f"2025-0{m}-15",
                              "18:00:00"))
    spark.createDataFrame(mrows, MOB_COLS)\
        .write.mode("overwrite").partitionBy("file_date")\
        .parquet(str(FIXTURES / "adobe_mob_t"))


def register_views(spark) -> None:
    """Expone las fixtures como ``global_temp.*`` — resiste el
    ``reset_spark_session`` entre etapas (se rellama en cada builder)."""
    for name in ("adobe_t", "adobe_mob_t", "hash_snap"):
        spark.read.parquet(str(FIXTURES / name))\
            .createOrReplaceGlobalTempView(name)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
def make_config() -> None:
    """Genera ``config_adobe/`` a partir del configuration.json real."""
    CONFIG_DIR.mkdir(exist_ok=True)
    rigid = json.loads(
        (PROJECT_ROOT / "config" / "configuration.json").read_text(
            encoding="utf-8"))
    for src, tbl in (("adobe_bancanet", "adobe_t"),
                     ("adobe_mobile", "adobe_mob_t")):
        ex = rigid[src]["extract"]
        ex["libname"] = ["global_temp"]
        ex["table_name"] = [tbl]
        ex["adobe"]["hash_id_db"] = "global_temp.hash_snap"
        rigid[src]["missing_treatment"]["mobs"]["table_name"] = [
            f"extract/{tbl}_historic"]
    (CONFIG_DIR / "configuration.json").write_text(
        json.dumps(rigid, indent=4, ensure_ascii=False), encoding="utf-8")
    custom = {
        "sources": ["adobe_bancanet", "adobe_mobile"],
        "working_path": WORKDIR_DYN.as_posix(),
        "year_id": YEAR_ID,
        "n_mis": "6",
        "period": "4",
        "pivot_path": (FIXTURES / "pivot").as_posix(),
        "lista_trans": [
            "sum_mt", "mean_mt", "min_mt", "max_mt", "std_mt", "trend_mt",
            "longest_1_streak_onz_mt", "first_1_onz_mt",
            "transitions_onz_mt", "entropy_onz_mt",
            "kolmogorov_complexity_onz_mt"],
        "email_sender": "local",
        "email_receivers": ["local"],
        "n_threads": "2",
        "write_mode": "overwrite",
    }
    (CONFIG_DIR / "config_custom.json").write_text(
        json.dumps(custom, indent=4, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------
def run_pipeline() -> None:
    """Corre las dos fuentes con el builder que re-registra las vistas."""
    from datapull import DataPull
    from libs.paths import PathsConfig
    from main_dynamic import (build_session_builder, run_source)

    real_build = build_session_builder(local=True)

    def builder():
        spark = real_build()
        register_views(spark)   # re-registrar tras reset_spark_session
        return spark

    rigid = json.loads((CONFIG_DIR / "configuration.json").read_text())
    custom = json.loads((CONFIG_DIR / "config_custom.json").read_text())
    datapull = DataPull(rigid, custom, builder)
    for source in custom["sources"]:
        ok = run_source(datapull, source, paths=PathsConfig(),
                        is_dynamic=True, step_filter=None,
                        cleanup=False, send_email=True)
        if not ok:
            sys.exit(f"pipeline failed for {source}")


def resume_scenario() -> None:
    """Simula un kill a mitad del extract de bancanet y verifica la
    reanudación.

    1. Borra ``extract/`` y mata la corrida justo antes de escribir
       ``<table>_historic`` (monkeypatch de ``_join_full_cohorts``):
       quedan en disco los parquets mensuales y los cohortes
       de vintage — el estado de un run matado a medias.
    2. Re-ejecuta ``run_source`` con un builder **sin** las vistas
       ``global_temp`` (sin "Hive"): cualquier reejecución de las queries
       mensuales lanzaría ``AnalysisException``. Solo funciona si cada
       parquet ya escrito se recarga (meses, cohortes,
       ``pivot_prepared``).
    3. Además borra ``mobs``/``vector_assembler`` para comprobar que el
       resto de la cadena reanuda desde donde falta.
    """
    from datapull import DataPull
    from libs import extract as extract_mod
    from libs.paths import PathsConfig
    from main_dynamic import build_session_builder, run_source

    rigid = json.loads((CONFIG_DIR / "configuration.json").read_text())
    custom = json.loads((CONFIG_DIR / "config_custom.json").read_text())
    src_b = WORKDIR_DYN / YEAR_ID / "adobe_bancanet"
    real_build = build_session_builder(local=True)

    def builder_with_views():
        spark = real_build()
        register_views(spark)
        return spark

    # --- 1) kill simulado tras los parquets mensuales -------------------
    shutil.rmtree(src_b / "extract")
    killed = {"called": False}
    orig_join = extract_mod.Extract._join_full_cohorts

    def boom(self, *a, **kw):
        killed["called"] = True
        raise RuntimeError("simulated kill mid-extract")

    extract_mod.Extract._join_full_cohorts = boom
    try:
        run_source(DataPull(rigid, custom, builder_with_views),
                   "adobe_bancanet", paths=PathsConfig(),
                   is_dynamic=True, step_filter=None,
                   cleanup=False, send_email=False)
    finally:
        extract_mod.Extract._join_full_cohorts = orig_join
    assert killed["called"], "el kill simulado nunca se ejecutó"
    # quedaron meses + cohortes de vintage, sin _historic
    extract_dir = src_b / "extract"
    months = [p for p in extract_dir.iterdir()
              if p.name.startswith("adobe_t_20")]
    assert months, "no quedaron parquets mensuales tras el kill"
    assert not (extract_dir / "adobe_t_historic").exists()

    # --- 2) re-ejecución sin vistas: solo puede recargar parquets -------
    for d in ("mobs", "vector_assembler"):
        shutil.rmtree(src_b / d, ignore_errors=True)
    ok = run_source(DataPull(rigid, custom, real_build),
                    "adobe_bancanet", paths=PathsConfig(),
                    is_dynamic=True, step_filter=None,
                    cleanup=False, send_email=False)
    if not ok:
        sys.exit("resume de adobe_bancanet falló (¿re-executó queries "
                 "mensuales sin vistas?)")
    assert (extract_dir / "adobe_t_historic" / "_SUCCESS").exists(), \
        "el _historic no se reconstruyó en la reanudación"
    print("RESUME OK — extract reanudado desde parquets mensuales")


# ---------------------------------------------------------------------------
# verificación
# ---------------------------------------------------------------------------
def _read(spark, *parts):
    path = Path(*parts).as_posix()
    return spark.read.parquet(path)


def verify() -> None:
    """Comprueba los outputs del pipeline de las dos fuentes."""
    spark = bootstrap.get_spark("adobe_e2e_verify")
    spark.sparkContext.setLogLevel("ERROR")
    src_b = WORKDIR_DYN / YEAR_ID / "adobe_bancanet"
    src_m = WORKDIR_DYN / YEAR_ID / "adobe_mobile"
    checks = []

    def check(name, cond):
        checks.append((name, bool(cond)))

    # --- extract: identidad resuelta, sin fuga de futuro ---------------
    hist = _read(spark, src_b, "extract",
                 "adobe_t_historic")
    check("extract: numcliente resuelto",
          hist.filter("numcliente is null").count() == 0)
    check("extract: sin cliente del snapshot futuro (HF)",
          hist.filter("numcliente = 'c9'").count() == 0)
    check("extract: sesión con backfill presente",
          hist.filter("session_id = '11-111-7'").count() == 2)
    check("extract: hit fuera de filtro de página no está",
          hist.filter("pagename = 'MX|OTRA|PAGINA'").count() == 0)
    # el extract emite la malla cliente×mes completa: c4 aparece pero
    # con todas las columnas de hit a null
    check("extract: c4 (sin actividad) solo meses vacíos",
          hist.filter("numcliente = 'c4'").count() > 0
          and hist.filter("numcliente = 'c4' AND file_date is not null")
          .count() == 0)
    check("extract: tfrom mensual presente",
          "tfrom" in hist.columns)

    # --- weeks: tfrom semanal ------------------------------------------
    wk_hist = _read(spark, src_b, "weekly_extract",
                    "adobe_t_historic")
    max_w = wk_hist.agg({"tfrom": "max"}).first()[0]
    check("weeks: tfrom semanal > tfrom mensual", max_w > 6)
    check("weeks: file_date conservada",
          "file_date" in wk_hist.columns)

    # --- groupby: dominios ----------------------------------------------
    gb = _read(spark, src_b, "groupby")
    gb_cols = gb.columns
    check("groupby: vars semanales", "bnt_pil_view" in gb_cols
          and "bnt_pil_view_cnt" in gb_cols and "bnt_hits" in gb_cols)
    check("groupby: T_* semanales",
          "bnt_t_session_sum" in gb_cols and "bnt_t_vista_mean" in gb_cols)
    check("groupby: dominio quincenal",
          "bnt_pil_view_q" in gb_cols)
    check("groupby: dominio last_days",
          "bnt_pil_view_ld" in gb_cols)
    check("groupby: dominio daily_seq",
          "bnt_pil_view_d" in gb_cols)
    # weekly: c2 activó cada semana -> max del flag = 1 en varias semanas
    c2w = gb.filter("numcliente='c2' AND bnt_lop_view = 1").count()
    check("groupby: c2 con LOP semanal", c2w >= 20)
    # ld: c3 solo activó últimos 12 días -> sin rows en ventanas < 12? no:
    # ld es "últimos N días": c3 aparece en todas las ventanas >= min gap
    c3_ld = gb.filter("numcliente='c3' AND bnt_pil_view_ld IS NOT NULL")\
        .collect()
    check("groupby: c3 en ventanas ld (hits en <=12d)",
          len(c3_ld) > 0 and all(r["bnt_pil_view_ld"] >= 1 for r in c3_ld))
    # dseq: c3 tiene secuencia diaria
    check("groupby: c3 dseq diaria",
          gb.filter("numcliente='c3' AND bnt_pil_view_d = 1").count() >= 10)

    # --- pivot: columnas wide -------------------------------------------
    pv = _read(spark, src_b, "pivot")
    pv_cols = pv.columns
    check("pivot: cols semanales", any(c.startswith("bnt_pil_view_")
          and not c.startswith(("bnt_pil_view_ld", "bnt_pil_view_d",
                                "bnt_pil_view_q", "bnt_pil_view_cnt"))
          for c in pv_cols))
    check("pivot: cols ld", any("bnt_pil_view_ld_" in c for c in pv_cols))
    check("pivot: cols dseq", any("bnt_pil_view_d_" in c for c in pv_cols))
    check("pivot: cols fortnight",
          any("bnt_pil_view_q_" in c for c in pv_cols))
    check("pivot: sin columnas junk all-null",
          "bnt_pil_view_ld_0" not in pv_cols)
    # keep_pivot_clients: TODOS los clientes del pivot llegan al wide;
    # los sin actividad (c4, m1) quedan con features a 0
    check("pivot: todos los clientes del pivot",
          pv.select("numcliente").distinct().count() == 5)
    c4w = pv.filter("numcliente='c4'").first()
    check("pivot: c4 presente con features a 0",
          c4w is not None and all(
              c4w[c] == 0 for c in pv_cols if c.startswith("bnt_")))

    # --- fe -------------------------------------------------------------
    feats = WORKDIR_DYN / YEAR_ID / "adobe_bancanet" / "features"
    check("fe: raw_features", (feats / "raw_features").exists())
    for t in ("sum_mt", "mean_mt", "longest_1_streak_onz_mt",
              "transitions_onz_mt", "entropy_onz_mt"):
        check(f"fe: {t}_features", (feats / f"{t}_features").exists())
    st = _read(spark, feats, "longest_1_streak_onz_mt_features")
    racha_cols = [c for c in st.columns
                  if "racha" not in c and "bnt_pil_view" in c]
    check("fe: rachas sobre vars binarias", len(racha_cols) > 0)

    # --- mobs -------------------------------------------------------------
    cm = _read(spark, src_b, "mobs", "calculate_mobs")
    mobs = {r["numcliente"]: r["mobs_adobe"] for r in cm.collect()}
    check("mobs: mobs_adobe existe", "mobs_adobe" in cm.columns)
    check("mobs: c1 antigüedad completa (capped)", mobs.get("c1") >= 6)
    check("mobs: c3 antigüedad 1 mes", mobs.get("c3") == 1)
    check("mobs: imputate_mobs escrito",
          (src_b / "mobs" / "imputate_mobs").exists())
    # imputación -99999 (PreviousFixerAdobe): la ventana _Nw exige
    # N semanas; mobs_adobe está en meses (x4) y ausente = 0
    raw_m = _read(spark, src_b, "mobs", "imputate_mobs", "raw_features")
    ids = {r["numcliente"]: r["id_msf_tools_v2_12315"]
           for r in raw_m.collect()}
    check("mobs: todos los clientes en raw_features",
          set(ids) == {"c1", "c2", "c3", "c4", "m1"})
    sm = _read(spark, src_b, "mobs", "imputate_mobs",
               "sum_mt_features")
    sm_rows = {r["id_msf_tools_v2_12315"]: r for r in sm.collect()}
    wcols = [c for c in sm.columns if c.endswith("w")]
    r_c3 = sm_rows[ids["c3"]]
    check("mobs: c3 -99999 en ventana 7w (mobs=1)",
          r_c3["sum_mt_bnt_pil_view_7w"] == -99999)
    check("mobs: c3 conserva ventana 4w (mobs=1)",
          r_c3["sum_mt_bnt_pil_view_4w"] != -99999)
    r_c4 = sm_rows[ids["c4"]]
    check("mobs: c4 (sin actividad) todo -99999",
          all(r_c4[c] == -99999 for c in wcols))
    r_c1 = sm_rows[ids["c1"]]
    check("mobs: c1 sin -99999 (ventana completa)",
          all(r_c1[c] != -99999 for c in wcols))

    # --- va ----------------------------------------------------------------
    check("va: vector_assembler",
          (src_b / "vector_assembler").exists())
    va = _read(spark, src_b, "vector_assembler")
    check("va: todos los clientes del pivot",
          va.select("numcliente").distinct().count() == 5)

    # --- resume --------------------------------------------------------------
    check("resume: pivot_prepared persistido",
          (WORKDIR_DYN / YEAR_ID / "pivot_prepared" / "_SUCCESS").exists())
    check("resume: _historic reconstruido tras kill",
          (src_b / "extract" / "adobe_t_historic" / "_SUCCESS").exists())
    check("resume: mobs re-computados tras borrado",
          (src_b / "mobs" / "imputate_mobs" / "raw_features"
           / "_SUCCESS").exists())

    # --- particionado vintage/tfrom -------------------------------------
    def _has_part_dirs(base: Path, prefix: str) -> bool:
        return base.exists() and any(
            p.is_dir() and p.name.startswith(prefix + "=")
            for p in base.iterdir())

    def _has_nested_parts(base: Path, outer: str, inner: str) -> bool:
        return base.exists() and any(
            _has_part_dirs(p, inner) for p in base.iterdir()
            if p.is_dir() and p.name.startswith(outer + "="))

    check("parquet: _historic particionado por vintage_date+tfrom",
          _has_nested_parts(src_b / "extract" / "adobe_t_historic",
                            "vintage_date", "tfrom"))
    check("parquet: weekly_extract particionado por tfrom",
          _has_nested_parts(src_b / "weekly_extract" / "adobe_t_historic",
                            "vintage_date", "tfrom"))
    check("parquet: groupby particionado",
          _has_nested_parts(src_b / "groupby", "vintage_date", "tfrom"))
    check("parquet: pivot wide particionado por vintage_date",
          _has_part_dirs(src_b / "pivot", "vintage_date"))
    check("parquet: pivot_prepared particionado por vintage",
          _has_part_dirs(WORKDIR_DYN / YEAR_ID / "pivot_prepared",
                         "vintage"))

    # --- mobile -------------------------------------------------------------
    hist_m = _read(spark, src_m, "extract", "adobe_mob_t_historic")
    check("mobile: extract resuelve numcliente",
          hist_m.filter("numcliente = 'm1'").count() > 0)
    gb_m = _read(spark, src_m, "groupby")
    check("mobile: flags Flag_interest_*",
          "mob_flag_interest_lop" in gb_m.columns)
    lop = gb_m.filter(
        "numcliente='m1' AND mob_flag_interest_lop = 1").count()
    check("mobile: flag LOP activa", lop > 0)

    # --- email: un solo mail por fuente con los controles plegados ----
    import email as _em
    eml_dir = TESTS_LOCAL / "workdir_adobe" / "emails"
    mails = list(eml_dir.glob("*.eml")) if eml_dir.exists() else []
    check("email: un mail por fuente (2 en total)", len(mails) == 2)
    bnt_mail = [p for p in mails if "bancanet" in p.name]
    if bnt_mail:
        msg = _em.message_from_bytes(bnt_mail[0].read_bytes())
        body = msg.get_payload(0).get_payload(decode=True)\
            .decode("utf-8", "replace")
        check("email: controles del extract en el reporte",
              "Input controls" in body)
        check("email: subject OK con feats",
              "OK" in (msg.get("Subject") or ""))
    else:
        check("email: reporte de bancanet existe", False)

    print("=" * 60)
    failed = [n for n, ok in checks if not ok]
    for n, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {n}")
    print("=" * 60)
    spark.stop()
    if failed:
        sys.exit(f"{len(failed)} checks failed: {failed}")
    print(f"ADOBE E2E OK — {len(checks)} checks")


def main() -> None:
    spark = bootstrap.get_spark("adobe_e2e_fixtures")
    spark.sparkContext.setLogLevel("ERROR")
    write_fixtures(spark)
    spark.stop()
    make_config()
    if WORKDIR_DYN.exists():
        shutil.rmtree(WORKDIR_DYN)
    run_pipeline()
    resume_scenario()
    verify()


if __name__ == "__main__":
    main()
