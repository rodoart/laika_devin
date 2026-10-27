"""E2E golden-master del datapull sobre fuentes reales + sinteticas.

Corre ``payroll_streaks`` y ``payroll_income`` (bloques reales de
``configuration.json``, solo parcheados a ``global_temp``) mas
``bureau_pull``, ``liabilities`` y ``rev_balance`` (bloques completados
sinteticamente — en el config real estan truncados/ausentes) sobre data
sintetica representativa, y congela TODOS los parquets del workdir en un
golden snapshot (``golden_regression.json``).

El objetivo es detectar regresiones: tras cualquier optimizacion,
``verify()`` re-corre el pipeline en un workdir nuevo y exige que cada
parquet sea identico (schema + contenido, tolerancia en floats).

Uso:
    python tests/local/regression_e2e.py            # run completo + golden
    python tests/local/regression_e2e.py verify     # re-run en workdir limpio
                                                    # + diff vs golden
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap  # noqa: E402  (shims + HADOOP_HOME + PYSPARK_PYTHON)

TESTS_DIR = Path(__file__).resolve().parent
WORKDIR = TESTS_DIR / "workdir_regression"
FIXTURES = WORKDIR / "fixtures"
WORKDIR_DYN = TESTS_DIR / "workdir_regression_dyn"
CONFIG_DIR = TESTS_DIR / "config_regression"
GOLDEN = TESTS_DIR / "golden_regression.json"

YEAR_ID = "202601"
V1, V2 = "2025-08-31", "2025-09-30"

# Pivot: clientes variados — actividad completa, huecos, cross-vintage,
# sin actividad, solo-en-algunas-fuentes.
_D1, _D2 = date(2025, 8, 31), date(2025, 9, 30)
PIVOT_ROWS = [
    ("101", _D1), ("101", _D2),   # actividad en todo
    ("102", _D1),                 # payroll via num_cliente_2 (stack col)
    ("103", _D2),                 # huecos en payroll + bureau
    ("104", _D1),                 # SIN actividad en ninguna fuente
    ("105", _D1), ("105", _D2),   # huecos + liabilities
    ("106", _D2),                 # bureau + rev_balance pesados
]

SOURCES = ["payroll_streaks", "payroll_income", "bureau_pull",
           "liabilities", "rev_balance", "channels", "financiera",
           "giros_credito"]

# ``credit_card_processed_data`` de prod NO trae ``fec_apertura``
# (select_variables[1] = 'numcliente, mob, mis_dt') — es el crash del
# mail (``credit_card_mobs`` exigia fec_apertura). El datapull original
# b7d2ecc tiene el mismo requisito, asi que para el diff orig-vs-curr el
# fixture la incluye (path seniority compartido). Con
# ``CREDIT_CARD_FEC=False`` la config y el fixture quedan prod-fieles y
# se ejercita el path nuevo ``mob + tfrom`` (solo corre en el actual).
CREDIT_CARD_FEC = os.environ.get("REG_CREDIT_CARD_FEC", "1") == "1"

# ``vintage`` del pivot maestro de prod es STRING (no date) — el mail
# ``TypeError: can only concatenate str (not "relativedelta") to str``
# en ``_get_range_dates`` al re-preparar el pivot. El fixture usa dates
# por defecto (el original b7d2ecc tiene el mismo bug latente y no
# correria); ``REG_PIVOT_VINTAGE_STR=1`` lo deja prod-fiel.
PIVOT_VINTAGE_STR = os.environ.get("REG_PIVOT_VINTAGE_STR", "0") == "1"


def _months(start: str, end: str):
    """Primeros de mes 'YYYY-MM-01' en el rango [start, end] (YYYY-MM)."""
    y0, m0 = map(int, start.split("-"))
    y1, m1 = map(int, end.split("-"))
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}-{m:02d}-01")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


ALL_MONTHS = _months("2025-02", "2025-09")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
def write_fixtures(spark) -> None:
    """Escribe pivot + vistas de input + tablas auxiliares pre-colocadas."""
    if WORKDIR.exists():
        shutil.rmtree(WORKDIR)
    FIXTURES.mkdir(parents=True)

    # --- pivot --------------------------------------------------------
    pivot_rows = ([(c, str(v)) for c, v in PIVOT_ROWS]
                  if PIVOT_VINTAGE_STR else PIVOT_ROWS)
    spark.createDataFrame(
        pivot_rows, ["numcliente", "vintage"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "pivot"))

    # --- asigna_nomina (payroll_streaks + payroll_income) -------------
    # cols: process_date, num_cliente{,_2,_3}, tef{,_1,_2},
    #       sum_importe{,_1,_2}
    rows = []
    def asigna(cli, cli2, cli3, month, tef, t1, t2):
        rows.append((month, cli, cli2, cli3, tef, t1, t2,
                     (tef or 0) * 100 + 11, (t1 or 0) * 100 + 22,
                     (t2 or 0) * 100 + 33))
    # OJO: el aggregate hace join nomina.tef con asigna.tef_1/tef_2 por
    # cliente — los tefs deben coincidir o la tabla queda vacia.
    for mo in ALL_MONTHS:                       # 101: los 8 meses
        asigna("101", "", "", mo, 2, 3, 5)
    for mo in _months("2025-03", "2025-08"):    # 102: via num_cliente_2
        asigna("", "102", "", mo, 4, 2, 6)
    for mo in ("2025-05-01", "2025-07-01", "2025-09-01"):  # 103 huecos
        asigna("103", "", "", mo, 1, 1, 2)
    for mo in ("2025-03-01", "2025-06-01", "2025-08-01"):  # 105 huecos
        asigna("105", "", "", mo, 3, 3, 4)
    asigna("106", "", "", "2025-08-01", 2, 2, 2)          # 106 suelto
    spark.createDataFrame(
        rows, ["process_date", "num_cliente", "num_cliente_2",
               "num_cliente_3", "tef", "tef_1", "tef_2",
               "sum_importe", "sum_importe_1", "sum_importe_2"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "asigna"))

    # --- nomina (detalle diario payroll_streaks) ------------------------
    nrows = []
    def nomina(cli, day, tef, imp):
        nrows.append((day[:8] + "01", day, cli, "", "", tef, imp))
    # tef debe matchear tef_1/tef_2 del cliente en asigna; ademas hace
    # falta cobertura de los 4 payment_type (semanal/quincenal/mensual/
    # indeterminado) — el pivot de payment_type_per_month crea columnas
    # solo para los valores presentes y greatest() las exige todas.
    for mo in _months("2025-03", "2025-09"):
        for day, tef in (("01", 3), ("08", 3), ("15", 5), ("22", 5)):
            nomina("101", mo[:8] + day, tef, 1500.0)
    nomina("102", "2025-04-11", 2, 1200.0)
    nomina("102", "2025-04-25", 6, 1100.0)
    nomina("102", "2025-06-11", 2, 1100.0)
    nomina("102", "2025-06-25", 6, 1150.0)
    nomina("103", "2025-07-10", 1, 900.0)
    nomina("103", "2025-09-11", 2, 950.0)
    nomina("105", "2025-06-15", 3, 800.0)
    nomina("105", "2025-08-14", 4, 850.0)
    spark.createDataFrame(
        nrows, ["process_date", "fecha", "num_cliente", "num_cliente_2",
                "num_cliente_3", "tef", "importe"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "nomina"))

    # --- adv_colocacion (bureau_pull extract + mobs_preparation) --------
    brows = []
    def buro(cli, mo, a5, sdo, dep, lin, pag, mop):
        brows.append((cli, mo[:8] + "28", a5, sdo, dep, lin, pag, mop))
    for i, mo in enumerate(_months("2025-02", "2025-08")):   # 106 bureau
        buro("106", mo, 14 + i, 5000 + i * 100, 300, 20000, 450 + i, 1)
    for i, mo in enumerate(_months("2025-02", "2025-07")):   # 101
        buro("101", mo, 30 + i, 800 + i * 50, 100, 9000, 120 + i, 0)
    buro("103", "2025-03-28", 8, 900, 50, 4000, 90, 2)       # 103 suelto
    buro("105", "2025-04-28", 20, 700, 60, 5000, 100, 0)
    spark.createDataFrame(
        brows, ["cliente", "fe_informacion", "a5", "sdovenamex",
                "sdodepotro", "lineamex", "pagamex", "mop0tcamex"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "adv_colocacion"))

    # --- liabilities_test2 (mobs_preparation + queries_liabilities) -----
    lrows = []
    snaps = ["maestra", "perfiles", "perfiles_ejecutivo", "cheques",
             "otros"]
    for i, mo in enumerate(_months("2025-02", "2025-08")):
        lrows.append(("101", mo[:8] + "28", "2020-01-15",
                      f"CTA-101-{i}", snaps[i % 5]))
        lrows.append(("105", mo[:8] + "28", "2021-06-01",
                      f"CTA-105-{i}", snaps[(i + 2) % 5]))
    lrows.append(("106", "2025-06-28", "2025-05-01", "CTA-106", "maestra"))
    spark.createDataFrame(
        lrows, ["num_cliente", "inf_dt", "fec_apertura", "num_contrato",
                "snapshot"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "liabilities_test2"))

    # --- credit_card_processed_data (mobs_preparation) -----------------
    if CREDIT_CARD_FEC:
        spark.createDataFrame(
            [("101", "2025-04-28", 40, "2018-03-10"),
             ("105", "2025-05-28", 25, "2019-11-20"),
             ("106", "2025-06-28", 9, "2023-01-05")],
            ["num_cliente", "mis_dt", "mob", "fec_apertura"]
        ).write.mode("overwrite").parquet(
            str(FIXTURES / "credit_card"))
    else:
        # prod-fiel: la tabla no trae fec_apertura (crash del mail —
        # ejercita el path ``mob + tfrom`` de credit_card_mobs)
        spark.createDataFrame(
            [("101", "2025-04-28", 40),
             ("105", "2025-05-28", 25),
             ("106", "2025-06-28", 9)],
            ["num_cliente", "mis_dt", "mob"]
        ).write.mode("overwrite").parquet(
            str(FIXTURES / "credit_card"))

    # --- onus_capt_snap (extract de liabilities) -------------------------
    orows = []
    for mo in _months("2025-02", "2025-08"):
        orows.append(("101", mo[:8] + "28", 12000.0, 500.0, "PGM-A"))
        orows.append(("105", mo[:8] + "28", 8000.0, 300.0, "PGM-B"))
    orows.append(("103", "2025-05-28", 4000.0, 100.0, "PGM-A"))
    orows.append(("103", "2025-07-28", 4500.0, 150.0, "PGM-A"))
    spark.createDataFrame(
        orows, ["num_cliente", "inf_dt", "saldo_total", "saldo_pgom",
                "producto_pgom"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "onus"))

    # --- maestra auxiliar de liabilities --------------------------------
    # <source>/auxiliar/outputs/maestra — la lee LiabilitiesPullAggregate
    mrows = []
    prods = ["MAESTRA", "PERFILES", "PGJ", "CHEQUES", "OTRO"]
    for v in (V1, V2):
        for cli, base in (("101", 1000.0), ("105", 600.0), ("103", 300.0)):
            for tf in range(6):
                for p_i, prod in enumerate(prods):
                    if cli == "103" and prod not in ("MAESTRA", "CHEQUES"):
                        continue
                    mrows.append((cli, v, float(tf), base + tf * 10 + p_i,
                                  prod))
    spark.createDataFrame(
        mrows, ["numcliente", "vintage_date", "tfrom", "saldo_pgom",
                "producto"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "maestra"))

    # --- ccp_t_m (extract de rev_balance) -------------------------------
    crows = []
    for mo in _months("2025-03", "2025-09"):
        crows.append((mo[:8] + "28", "A1", "101", 1.9, 60))
        crows.append((mo[:8] + "28", "A6", "106", 2.4, 30))
    crows.append(("2025-07-28", "A3", "103", 1.5, 12))
    spark.createDataFrame(
        crows, ["proc_dt", "crd_acct_nbr", "num_cliente", "eir",
                "monthsonbooks"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "ccp"))

    # --- epp / plan (inputs ya-agregados del aggregate rev_balance) -----
    # Van en <workdir>/<year>/rev_balance/extract/{epp,plan} (se copian
    # tras el run del extract, ver run_pipeline).
    erows = []
    prows = []
    for v in (V1, V2):
        for acct, cli_base in (("A1", 900.0), ("A6", 700.0)):
            for tf in range(6):
                erows.append((v[:8] + "28", 1, "4DB01", 24.0, 6.0, 19.0,
                              cli_base * 2, cli_base,
                              "2024-11-15", acct, v, float(tf)))
                for pn in ("10", "20", "90400"):
                    prows.append((pn + "55", cli_base / 3, 44.0, 21.0,
                                  22.0, 23.0, acct, v, float(tf)))
    spark.createDataFrame(
        erows, ["proc_dt", "estatus", "promo_id", "plazo", "plazo_actual",
                "tasa", "original_balance", "current_balance",
                "Fecha_Aceptacion", "crd_acct_nbr", "vintage_date",
                "tfrom"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "epp"))
    spark.createDataFrame(
        prows, ["plan_nbr", "current_balance", "int_grav_exent", "tasa",
                "tasa_compras", "tasa_cash", "crd_acct_nbr",
                "vintage_date", "tfrom"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "plan"))

    # --- cat_promos / ecm (vistas) --------------------------------------
    spark.createDataFrame(
        [("4DB01", "Disponible"), ("5E77", "Efectivo")],
        ["promo_id", "grupo"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "cat_promos"))
    spark.createDataFrame(
        [("2025-06-28", "101", "A1"), ("2025-07-28", "106", "A6"),
         ("2025-08-28", "103", "A3")],
        ["proc_dt", "numcliente", "crd_acct_nbr"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "ecm"))

    # --- total_multi_cte (channels extract) -----------------------------
    # columnas utiles del select reducido del harness
    trows = []
    for i, mo in enumerate(_months("2025-02", "2025-08")):
        trows.append((mo[:8] + "28", 101, 1, 3 + i, 1, 1,
                      2 + i, 1, 1, 0, 5 + i, 0))
        trows.append((mo[:8] + "28", 105, 0, 1 + i, 0, 1,
                      1, 2, 0, 1, 2, 1))
    trows.append(("2025-05-28", 103, 1, 2, 0, 0, 1, 1, 1, 1, 3, 0))
    spark.createDataFrame(
        trows, ["proc_dt", "NUM_CLIENTE", "ATM", "TNX_ATM", "BNET_EMP",
                "BNET_PERS", "TNX_BNET_EMPR", "TNX_BANCANET_PERS", "IVR",
                "CSR", "TNX_IVR", "TNX_CSR"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "total_multi"))

    # --- finaypns (financiera extract) ----------------------------------
    # cve_ayp -> b1 via cves_dict del aggregate: 2030 adela, 1011
    # cheques, 1055 tarjetas, 8000 na; 9999 queda sin mapeo (b1 null ->
    # no suma en ningun feature)
    frows = []
    for i, mo in enumerate(_months("2025-02", "2025-08")):
        frows.append((mo[:8] + "28", "101", 2030, 1000 + i * 10))
        frows.append((mo[:8] + "28", "101", 1011, 500 + i * 5))
        frows.append((mo[:8] + "28", "105", 1055, 200 + i * 3))
    frows.append(("2025-06-28", "103", 8000, 150))
    frows.append(("2025-07-28", "103", 9999, 999))
    spark.createDataFrame(
        frows, ["proc_dt", "numcliente", "cve_ayp", "saldo"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "finaypns"))

    # --- mgiros (giros_credito extract) ---------------------------------
    # macrogiro pasa por quita_acentos (lower + sin acentos) -> c2; la
    # cuenta se resuelve a numcliente via el pivot enriquecido con ecm
    grows = []
    for i, mo in enumerate(_months("2025-03", "2025-09")):
        grows.append((mo[:8] + "28", "A1", "AEROLÍNEAS", "vuelos",
                      1200 + i * 10, 2))
        grows.append((mo[:8] + "28", "A1", "gasolina", "combustible",
                      800, 4))
        grows.append((mo[:8] + "28", "A6", "Alojamiento", "hotel c",
                      3000, 1))
    grows.append(("2025-07-28", "A3", "Restaurantes", "comida", 450, 3))
    # c2 null -> fillna 'sin clasificar' en el aggregate
    grows.append(("2025-08-28", "A3", None, "misc", 300, 1))
    # cuenta fuera del ecm: la fila no matchea el pivot (left join)
    grows.append(("2025-07-28", "AX", "aerolineas", "vuelos", 999, 1))
    spark.createDataFrame(
        grows, ["proc_dt", "id_cuenta16", "macrogiro", "giro",
                "purchase", "purchasetx"]
    ).write.mode("overwrite").parquet(str(FIXTURES / "mgiros"))


VIEWS = {
    "asigna_nomina_dt1_t_m": "asigna",
    "nomina_dt1_t_m": "nomina",
    "adv_colocacion_1_t_m": "adv_colocacion",
    "liabilities_test2": "liabilities_test2",
    "credit_card_processed_data": "credit_card",
    "onus_capt_snap_t_m": "onus",
    "ccp_t_m": "ccp",
    "cat_promos": "cat_promos",
    "ecm": "ecm",
    "total_multi_cte_t_m": "total_multi",
    "int_nxs_se_finaypns_snap_t_m": "finaypns",
    "int_xs_mgrs_mgiros_hist_t_m": "mgiros",
}


def register_views(spark) -> None:
    """Expone las fixtures como ``global_temp.*`` — se rellama en cada
    builder (resiste ``reset_spark_session``)."""
    for view, fixture in VIEWS.items():
        spark.read.parquet(str(FIXTURES / fixture))\
            .createOrReplaceGlobalTempView(view)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
def make_config() -> None:
    """Genera ``config_regression/`` a partir del configuration.json."""
    CONFIG_DIR.mkdir(exist_ok=True)
    rigid = json.loads(
        (PROJECT_ROOT / "config" / "configuration.json").read_text(
            encoding="utf-8"))

    # ---- fuentes reales: solo se repunta libname -> global_temp --------
    for src in ("payroll_streaks", "payroll_income"):
        ex = rigid[src]["extract"]
        ex["libname"] = ["global_temp"] * len(ex["libname"])

    # ---- bureau_pull: extract inyectado + mt/query_dict recortados -----
    bvars = ["a5", "sdovenamex", "sdodepotro", "lineamex", "pagamex",
             "mop0tcamex"]
    rigid["bureau_pull"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["adv_colocacion_1_t_m"],
            "select_variables": [
                "cliente as numcliente, fe_informacion, a5, sdovenamex,"
                " sdodepotro, lineamex, pagamex, mop0tcamex"],
            "type_date": [""],
            "retraso": [3],
            "name_mis": ["fe_informacion"],
            "table_pivote": ["master_data"],
            "key_pivote": ["numcliente"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["numcliente"],
        },
        "missing_treatment": {
            "fill_variables": {
                "variable": bvars,
                "source": ["buro"] * len(bvars),
                "metodo": ["historico"] * len(bvars),
            },
            "fill_methods": {
                "metodo": ["fill_historic"],
                "grouped": [["numcliente", "vintage_date"]],
                "valor": ["1"],
                "id_variables": ["historico"],
            },
            "mobs": {
                "table_name": ["adv_colocacion_1_t_m_historic",
                               "liabilities_test2_historic",
                               "credit_card_processed_data_historic"],
                "source": ["bureau", "liabilities", "credit_card"],
                "sources": ["bureau", "liabilities", "credit_card"],
                "mobs_tipo": ["mobs_bureau"],
            },
        },
        "group_by": {
            "table_name": "adv_colocacion_1_t_m_historic",
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            "query_dict": {
                "variable": bvars,
                "agg": ["sum", "sum", "sum", "max", "sum", "max"],
                # a5 debe conservar su nombre: getCostumerBureau hace
                # referencia literal a la columna (mob/hit/nmiss)
                "feature": ["a5"] + [f"buro_{v}" for v in bvars[1:]],
            },
        },
        "pivoting": {
            "source": "bureau_pull",
            "table_name": "adv_colocacion_1_t_m_historic",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "groupby", "mt", "pivoting", "fe",
                       "mobs", "va"],
    }

    # ---- liabilities: bloque sintetico completo -------------------------
    rigid["liabilities"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["onus_capt_snap_t_m"],
            "select_variables": [
                "num_cliente as numcliente, inf_dt,"
                " cast(saldo_total as double) as saldo_total,"
                " cast(saldo_pgom as double) as saldo_pgom,"
                " producto_pgom"],
            "type_date": [""],
            "retraso": [1],
            "name_mis": ["inf_dt"],
            "table_pivote": ["master_data"],
            "key_pivote": ["numcliente"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["numcliente"],
        },
        "missing_treatment": {
            "fill_variables": {
                "variable": ["saldo_total", "saldo_pgom"],
                "source": ["liabilities", "liabilities"],
                "metodo": ["historico", "historico"],
            },
            "fill_methods": {
                "metodo": ["fill_historic"],
                "grouped": [["numcliente", "vintage_date"]],
                "valor": ["1"],
                "id_variables": ["historico"],
            },
            "mobs": {
                "table_name": ["liabilities_test2_historic",
                               "credit_card_processed_data_historic"],
                "source": ["liabilities", "credit_card"],
                "sources": ["liabilities", "credit_card"],
                "mobs_tipo": ["mobs_liabilities"],
            },
        },
        "group_by": {
            "table_name": "onus_capt_snap_t_m_historic",
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            # sel_cols es la variable comun a TODAS las vistas
            # (maes/perf/peje/cheq/otro/pgom) — el rename la exige en
            # cada una; pgom_cols alimenta df_pgom y debe incluir las
            # claves del group_by (el select fijo es numcliente+pgom_cols)
            "sel_cols": ["saldo_pgom"],
            "pgom_cols": ["saldo_pgom", "producto_pgom",
                          "vintage_date", "tfrom"],
            "filter_cols": ["saldo_pgom"],
            "query_dict": {"variable": ["saldo_pgom"], "agg": ["sum"]},
            "PGMT_QUERY_DICT": {"variable": ["saldo_pgom"],
                                "agg": ["sum"]},
            "maes_new_cols": ["sdo_maes"],
            "perf_new_cols": ["sdo_perf"],
            "peje_new_cols": ["sdo_peje"],
            "cheq_new_cols": ["sdo_cheq"],
            "otro_new_cols": ["sdo_otro"],
            "pgom_new_cols": ["sdo_pgom"],
        },
        "pivoting": {
            "source": "liabilities",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "mt", "groupby", "pivoting", "fe",
                       "mobs", "va"],
    }

    # ---- rev_balance: bloque sintetico completo -------------------------
    rigid["rev_balance"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["ccp_t_m"],
            "select_variables": [
                "proc_dt, crd_acct_nbr, bigint(num_cliente) as numcliente,"
                " cast(eir as double) as eir, monthsonbooks"],
            "type_date": [""],
            "retraso": [0],
            "name_mis": ["proc_dt"],
            "table_pivote": ["master_data"],
            "key_pivote": ["crd_acct_nbr"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["crd_acct_nbr"],
        },
        "group_by": {
            "table_names": {
                "epp": "epp",
                "plan": "plan",
                "cpp": "ccp_t_m_historic",
                "catPromosID": "global_temp.cat_promos",
            },
            "primaryKey": ["crd_acct_nbr", "vintage_date", "tfrom"],
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            "querys_dict": {"epp_dict": {}},
        },
        "pivoting": {
            "source": "rev_balance",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "groupby", "pivoting", "fe", "va"],
    }

    # ---- channels: bloque sintetico (subset del query_str prod) --------
    rigid["channels"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["total_multi_cte_t_m"],
            "select_variables": [
                "cast(last_day(to_date(proc_dt)) as string) as proc_dt,"
                "cast(NUM_CLIENTE as double) as numcliente,"
                "cast(ATM as double) as atm,"
                "cast(TNX_ATM as double) as tnx_atm,"
                "cast(BNET_EMP as double) as bnet_emp,"
                "cast(BNET_PERS as double) as bnet_pers,"
                "cast(TNX_BNET_EMPR as double) as tnx_bnet_empr,"
                "cast(TNX_BANCANET_PERS as double) as tnx_bancanet_pers,"
                "cast(IVR as double) as ivr,"
                "cast(CSR as double) as csr,"
                "cast(TNX_IVR as double) as tnx_ivr,"
                "cast(TNX_CSR as double) as tnx_csr"],
            "type_date": [""],
            "retraso": [2],
            "name_mis": ["proc_dt"],
            "table_pivote": ["master_data"],
            "key_pivote": ["numcliente"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["numcliente"],
        },
        "missing_treatment": {
            "fill_variables": {"variable": ["atm", "tnx_atm"],
                               "source": ["channels", "channels"],
                               "metodo": ["Real 0", "Real 0"]},
            "fill_methods": {"metodo": ["fill_value"],
                             "grouped": ["numcliente"],
                             "valor": ["Real 0"],
                             "id_variables": [""]},
            "mobs": {
                "table_name": ["liabilities_test2_historic",
                               "credit_card_processed_data_historic",
                               "adv_colocacion_1_t_m_historic"],
                "source": ["liabilities", "credit_card", "bureau"],
                "sources": ["bureau", "liabilities", "credit_card"],
                "mobs_tipo": ["max_liab_cc"],
            },
        },
        "group_by": {
            "table_name": "total_multi_cte_t_m_historic",
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            "query_str": (
                "select cast(numcliente as string), atm, tnx_atm,"
                " bnet_emp, bnet_pers, tnx_bnet_empr, tnx_bancanet_pers,"
                " ivr, csr, tnx_ivr, tnx_csr, vintage_date, tfrom"
                " from channels"),
            "query_dict": {
                "flags": {"bnet": ["bnet_emp", "bnet_pers"],
                          "audiom": ["ivr", "csr"]},
                "sums": {"tnx_bnet": ["tnx_bnet_empr",
                                     "tnx_bancanet_pers"],
                         "tnx_audiom": ["tnx_ivr", "tnx_csr"]},
            },
        },
        "pivoting": {
            "source": "channels",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "groupby", "pivoting", "fe", "mobs",
                       "va"],
    }

    # ---- financiera: bloque sintetico (subset de productos prod) -------
    rigid["financiera"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["int_nxs_se_finaypns_snap_t_m"],
            "select_variables": [
                "proc_dt, numcliente, cast(cve_ayp as double) as cve_ayp,"
                " cast(saldo as double) as saldo, 0.0 as saldoaj"],
            "type_date": [""],
            "retraso": [2],
            "name_mis": ["proc_dt"],
            "table_pivote": ["master_data"],
            "key_pivote": ["numcliente"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["numcliente"],
        },
        "missing_treatment": {
            "fill_variables": {"variable": ["a5"],
                               "source": ["buro"],
                               "metodo": ["historico"]},
            "fill_methods": {"metodo": ["fill_historic"],
                             "grouped": [["numcliente", "vintage_date"]],
                             "valor": ["1"],
                             "id_variables": ["historico"]},
            "mobs": {
                "table_name": ["liabilities_test2_historic",
                               "credit_card_processed_data_historic",
                               "adv_colocacion_1_t_m_historic"],
                "source": ["liabilities", "credit_card", "bureau"],
                "sources": ["bureau", "liabilities", "credit_card"],
                "mobs_tipo": ["mobs_bureau"],
            },
        },
        "group_by": {
            "table_name": "int_nxs_se_finaypns_snap_t_m_historic",
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            "query_dict": {
                "variable": [["adela"], ["cheques"], ["tarjetas"],
                             ["tarjetas", "adela"],
                             ["cheques", "debito", "na"]],
                "agg": ["sum"] * 5,
                "feature": ["adela_b1", "cheques_b1", "tarjetas_b1",
                            "assets_b1", "liabilities_b1"],
            },
        },
        "pivoting": {
            "source": "financiera",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "groupby", "pivoting", "fe", "mobs",
                       "va"],
    }

    # ---- giros_credito: bloque sintetico (subset del mapa prod) --------
    # key_pivote=crd_acct_nbr: ejercita el pivot enriquecido con ecm —
    # es el crash del mail (pivot_prepared sin crd_acct_nbr)
    rigid["giros_credito"] = {
        "extract": {
            "libname": ["global_temp"],
            "table_name": ["int_xs_mgrs_mgiros_hist_t_m"],
            "select_variables": [
                "proc_dt, id_cuenta16 as crd_acct_nbr,"
                " quita_acentos(macrogiro) as c2,"
                " quita_acentos(giro) as c3,"
                " cast(purchase as double) as purchase,"
                " cast(purchasetx as double) as purchasetx"],
            "type_date": [""],
            "retraso": [2],
            "name_mis": ["proc_dt"],
            "table_pivote": ["p"],
            "key_pivote": ["crd_acct_nbr"],
            "name_vintage_pivote": ["vintage"],
            "key_source": ["crd_acct_nbr"],
        },
        "missing_treatment": {
            "fill_variables": {"variable": ["a5"],
                               "source": ["buro"],
                               "metodo": ["historico"]},
            "fill_methods": {"metodo": ["fill_historic"],
                             "grouped": [["numcliente", "vintage_date"]],
                             "valor": ["1"],
                             "id_variables": ["historico"]},
            "mobs": {
                "table_name": ["liabilities_test2_historic",
                               "credit_card_processed_data_historic",
                               "adv_colocacion_1_t_m_historic"],
                "source": ["liabilities", "credit_card", "bureau"],
                "sources": ["bureau", "liabilities", "credit_card"],
                "mobs_tipo": ["mobs_bureau"],
            },
        },
        "group_by": {
            "table_name": "int_xs_mgrs_mgiros_hist_t_m_historic",
            "group_by_key": ["numcliente", "vintage_date", "tfrom"],
            "query_dict": {
                "aerolineas": "airline",
                "alojamiento": "hotel",
                "gasolina": "gasoline",
                "restaurantes": "restaurant",
                "sin clasificar": "unclassified",
            },
        },
        "pivoting": {
            "source": "giros_credito",
            "pivot_key": ["numcliente", "vintage_date", "tfrom"],
        },
        "steps_list": ["extract", "groupby", "pivoting", "fe", "mobs",
                       "va"],
    }

    # ---- claves globales que apuntan a fixtures -------------------------
    mp = rigid["mobs_preparation"]
    mp["libname"] = ["global_temp"] * 3
    mp["select_variables"][1] = (
        "bigint(num_cliente) as numcliente, mob, mis_dt"
        + (", fec_apertura" if CREDIT_CARD_FEC else ""))
    mp["select_variables"][2] = (
        "cliente as numcliente, to_date(fe_informacion) as fe_informacion,"
        " a5")
    rigid["query_ecm"] = (
        "SELECT proc_dt, numcliente, crd_acct_nbr FROM global_temp.ecm")
    rigid["queries_liabilities"] = [
        q.replace("gcprcmsbx_work.liabilities_test2",
                  "global_temp.liabilities_test2")
        for q in rigid["queries_liabilities"]]

    (CONFIG_DIR / "configuration.json").write_text(
        json.dumps(rigid, indent=4, ensure_ascii=False), encoding="utf-8")

    custom = {
        "sources": SOURCES,
        "working_path": WORKDIR_DYN.as_posix(),
        "year_id": YEAR_ID,
        # period=3: get_months(3,6)=[3,6] y las ventanas de fe sobre
        # tfroms mensuales/semanales siempre incluyen _3m/_6m(_w) —
        # la imputacion de mobs las exige (otros periodos desalinean
        # ventanas vs meses a imputar, p.ej. 4->[4,6] con solo _4m).
        "n_mis": "6",
        "period": "3",
        "pivot_path": (FIXTURES / "pivot").as_posix(),
        "lista_trans": ["sum", "mean", "min", "max", "std",
                        "sum_mt", "mean_mt", "min_mt", "max_mt",
                        "trend_mt"],
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
def _prepare_aux_dirs(working_path: Path) -> None:
    """Copia fixtures pre-colocadas que los agregados leen dentro del
    workdir: ``liabilities/auxiliar/outputs/maestra`` y
    ``rev_balance/extract/{epp,plan}``."""
    liab_aux = (working_path / YEAR_ID / "liabilities" / "auxiliar"
                / "outputs")
    liab_aux.mkdir(parents=True, exist_ok=True)
    dst = liab_aux / "maestra"
    if not dst.exists():
        shutil.copytree(FIXTURES / "maestra", dst)
    rev_extract = working_path / YEAR_ID / "rev_balance" / "extract"
    rev_extract.mkdir(parents=True, exist_ok=True)
    for name in ("epp", "plan"):
        dst = rev_extract / name
        if not dst.exists():
            shutil.copytree(FIXTURES / name, dst)


def run_pipeline(working_path: Path = WORKDIR_DYN) -> None:
    """Corre las 5 fuentes con el builder que re-registra las vistas."""
    from datapull import DataPull
    from libs.paths import PathsConfig
    from main_dynamic import build_session_builder, run_source

    real_build = build_session_builder(local=True)

    def builder():
        spark = real_build()
        register_views(spark)
        return spark

    rigid = json.loads((CONFIG_DIR / "configuration.json").read_text())
    custom = json.loads((CONFIG_DIR / "config_custom.json").read_text())
    custom["working_path"] = working_path.as_posix()
    datapull = DataPull(rigid, custom, builder)
    _prepare_aux_dirs(working_path)
    for source in custom["sources"]:
        ok = run_source(datapull, source, paths=PathsConfig(),
                        is_dynamic=True, step_filter=None,
                        cleanup=False, send_email=False)
        if not ok:
            sys.exit(f"pipeline failed for {source}")


# ---------------------------------------------------------------------------
# golden snapshot / verify
# ---------------------------------------------------------------------------
def _df_fingerprint(spark, path: Path):
    """Schema + hash del contenido de un parquet.

    Agnostico al orden de filas y columnas (pivot no garantiza orden):
    columnas ordenadas alfabeticamente, filas ordenadas por todos sus
    valores. ``id_msf_tools_v2_12315`` (``monotonically_increasing_id``)
    no es determinista y se excluye; floats redondeados a 6 decimales y
    columnas objeto (vectores, fechas) pasadas a string."""
    df = spark.read.parquet(path.as_posix())
    pdf = df.toPandas()
    drop = [c for c in ("id_msf_tools_v2_12315",) if c in pdf.columns]
    pdf = pdf.drop(columns=drop)
    pdf = pdf.reindex(sorted(pdf.columns), axis=1)
    for c in pdf.columns:
        if pdf[c].dtype.kind == "f":
            pdf[c] = pdf[c].round(6)
        elif pdf[c].dtype == object:
            pdf[c] = pdf[c].astype(str)
    pdf = pdf.sort_values(by=list(pdf.columns),
                          na_position="last").reset_index(drop=True)
    payload = {
        # listas, no tuplas: tras json.loads el golden tambien son listas
        "schema": sorted([f.name, f.dataType.simpleString()]
                         for f in df.schema.fields
                         if f.name not in drop),
        "n": len(pdf),
        "hash": hashlib.sha256(
            pd.util.hash_pandas_object(pdf, index=True).values.tobytes()
        ).hexdigest(),
    }
    return payload


def _parquet_dirs(root: Path):
    """Dirs con ``_SUCCESS`` bajo ``root`` (particiones de mas abajo no
    cuentan — se lee la raiz del parquet; ``_meta`` del extract son
    marcadores/controles, no datos del pipeline)."""
    out = []
    for p in sorted(root.rglob("_SUCCESS")):
        d = p.parent
        # saltar subdirs de particion (vintage_date=..., tfrom=...)
        if "=" in d.name:
            continue
        if "_meta" in d.parts:
            continue
        out.append(d)
    return out


# Fuentes con ``weeks`` integrado en el extract: su
# ``extract/<t>_historic`` equivale al antiguo
# ``weekly_extract/<t>_historic`` — el viejo historic con tfrom mensual
# y los dirs ``filtered_*`` ya no existen.
WEEKLY_INTEGRATED_SOURCES = ("payroll_streaks",)


def _equiv_golden_rel(rel: str) -> Optional[str]:
    """Rel de golden -> rel del run nuevo que contiene lo mismo.

    ``None`` si el dir ya no existe porque su contenido migró al
    ``extract/`` integrado (se verifica por el otro lado del mapeo).
    """
    for src in WEEKLY_INTEGRATED_SOURCES:
        if f"/{src}/weekly_extract/" in rel:
            return None if "/filtered_" in rel else \
                rel.replace("/weekly_extract/", "/extract/")
        if f"/{src}/extract/" in rel:
            return None
    return rel


def snapshot() -> None:
    """Recorre el workdir y congela schema+contenido de cada parquet."""
    spark = bootstrap.get_spark("regression_snapshot")
    gold = {}
    for d in _parquet_dirs(WORKDIR_DYN):
        rel = d.relative_to(WORKDIR_DYN).as_posix()
        gold[rel] = _df_fingerprint(spark, d)
    GOLDEN.write_text(json.dumps(gold, indent=1, sort_keys=True),
                      encoding="utf-8")
    spark.stop()
    print(f"GOLDEN: {len(gold)} parquets -> {GOLDEN.name}")


def verify() -> None:
    """Re-corre el pipeline en un workdir limpio y compara con golden."""
    if not GOLDEN.exists():
        sys.exit("no hay golden_regression.json — corre sin args primero")
    gold = json.loads(GOLDEN.read_text(encoding="utf-8"))

    tmp = Path(tempfile.mkdtemp(prefix="regression_verify_"))
    try:
        run_pipeline(tmp)
        spark = bootstrap.get_spark("regression_verify")
        checks, fails = 0, []
        seen = {}
        for d in _parquet_dirs(tmp):
            rel = d.relative_to(tmp).as_posix()
            seen[rel] = _df_fingerprint(spark, d)
        spark.stop()

        for rel, ref in gold.items():
            equiv = _equiv_golden_rel(rel)
            if equiv is None:
                # layout viejo sin sucesor directo; su contenido se
                # verifica por el otro lado de la equivalencia
                continue
            checks += 1
            cur = seen.get(equiv)
            if cur is None:
                # Passthrough de mobs: ``imputate_mobs/<X>`` ya no se
                # copia — el VA lo lee de ``features/<X>``. El FALTA es
                # legítimo si ese gemelo existe y su contenido sigue
                # siendo idéntico al golden (era una copia verbatim).
                if "/mobs/imputate_mobs/" in rel:
                    twin = rel.replace(
                        "/mobs/imputate_mobs/", "/features/")
                    if (twin in gold and twin in seen
                            and seen[twin] == gold[twin]
                            and gold[twin] == ref):
                        continue
                fails.append(f"FALTA {rel}")
            elif cur != ref:
                diffs = [k for k in ref if ref[k] != cur[k]]
                detail = ""
                if "schema" in diffs:
                    only_gold = sorted(set(map(tuple, ref["schema"]))
                                       - set(map(tuple, cur["schema"])))
                    only_cur = sorted(set(map(tuple, cur["schema"]))
                                      - set(map(tuple, ref["schema"])))
                    detail = f" gold_only={only_gold[:5]} cur_only={only_cur[:5]}"
                fails.append(f"DIFF {rel} ({','.join(diffs)}){detail}")
        for rel in seen:
            if rel not in gold:
                # nuevo ``extract/<t>`` de una fuente con weeks
                # integrado: su contenido se verificó contra el viejo
                # ``weekly_extract/<t>`` por el lado golden->nuevo
                if any(f"/{src}/extract/" in rel
                       for src in WEEKLY_INTEGRATED_SOURCES):
                    continue
                checks += 1
                fails.append(f"EXTRA {rel}")
        print(f"{checks - len(fails)}/{checks} parquets identicos")
        for f in fails:
            print("  ", f)
        if fails:
            sys.exit("golden mismatch")
        print("GOLDEN OK")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    if len(sys.argv) > 1:
        if sys.argv[1] == "verify":
            verify()
            return
        if sys.argv[1] == "snapshot":
            # re-fingerprint sin re-correr el pipeline (workdir existente)
            snapshot()
            return
    if WORKDIR_DYN.exists():
        shutil.rmtree(WORKDIR_DYN)
    make_config()
    spark = bootstrap.get_spark("regression_fixtures")
    try:
        write_fixtures(spark)
    finally:
        spark.stop()
    run_pipeline()
    snapshot()


if __name__ == "__main__":
    main()
