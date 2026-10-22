"""
Tests de la integración Adobe Clickstream en el datapull:

- ``_flag_condition`` / reglas de flags del config (``group_by.flags``).
- ``AdobeExtractSpecialTreatment``: filtros de página, ``session_id``,
  backfill de ``post_evar5``, resolución hash -> ``numcliente`` **sin
  fuga de información** (``proc_dt < mes del lote``).
- ``AdobeAggregate``: ``T_*`` (con el fix de ``T_vista``), dominios
  ``weekly``/``fortnightly``/``last_days``/``daily_seq``.
- ``GeneratePivot.drop_null_values``: dominios de tfrom mezclados sin
  columnas all-null.
- ``Mobs.adobe_mobs``: antigüedad digital capped.
"""
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F

from libs.aggregattes.adobe import AdobeAggregate, _flag_condition
from libs.extract_special_treatments.adobe import (
    AdobeExtractSpecialTreatment)
from libs.generate_pivot import GeneratePivot
from libs.mobs.mobs import Mobs


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
BNT_FLAGS = {
    "PIL_view": [
        {"col": "evar25", "op": "startswith",
         "value": "MX|BNP|Dashboard|Overlay|"},
        {"col": "evar25", "op": "contains_any",
         "value": ["Logra", "PERSONAL"]},
        {"col": "evar25", "op": "endswith_any",
         "value": ["NoPorAhora", "ConoceMas"]}],
    "PIL_interested": [
        {"col": "evar25", "op": "startswith",
         "value": "MX|BNP3|OTROS_PRODUCTOS|OFERTAS_CITI|Creditopersonal|CAPTURA"},
        {"col": "evar25", "op": "endswith", "value": "OfertasCitibanamex"}],
}

MOB_FLAGS = {
    "Flag_interest_LOP": [
        {"col": "post_evar3", "op": "eq", "value": "OFERTASPARAMETRICAS"},
        {"col": "post_evar63", "op": "contains", "value": "DIS"}],
}


def _agg_dict(flags=None, prefix="bnt", **overrides):
    d = {
        "prefix": prefix,
        "time_vars": ["T_inicio", "T_vista", "T_session"],
        "time_stats": ["sum", "mean"],
        "flags": flags if flags is not None else BNT_FLAGS,
        "domains": {
            "weekly": {"enabled": True},
            "fortnightly": {"enabled": True},
            "last_days": {"enabled": True, "windows": [1, 7, 15],
                          "flag_stats": ["sum"],
                          "time_stats": ["sum"]},
            "daily_seq": {"enabled": True, "days": 3},
        },
    }
    d.update(overrides)
    return d


def _write_historic(spark, path, rows):
    """Escribe un ``<table>_historic`` mínimo como el que deja ``weeks``."""
    cols = ["numcliente", "vintage_date", "tfrom", "tfrom_date",
            "file_date", "session_id", "date_time", "evar25",
            "post_evar3", "post_evar63"]
    spark.createDataFrame(rows, cols).write.mode("overwrite").parquet(path)


# ---------------------------------------------------------------------------
# _flag_condition — traducción de reglas del config
# ---------------------------------------------------------------------------
class TestFlagCondition:
    def _eval(self, spark, rules, row):
        """Evalúa reglas sobre una fila dict ``{col: valor}``."""
        df = spark.createDataFrame([row])
        conds = [_flag_condition(r) for r in rules]
        c = conds[0]
        for x in conds[1:]:
            c = c & x
        return df.select(F.when(c, 1).otherwise(0).alias("f")).first()["f"]

    def test_ops(self, spark):
        assert self._eval(
            spark, [{"col": "c1", "op": "eq", "value": "X"}],
            {"c1": "X"}) == 1
        assert self._eval(
            spark, [{"col": "c1", "op": "contains", "value": "DIS"}],
            {"c1": "MX|DIS|Y"}) == 1
        assert self._eval(
            spark, [{"col": "c1", "op": "startswith", "value": "MX|"}],
            {"c1": "MX|BNP"}) == 1
        assert self._eval(
            spark, [{"col": "c1", "op": "endswith_any",
                     "value": ["A", "B"]}], {"c1": "zzB"}) == 1
        assert self._eval(
            spark, [{"col": "c1", "op": "contains_any",
                     "value": ["Logra", "PERSONAL"]}],
            {"c1": "Overlay|Logra mas|NoPorAhora"}) == 1

    def test_and_semantics(self, spark):
        # todas las reglas deben cumplirse (AND)
        rules = [{"col": "c1", "op": "startswith", "value": "MX|"},
                 {"col": "c2", "op": "eq", "value": "Y"}]
        assert self._eval(spark, rules, {"c1": "MX|A", "c2": "Y"}) == 1
        assert self._eval(spark, rules, {"c1": "MX|A", "c2": "Z"}) == 0

    def test_bancanet_pil_view(self, spark):
        hit = {"evar25": "MX|BNP|Dashboard|Overlay|Logra mas|PERSONAL|ConoceMas"}
        miss = {"evar25": "MX|BNP|Dashboard|Overlay|Logra mas|PERSONAL|Otro"}
        assert self._eval(spark, BNT_FLAGS["PIL_view"], hit) == 1
        assert self._eval(spark, BNT_FLAGS["PIL_view"], miss) == 0


# ---------------------------------------------------------------------------
# AdobeExtractSpecialTreatment
# ---------------------------------------------------------------------------
class TestAdobeExtractSpecialTreatment:
    def _parent(self, spark, adobe_cfg):
        return SimpleNamespace(
            sources_cat={"adobe": adobe_cfg}, sqlContext=spark)

    def _cfg(self, keep=None):
        return {
            "hash_id_db": "hash_snap",
            "clickstream_filters": [
                {"key": "pagename",
                 "page_names": ["MX|BNP3|OFERTAS"]},
                {"key": "post_clickmaplinkbyregion",
                 "page_names": ["Iniciar sesi"]},
            ],
            "event_filter": "",
            "use_event_filter": False,
            "keep_columns": keep or [
                "numcliente", "session_id", "file_date", "date_time",
                "evar25"],
        }

    def _hash_table(self, spark):
        # snapshot vigente (2025-07-01) + uno futuro (2025-08-15) que
        # NO debe usarse para un lote de agosto 2025
        spark.createDataFrame(
            [("c1", "H1", "S1", "2025-07-01"),
             ("c_future", "H2", "S2", "2025-08-15")],
            ["numcliente", "numcliente_hash", "numcliente_salti",
             "proc_dt"],
        ).createOrReplaceTempView("hash_snap")

    def _hits(self, spark):
        cols = ["post_visid_high", "post_visid_low", "visit_num",
                "post_evar5", "date_time", "file_date", "pagename",
                "post_evar25", "post_clickmaplinkbyregion", "evar25"]
        return spark.createDataFrame(
            [
                # hit 1 de la sesión sin evar5; hit 2 lo trae -> backfill
                ("1", "2", 3, "", "2025-08-01 10:00:00", "2025-08-01",
                 "MX|BNP3|OFERTAS|X", "", "", "E"),
                ("1", "2", 3, "h1", "2025-08-01 10:05:00", "2025-08-01",
                 "MX|BNP3|OFERTAS|Y", "", "", "E"),
                # hit que solo resuelve con snapshot FUTURO -> debe caer
                ("1", "2", 3, "h2", "2025-08-01 10:10:00", "2025-08-01",
                 "MX|BNP3|OFERTAS|Z", "", "", "E"),
                # hit fuera de los filtros de página -> debe caer
                ("1", "2", 3, "h1", "2025-08-01 10:15:00", "2025-08-01",
                 "MX|OTRA|COSA", "", "", "E"),
            ], cols)

    def test_pagename_filters_clauses(self, spark):
        st = AdobeExtractSpecialTreatment(
            parent=self._parent(spark, self._cfg()),
            input_df=None, table_name="t", date="2025-08-01", query="")
        clauses = st._pagename_filters()
        assert len(clauses) == 2
        assert "UPPER(TRIM(pagename)) LIKE 'MX|BNP3|OFERTAS%'" in clauses[0]
        assert "UPPER(TRIM(post_clickmaplinkbyregion)) LIKE 'INICIAR SESI%'" \
            in clauses[1]

    def test_special_treatment_end_to_end(self, spark):
        self._hash_table(spark)
        st = AdobeExtractSpecialTreatment(
            parent=self._parent(spark, self._cfg()),
            input_df=self._hits(spark),
            table_name="t", date="2025-08-01", query="")
        out = st._special_treatment(st.input_df)
        rows = out.orderBy("date_time").collect()

        # 2 hits sobreviven: evar5 vacío + h1 resuelven con snapshot
        # 2025-07-01; el hit h2 (solo en snapshot futuro) y el fuera de
        # página caen
        assert len(rows) == 2
        assert {r["numcliente"] for r in rows} == {"c1"}
        assert all(r["session_id"] == "1-2-3" for r in rows)
        assert set(out.columns) == set(self._cfg()["keep_columns"])

    def test_no_future_snapshot(self, spark):
        """El lote de agosto usa el último proc_dt < 2025-08-01, no el
        snapshot del 15 de agosto (sin información del futuro)."""
        self._hash_table(spark)
        st = AdobeExtractSpecialTreatment(
            parent=self._parent(spark, self._cfg()),
            input_df=self._hits(spark),
            table_name="t", date="2025-08-01", query="")
        out = st._special_treatment(st.input_df)
        assert out.filter(F.col("numcliente") == "c_future").count() == 0


# ---------------------------------------------------------------------------
# AdobeAggregate
# ---------------------------------------------------------------------------
class TestAdobeAggregate:
    def _run(self, spark, tmp_path, rows, agg_dict=None, tables=None):
        prev = tmp_path / "weekly_extract"
        hist = prev / "adobe_t_historic"
        _write_historic(spark, hist.as_posix(), rows)
        agg = AdobeAggregate(
            previous_path=prev.as_posix(),
            agg_dict=agg_dict or _agg_dict(),
            sqlContext=spark,
            tables=tables or ["adobe_t_historic"],
            is_dynamic=False)
        agg.aggregate()
        return spark.read.parquet((tmp_path / "groupby").as_posix())

    def test_weekly_domain_and_flags(self, spark, tmp_path):
        rows = [
            # sesión con 2 hits a 5 min -> T_vista 300 en el segundo
            ("c1", "2025-08-31", 4, "2025-07-28", "2025-07-30",
             "1-2-3", "2025-07-30 10:00:00",
             "MX|BNP|Dashboard|Overlay|Logra|PERSONAL|ConoceMas", "", ""),
            ("c1", "2025-08-31", 4, "2025-07-28", "2025-07-30",
             "1-2-3", "2025-07-30 10:05:00", "OTRA", "", ""),
        ]
        out = self._run(spark, tmp_path, rows)
        wk = out.filter(
            "tfrom = 4 AND bnt_pil_view IS NOT NULL").collect()
        assert len(wk) == 1
        r = wk[0].asDict()
        assert r["bnt_pil_view"] == 1          # max del flag binario
        assert r["bnt_pil_view_cnt"] == 1      # sum de hits con el flag
        assert r["bnt_hits"] == 2
        # T_vista corregido: primer hit 0, segundo 300s -> sum 300
        assert r["bnt_t_vista_sum"] == 300
        # T_session = duración de la sesión (300) en los 2 hits -> sum 600
        assert r["bnt_t_session_mean"] == 300

    def test_t_vista_bug_fixed(self, spark, tmp_path):
        """Primer hit de sesión -> T_vista=0 (no null), resto = delta."""
        rows = [
            ("c1", "2025-08-31", 4, "2025-07-28", "2025-07-30",
             "s", "2025-07-30 10:00:00", "A", "", ""),
            ("c1", "2025-08-31", 4, "2025-07-28", "2025-07-30",
             "s", "2025-07-30 10:10:00", "A", "", ""),
        ]
        out = self._run(spark, tmp_path, rows)
        r = out.filter("bnt_t_vista_sum IS NOT NULL").first()
        # 0 + 600 = 600 (el bug del original daba null + 0 = 0)
        assert r["bnt_t_vista_sum"] == 600

    def test_fortnightly_domain(self, spark, tmp_path):
        rows = [
            ("c1", "2025-08-31", 5, "2025-08-11", "2025-08-12",
             "s", "2025-08-12 10:00:00",
             "MX|BNP|Dashboard|Overlay|Logra|PERSONAL|NoPorAhora", "", ""),
        ]
        out = self._run(spark, tmp_path, rows)
        # semana 5 -> quincena floor(5/2)=2
        q = out.filter("bnt_pil_view_q IS NOT NULL").collect()
        assert len(q) == 1
        assert q[0]["tfrom"] == 2.0 and q[0]["bnt_pil_view_q"] == 1

    def test_last_days_domain(self, spark, tmp_path):
        # vintage 2025-08-31 -> end=2025-08-31; hit del 29 -> last_days=3
        rows = [
            ("c1", "2025-08-31", 4, "2025-08-25", "2025-08-29",
             "s", "2025-08-29 10:00:00",
             "MX|BNP|Dashboard|Overlay|Logra|PERSONAL|ConoceMas", "", ""),
        ]
        out = self._run(spark, tmp_path, rows)
        ld = out.filter("bnt_pil_view_ld IS NOT NULL").collect()
        # ventanas 1,7,15 -> el hit (3d) entra en 7 y 15, no en 1
        tf = {r["tfrom"] for r in ld}
        assert tf == {7.0, 15.0}
        assert all(r["bnt_pil_view_ld"] == 1 for r in ld)
        assert all(r["bnt_hits_ld"] == 1 for r in ld)

    def test_daily_seq_domain(self, spark, tmp_path):
        rows = [
            ("c1", "2025-08-31", 4, "2025-08-25", "2025-08-30",
             "s", "2025-08-30 10:00:00",
             "MX|BNP|Dashboard|Overlay|Logra|PERSONAL|ConoceMas", "", ""),
        ]
        out = self._run(spark, tmp_path, rows)
        d = out.filter("bnt_pil_view_d IS NOT NULL").collect()
        # last_days=2 (30 -> 31-08) con days=3
        assert [r["tfrom"] for r in d] == [2.0]
        assert d[0]["bnt_pil_view_d"] == 1

    def test_mobile_flags(self, spark, tmp_path):
        rows = [
            ("c1", "2025-08-31", 1, "2025-08-25", "2025-08-29",
             "s", "2025-08-29 10:00:00", "",
             "OFERTASPARAMETRICAS", "MX|DIS|X"),
        ]
        out = self._run(spark, tmp_path, rows,
                        agg_dict=_agg_dict(flags=MOB_FLAGS, prefix="mob"))
        wk = out.filter("mob_flag_interest_lop IS NOT NULL").first()
        assert wk["mob_flag_interest_lop"] == 1


# ---------------------------------------------------------------------------
# GeneratePivot.drop_null_values
# ---------------------------------------------------------------------------
class TestPivotDropNulls:
    def _pivot(self, spark, tmp_path, drop):
        gb = tmp_path / "groupby"
        spark.createDataFrame(
            # fila semanal (vars sin sufijo) + fila ld (vars _ld, resto null)
            [("c1", "v1", 1.0, "d", 1.0, None),
             ("c1", "v1", 7.0, "d", None, 5.0)],
            ["numcliente", "vintage_date", "tfrom", "tfrom_date",
             "bnt_x", "bnt_x_ld"],
        ).write.mode("overwrite").parquet(gb.as_posix())
        gp = GeneratePivot(
            previous_path=gb.as_posix(),
            pivot_dict={"source": "adobe_bancanet",
                        "pivot_key": ["numcliente", "vintage_date",
                                      "tfrom", "tfrom_date"],
                        "drop_null_values": drop},
            sqlContext=spark)
        gp.pivot_data()
        return spark.read.parquet((tmp_path / "pivot").as_posix())

    def test_drop_null_values_avoids_junk_columns(self, spark, tmp_path):
        df = self._pivot(spark, tmp_path, drop=True)
        cols = set(df.columns)
        # sin el filtro, la fila semanal (tfrom=1) aportaría el par
        # (bnt_x_ld, 1, null) -> columna junk 'bnt_x_ld_1' all-null
        assert "bnt_x_1" in cols and "bnt_x_ld_7" in cols
        assert "bnt_x_ld_1" not in cols and "bnt_x_7" not in cols


# ---------------------------------------------------------------------------
# Mobs.adobe_mobs
# ---------------------------------------------------------------------------
class TestAdobeMobs:
    def test_mobs_adobe_capped(self, spark, tmp_path):
        datapull = tmp_path / "datapull"
        hist = datapull / "extract" / "adobe_t_historic"
        spark.createDataFrame(
            [("c1", "v1", "2025-08-20", 1.0),   # activo en el mes vintage
             ("c1", "v1", "2025-06-10", 3.0),   # y hace 3 meses
             ("c1", "v1", None, 6.0),           # mes vacío de la malla
             ("c2", "v1", "2025-03-05", 6.0)],  # ventana completa (borde)
            ["numcliente", "vintage_date", "file_date", "tfrom"],
        ).write.mode("overwrite").parquet(hist.as_posix())

        m = Mobs(year="202508", mt_dict={}, source="adobe_bancanet",
                 n_mis=6, n_threads=1)
        out = m.adobe_mobs(
            (datapull / "mobs").as_posix(),
            ["extract/adobe_t_historic"], spark)
        mobs = {r["numcliente"]: r["mobs_adobe"] for r in out.collect()}
        assert mobs["c1"] == 3          # max(tfrom) solo sobre hits reales
        assert mobs["c2"] == 6          # cap natural al borde de la ventana


# ---------------------------------------------------------------------------
# GeneratePivot.keep_pivot_clients
# ---------------------------------------------------------------------------
class TestKeepPivotClients:
    def test_clients_without_data_stay_with_zeros(self, spark, tmp_path):
        """Todo cliente del pivot aparece en el wide aunque no tenga
        datos — la base de la imputación -99999 vía mobs."""
        gb = tmp_path / "groupby"
        spark.createDataFrame(
            [("c1", "v1", 1.0, "d", 3.0)],
            ["numcliente", "vintage_date", "tfrom", "tfrom_date",
             "bnt_x"],
        ).write.mode("overwrite").parquet(gb.as_posix())
        clients = spark.createDataFrame(
            [("c1", "v1"), ("c2", "v1"), ("c3", "v1")],
            ["numcliente", "vintage_date"])
        gp = GeneratePivot(
            previous_path=gb.as_posix(),
            pivot_dict={"source": "adobe_bancanet",
                        "pivot_key": ["numcliente", "vintage_date",
                                      "tfrom", "tfrom_date"]},
            sqlContext=spark, pivot_clients=clients)
        gp.pivot_data()
        out = spark.read.parquet((tmp_path / "pivot").as_posix())
        vals = {r["numcliente"]: r["bnt_x_1"] for r in out.collect()}
        assert sorted(vals) == ["c1", "c2", "c3"]
        assert vals["c1"] == 3.0
        assert vals["c2"] == 0 and vals["c3"] == 0

    def test_disabled_keeps_old_behavior(self, spark, tmp_path):
        gb = tmp_path / "groupby"
        spark.createDataFrame(
            [("c1", "v1", 1.0, "d", 3.0)],
            ["numcliente", "vintage_date", "tfrom", "tfrom_date",
             "bnt_x"],
        ).write.mode("overwrite").parquet(gb.as_posix())
        gp = GeneratePivot(
            previous_path=gb.as_posix(),
            pivot_dict={"source": "adobe_bancanet",
                        "pivot_key": ["numcliente", "vintage_date",
                                      "tfrom", "tfrom_date"]},
            sqlContext=spark)
        gp.pivot_data()
        out = spark.read.parquet((tmp_path / "pivot").as_posix())
        assert out.count() == 1


# ---------------------------------------------------------------------------
# PreviousFixerAdobe — imputación -99999 sobre ventanas _<N>w
# ---------------------------------------------------------------------------
class TestPreviousFixerAdobe:
    def test_windows_beyond_mobs_get_neg99999(self, spark):
        """mobs_adobe en meses * 4 semanas: ventanas mayores -> -99999;
        mobs ausente (sin actividad) -> todo -99999."""
        from libs.mobs.adobe import PreviousFixerAdobe
        df = spark.createDataFrame(
            [(0, 1, 5.0, 9.0, 2.0),      # 1 mes: 4w cubierta, 7w no
             (1, 0, 5.0, 9.0, 2.0),      # mobs 0: nada cubierto
             (2, None, 5.0, 9.0, 2.0)],  # fuera de calculate_mobs
            ["id_msf_tools_v2_12315", "mobs_adobe",
             "sum_mt_a_4w", "sum_mt_a_7w", "sum_mt_a_2w"])
        out = PreviousFixerAdobe.payroll_mob_imputation(df, [], [])
        assert "mobs_adobe" not in out.columns
        rows = {r["id_msf_tools_v2_12315"]: r for r in out.collect()}
        assert rows[0]["sum_mt_a_4w"] == 5.0    # cubierta: valor intacto
        assert rows[0]["sum_mt_a_7w"] == -99999
        assert rows[0]["sum_mt_a_2w"] == 2.0
        assert rows[1]["sum_mt_a_4w"] == -99999
        assert rows[2]["sum_mt_a_7w"] == -99999
