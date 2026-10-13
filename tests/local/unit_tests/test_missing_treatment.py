"""
Tests de ``libs.missing_treatment`` — estrategias de relleno de nulos
(``fill_value``, ``fill_stat``, ``fill_snap``, ``fill_historic``) y las
funciones de nivel módulo usadas para el cálculo de MOBs
(``get_numeric_columns``, ``count_non_null_values``,
``get_last_contiguous_non_null_value``, ``contiguous_mobs``,
``discontiguous_mobs``).
"""
import pytest
from pyspark.sql import functions as F

from libs.missing_treatment import (
    MissingTreatment,
    contiguous_mobs,
    count_non_null_values,
    discontiguous_mobs,
    get_last_contiguous_non_null_value,
    get_numeric_columns,
)


# ---------------------------------------------------------------------------
# Config mínima de MissingTreatment
# ---------------------------------------------------------------------------
def _mt_dict(metodo="real0", id_variables=None):
    """Dict mínimo: fill_variables con un método 'realN' (rellena con N)."""
    return {
        "missing_treatment": {
            "fill_variables": {"variable": ["v"], "metodo": [metodo]},
            "fill_methods": {
                "id_variables": id_variables or ["snapshot"],
                "metodo": ["fill_value"],
                "grouped": [["numcliente"]],
                "valor": ["1"],
            },
        }
    }


def _mt(metodo="real0", id_variables=None):
    return MissingTreatment(year="202401", mt_dict=_mt_dict(metodo, id_variables),
                            source="test_src", n_mis=4, n_threads=1)


# ---------------------------------------------------------------------------
# fill_value / fill_stat / fill_snap
# ---------------------------------------------------------------------------
class TestFillMethods:
    def test_fill_value_real(self, spark):
        """'realN' rellena nulos con N."""
        df = spark.createDataFrame(
            [("1", 0, None), ("1", 1, 5.0), ("2", 0, None)],
            ["numcliente", "tfrom", "v"])
        out = _mt(metodo="real7").fill_value(df, 0)
        vals = {r.tfrom: r.v for r in
                out.filter("numcliente='1'").collect()}
        assert vals[0] == 7.0
        assert vals[1] == 5.0
        assert out.filter("numcliente='2'").first().v == 7.0

    def test_fill_value_ignores_missing_vars(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        out = _mt().fill_value(df, 0)
        assert out.columns == ["x"]

    def test_fill_stat_average(self, spark, tmp_path):
        """nmbr_id_stat='1' rellena nulos con la media por grupo."""
        mt = MissingTreatment(
            year="202401",
            mt_dict={
                "missing_treatment": {
                    "fill_variables": {"variable": ["v"],
                                       "metodo": ["snapshot"]},
                    "fill_methods": {
                        "id_variables": ["snapshot"],
                        "metodo": ["fill_stat"],
                        "grouped": [["numcliente"]],
                        "valor": ["1"],
                    },
                }
            },
            source="test_src", n_mis=4, n_threads=1)
        df = spark.createDataFrame(
            [("1", 0, None), ("1", 1, 2.0), ("1", 2, 4.0)],
            ["numcliente", "tfrom", "v"])
        out = mt.fill_stat(df, ["numcliente"], "1",
                           tmp_path.as_posix(), spark)
        vals = {r.tfrom: r.v for r in out.collect()}
        assert vals[0] == pytest.approx(3.0)  # media de {2,4}
        assert vals[1] == 2.0
        assert vals[2] == 4.0

    def test_fill_snap(self, spark):
        """fill_snap rellena tfrom=0 con el primer valor no nulo del cliente."""
        mt = MissingTreatment(
            year="202401",
            mt_dict={
                "missing_treatment": {
                    "fill_variables": {"variable": ["v"],
                                       "metodo": ["snapshot"]},
                    "fill_methods": {"id_variables": ["snapshot"]},
                }
            },
            source="test_src", n_mis=4, n_threads=1)
        df = spark.createDataFrame(
            [("1", 0, None), ("1", 1, 9.0), ("1", 2, 8.0),
             ("2", 0, 7.0), ("2", 1, 6.0)],
            ["numcliente", "tfrom", "v"])
        out = mt.fill_snap(df, ["numcliente"])
        vals = {(r.numcliente, r.tfrom): r.v for r in out.collect()}
        assert vals[("1", 0)] == 9.0    # primer no-nulo del cliente 1
        assert vals[("1", 1)] == 9.0
        assert vals[("2", 0)] == 7.0    # no era nulo -> se queda


# ---------------------------------------------------------------------------
# Helpers de nivel módulo
# ---------------------------------------------------------------------------
class TestModuleHelpers:
    def test_get_numeric_columns(self, spark):
        df = spark.createDataFrame(
            [(1, 2.5, "a", True)], ["i", "d", "s", "b"])
        nums = get_numeric_columns(df)
        assert set(nums) == {"i", "d"}  # int y double; string/bool fuera

    def test_count_non_null_values(self, spark):
        df = spark.createDataFrame(
            [("v1", "c1", 1), ("v1", "c1", None), ("v1", "c2", None)],
            ["vintage", "id", "x"])
        out = count_non_null_values(df, "vintage", "id", "x")
        vals = {r.id: r.non_null_count for r in out.collect()}
        assert vals == {"c1": 1.0, "c2": 0.0}

    def test_count_non_null_custom_output_name(self, spark):
        df = spark.createDataFrame([("v1", "c1", 1)], ["vintage", "id", "x"])
        out = count_non_null_values(df, "vintage", "id", "x",
                                    output_column="cnt")
        assert "cnt" in out.columns

    def test_last_contiguous_non_null(self, spark):
        """Devuelve el último sorting_column contiguo no nulo desde el inicio."""
        df = spark.createDataFrame(
            [("v", "c1", 1, 0), ("v", "c1", 1, 1), ("v", "c1", None, 2),   # c1: 0,1 contiguos
             ("v", "c2", None, 0), ("v", "c2", 5, 1)],                     # c2: empieza nulo -> -1
            ["vintage", "id", "val", "tfrom"])
        out = get_last_contiguous_non_null_value(
            df, "vintage", "id", "val", "tfrom", "last_ok")
        vals = {r.id: r.last_ok for r in out.collect()}
        assert vals["c1"] == 1
        assert vals["c2"] == -1

    def test_contiguous_mobs(self, spark):
        """Meses contiguos con dato desde t0 hasta el primer missing."""
        df = spark.createDataFrame(
            [("v", "c1", 0, 10.0), ("v", "c1", 1, 10.0), ("v", "c1", 2, 0.0),
             ("v", "c1", 3, 10.0)],
            ["vintage_date", "numcliente", "tfrom", "x"])
        out = contiguous_mobs(df, "cmobs", ["x"])
        val = out.first().cmobs
        # x>0 en t0,t1; x=0 en t2 -> 2 meses contiguos
        assert val == 2.0

    def test_contiguous_mobs_all_missing(self, spark):
        df = spark.createDataFrame(
            [("v", "c1", 0, None), ("v", "c1", 1, 0.0)],
            ["vintage_date", "numcliente", "tfrom", "x"])
        out = contiguous_mobs(df, "cmobs", ["x"])
        assert out.first().cmobs == 0.0  # -1 + 1

    def test_discontiguous_mobs(self, spark):
        """Meses totales con dato (contiguos o no)."""
        df = spark.createDataFrame(
            [("v", "c1", 0, 10.0), ("v", "c1", 1, 0.0), ("v", "c1", 2, 10.0),
             ("v", "c1", 3, 10.0)],
            ["vintage_date", "numcliente", "tfrom", "x"])
        out = discontiguous_mobs(df, "dmobs", ["x"])
        assert out.first().dmobs == 3.0  # t0, t2, t3 tienen dato

    def test_discontiguous_mobs_none_present(self, spark):
        df = spark.createDataFrame(
            [("v", "c1", 0, None), ("v", "c1", 1, 0.0)],
            ["vintage_date", "numcliente", "tfrom", "x"])
        out = discontiguous_mobs(df, "dmobs", ["x"])
        assert out.first().dmobs == 0.0
