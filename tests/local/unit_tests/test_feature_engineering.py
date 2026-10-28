"""
Tests de ``libs.feature_engineering`` — las utilidades de nombres
(``convert_to_three_digit_string``), la lógica de listas de variables
(``_obtain_variables``/``_filter_columns_feature``/``_list_mvars``) y
una transformación UDF end-to-end sobre datos pequeños.

Nota: ``FeatureEngineering._evaluate_path`` invoca ``hdfs dfs`` en
producción; en los tests se parchea a filesystem local.
"""
import os

import numpy as np
import pytest

from libs.feature_engineering import (
    FeatureEngineering,
    convert_to_three_digit_string,
)


# ---------------------------------------------------------------------------
# convert_to_three_digit_string
# ---------------------------------------------------------------------------
class TestThreeDigitString:
    def test_quarter(self):
        assert convert_to_three_digit_string(0.25) == "250"

    def test_zero(self):
        assert convert_to_three_digit_string(0.0) == "000"

    def test_full(self):
        assert convert_to_three_digit_string(1.0) == "1000"

    def test_three_quarters(self):
        assert convert_to_three_digit_string(0.75) == "750"


# ---------------------------------------------------------------------------
# Listas de variables (métodos que solo dependen de listas de columnas)
# ---------------------------------------------------------------------------
def _fe_stub():
    """Instancia sin constructor (no toca disco ni Spark)."""
    fe = FeatureEngineering.__new__(FeatureEngineering)
    fe.frequency_sufix = "m"
    return fe


class TestVariableLists:
    def test_obtain_variables(self):
        fe = _fe_stub()
        cols = ["a_0", "a_1", "b_0", "b_1", "nombre", "x_123"]
        variables = fe._obtain_variables(list(cols))
        # 'x_123' (sufijo >2 dígitos) y 'nombre' (sin _) quedan fuera
        assert set(variables) == {"a", "b"}

    def test_obtain_variables_empty(self):
        fe = _fe_stub()
        assert fe._obtain_variables(["c", "d"]) == []

    def test_filter_columns_feature(self):
        fe = _fe_stub()
        cols = ["hitbureau_0", "hitbureau_1", "hitbureau_2",
                "hitbureau_10", "other_0"]
        d = fe._filter_columns_feature(cols, "hitbureau")
        assert d == {"0": "hitbureau_0", "1": "hitbureau_1",
                     "2": "hitbureau_2", "10": "hitbureau_10"}

    def test_obtain_variables_negative_suffix(self):
        """``a_-4``/``a_-2`` no deben sobrevivir en cols_df — el remove()
        dentro del for original los dejaba pasar y luego entraban a
        list_mvars, generando SQL ``array(a_-4,...)`` que Spark parsea
        como resta (prod: Column 'mob_flag_interest_bal' does not exist).
        """
        fe = _fe_stub()
        cols = ["a_-4", "a_-2", "a_0", "a_1", "a_2", "a_3"]
        variables = fe._obtain_variables(cols)
        assert variables == ["a"]
        assert cols == ["a_0", "a_1", "a_2", "a_3"]

    def test_list_mvars_ignores_negative_tfrom(self):
        fe = _fe_stub()
        cols = ["a_-4", "a_-2", "a_0", "a_1", "a_2", "a_3"]
        out = fe._list_mvars(["a"], cols, 4)
        assert out == [["a_0", "a_1", "a_2", "a_3"]]

    def test_list_mvars_period_2(self):
        fe = _fe_stub()
        cols = [f"a_{i}" for i in range(6)]
        out = fe._list_mvars(["a"], cols, 2)
        # periodo 2 sobre 6 meses -> grupos [0,1], [0..3], [0..5]
        assert out == [["a_0", "a_1"],
                       ["a_0", "a_1", "a_2", "a_3"],
                       ["a_0", "a_1", "a_2", "a_3", "a_4", "a_5"]]

    def test_list_mvars_period_3(self):
        fe = _fe_stub()
        cols = [f"a_{i}" for i in range(6)]
        out = fe._list_mvars(["a"], cols, 3)
        assert out == [["a_0", "a_1", "a_2"],
                       ["a_0", "a_1", "a_2", "a_3", "a_4", "a_5"]]


# ---------------------------------------------------------------------------
# Transformación end-to-end (escribe parquet local)
# ---------------------------------------------------------------------------
@pytest.fixture
def fe_local(spark, tmp_path, monkeypatch):
    """FeatureEngineering funcional con _evaluate_path parcheado a local."""
    monkeypatch.setattr(FeatureEngineering, "_evaluate_path",
                        lambda self, path: os.path.isdir(path))
    df = spark.createDataFrame(
        [(i, float(i + 1), float(2 * i + 1), float(3 * i + 1), float(4 * i + 1))
         for i in range(4)],
        ["id", "a_0", "a_1", "a_2", "a_3"])
    fe = FeatureEngineering(
        df=df,
        working_path=tmp_path.as_posix(),
        period=2,
        sqlContext=spark,
        fe_dict=None,
        number_threads=2,
        mode="overwrite")
    # las queries de las transformaciones consultan la vista "df",
    # que FeatureEngineering.run() registra; la registramos aquí para
    # poder invocar los métodos _<trans>Transformation directamente.
    fe.df.createOrReplaceTempView("df")
    return fe, tmp_path


class TestFeatureEngineeringE2E:
    def test_constructor_writes_raw_features(self, fe_local):
        fe, tmp_path = fe_local
        assert (tmp_path / "features" / "raw_features").exists()

    def test_list_mvars_from_df(self, fe_local):
        fe, _ = fe_local
        # 4 columnas a_i y periodo 2 -> dos grupos
        assert fe.list_mvars == [["a_0", "a_1"],
                                 ["a_0", "a_1", "a_2", "a_3"]]

    def test_mean_transformation(self, fe_local, spark):
        fe, tmp_path = fe_local
        fe._meanTransformation()
        out_path = tmp_path / "features" / "mean_features"
        assert (out_path / "_SUCCESS").exists()
        out = spark.read.parquet(out_path.as_posix())
        cols = out.columns
        # columnas mean_<var>_<n>m
        assert any(c.startswith("mean_a_") for c in cols)
        # media de [a_0, a_1] por fila i: (i+1 + 2i+1)/2 = (3i+2)/2
        col_2m = [c for c in cols if "mean_a_" in c and "_2m" in c][0]
        vals = sorted(r.asDict()[col_2m] for r in out.collect())
        assert vals == pytest.approx([1.0, 2.5, 4.0, 5.5])

    def test_sum_transformation(self, fe_local, spark):
        fe, tmp_path = fe_local
        fe._sumTransformation()
        out = spark.read.parquet(
            (tmp_path / "features" / "sum_features").as_posix())
        assert out.count() == 4

    def test_rms_transformation(self, fe_local, spark):
        fe, tmp_path = fe_local
        fe._rmsTransformation()
        out = spark.read.parquet(
            (tmp_path / "features" / "rms_features").as_posix())
        cols = [c for c in out.columns if c.startswith("rms_")]
        assert cols
        col_2m = [c for c in cols if "_2m" in c][0]
        # rms de [i+1, 2i+1] por fila i
        expected = sorted(np.sqrt(( (i+1)**2 + (2*i+1)**2 ) / 2)
                          for i in range(4))
        vals = sorted(r.asDict()[col_2m] for r in out.collect())
        assert vals == pytest.approx(expected)

    def test_entropy_onz_transformation(self, fe_local, spark):
        """Las ONZ corren sobre variables binarias; probamos entropy."""
        fe, tmp_path = fe_local
        onz = fe.onz_transformations()
        onz._entropy_onzTransformation()
        out = spark.read.parquet(
            (tmp_path / "features" / "entropy_onz_features").as_posix())
        assert out.count() == 4
