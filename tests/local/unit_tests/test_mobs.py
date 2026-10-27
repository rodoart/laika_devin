"""
Tests de ``libs.mobs`` — imputación de MOBs
(``PreviousFixerPayrollIncome.payroll_mob_imputation``,
``PreviousFixerPayrollStreaks.payroll_mob_imputation``), helpers
(``_get_months``, ``remove_suffix``) y las funciones de nivel módulo de
``libs.mobs.mobs`` (``get_numeric_columns``, ``count_non_null_values``,
``get_last_contiguous_non_null_value``, ``contiguous_mobs``,
``discontiguous_mobs``).
"""
import pytest

from libs.mobs.mobs import (
    Mobs,
    contiguous_mobs,
    count_non_null_values,
    discontiguous_mobs,
    get_last_contiguous_non_null_value,
    get_numeric_columns,
)
from libs.mobs.payroll_income import (
    PreviousFixerPayroll,
    PreviousFixerPayrollIncome,
)
from libs.mobs.payroll_streaks import PreviousFixerPayrollStreaks


# ---------------------------------------------------------------------------
# _get_months
# ---------------------------------------------------------------------------
class TestGetMonths:
    def test_basic(self):
        assert PreviousFixerPayroll._get_months(3, 12) == [3, 6, 9, 12]

    def test_period_4(self):
        assert PreviousFixerPayroll._get_months(4, 12) == [4, 8, 12]

    def test_non_divisible_ends_at_n_mis(self):
        assert PreviousFixerPayroll._get_months(5, 12) == [5, 10, 12]

    def test_strings_accepted(self):
        assert PreviousFixerPayroll._get_months("3", "6") == [3, 6]

    def test_period_larger_than_n_mis(self):
        assert PreviousFixerPayroll._get_months(20, 12) == [12]


# ---------------------------------------------------------------------------
# remove_suffix
# ---------------------------------------------------------------------------
class TestRemoveSuffix:
    def _fixer(self):
        # instancia mínima sin tocar el FS: construcción manual de attrs
        obj = PreviousFixerPayroll.__new__(PreviousFixerPayroll)
        return obj

    def test_removes_month_suffix(self):
        f = self._fixer()
        out = f.remove_suffix(["feat_3m", "feat_12m", "other_6m"])
        assert out == ["feat", "feat", "other"]

    def test_keeps_non_suffixed(self):
        f = self._fixer()
        assert f.remove_suffix(["feature", "v_2x"]) == ["feature", "v_2x"]


# ---------------------------------------------------------------------------
# payroll_mob_imputation — payroll_income
# ---------------------------------------------------------------------------
class TestPayrollIncomeImputation:
    def _df(self, spark):
        return spark.createDataFrame(
            [("1", 1, 10.0, 0.0, 5.0),   # mobs<=2 -> -99999
             ("2", 2, 10.0, 0.0, 5.0),   # mobs<=2 -> -99999
             ("3", 5, 0.0, 8.0, 5.0)],   # mobs>2 -> 3m=0 toma 6m
            ["numcliente", "mobs_tefs", "feat_tefs_3m", "feat_tefs_6m",
             "other_tef_1_3m"])

    def test_rule1_low_mobs_becomes_missing(self, spark):
        """mobs <= 2 -> feature = -99999."""
        df = self._df(spark)
        out = PreviousFixerPayrollIncome.payroll_mob_imputation(
            df, features=["feat_tefs"], lista_meses=[3, 6])
        vals = {r.numcliente: r.feat_tefs_3m for r in out.collect()}
        assert vals["2"] == -99999
        assert vals["1"] == -99999

    def test_rule2_zero_takes_previous(self, spark):
        """feature == 0 -> toma el valor de la columna siguiente en la
        lista (el df se ordena lista_meses desc: [6, 3], así que un 0 en
        ``3m`` toma el valor de ``6m``)."""
        df = self._df(spark)
        out = PreviousFixerPayrollIncome.payroll_mob_imputation(
            df, features=["feat_tefs"], lista_meses=[3, 6])
        # cliente 3: 3m era 0 -> toma el valor de 6m (8.0)
        val = out.filter("numcliente='3'").first().feat_tefs_3m
        assert val == 8.0

    def test_mobs_columns_dropped(self, spark):
        df = self._df(spark)
        out = PreviousFixerPayrollIncome.payroll_mob_imputation(
            df, features=["feat_tefs"], lista_meses=[3])
        for c in ("mobs_tef_1", "mobs_tef_2", "mobs_tefs"):
            assert c not in out.columns


# ---------------------------------------------------------------------------
# payroll_mob_imputation — payroll_streaks
# ---------------------------------------------------------------------------
class TestPayrollStreaksImputation:
    def _df(self, spark):
        return spark.createDataFrame(
            [("1", 0, 7.0, 3.0),
             ("2", 4, 7.0, 3.0)],
            ["numcliente", "discontiguous_mobs_tefs", "feat_tefs_3m",
             "feat_tef_1_3m"])

    def test_zero_discontiguous_becomes_missing(self, spark):
        """discontiguous_mobs == 0 -> feature = -99999."""
        df = self._df(spark)
        out = PreviousFixerPayrollStreaks.payroll_mob_imputation(
            df, features=["feat_tefs"], lista_meses=[3])
        vals = {r.numcliente: r.feat_tefs_3m for r in out.collect()}
        assert vals["1"] == -99999
        assert vals["2"] == 7.0

    def test_tef_1_uses_its_own_mobs(self, spark):
        """Las features 'tef_1' miran discontiguous_mobs_tef_1."""
        df = spark.createDataFrame(
            [("1", 0, 2, 7.0)],  # tefs=0, tef_1=2
            ["numcliente", "discontiguous_mobs_tefs",
             "discontiguous_mobs_tef_1", "feat_tef_1_3m"])
        out = PreviousFixerPayrollStreaks.payroll_mob_imputation(
            df, features=["feat_tef_1"], lista_meses=[3])
        # mira tef_1 (=2 != 0) -> se conserva
        assert out.first().feat_tef_1_3m == 7.0

# ---------------------------------------------------------------------------
# Funciones de nivel módulo de libs.mobs.mobs
# ---------------------------------------------------------------------------
class TestMobsModuleHelpers:
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


# ---------------------------------------------------------------------------
# Clase Mobs — helpers internos
# ---------------------------------------------------------------------------
class TestMobsClass:
    def _mobs(self, mobs_cfg=None):
        cfg = {
            "missing_treatment": {
                "mobs": mobs_cfg or {
                    "source": ["liabilities", "credit_card"],
                    "table_name": ["t1", "t2"],
                    "sources": [],
                    "mobs_tipo": ["mobs_liabilities"],
                },
            }
        }
        return Mobs(year="202401", mt_dict=cfg, source="test_src",
                    n_mis=12, n_threads=1)

    def test_list_tables(self):
        """_list_tables mapea source -> table_name del dict."""
        mobs = self._mobs()
        assert mobs._list_tables("liabilities") == ["t1"]
        assert mobs._list_tables("credit_card") == ["t2"]
        assert mobs._list_tables("inexistente") == []

    def test_get_months(self):
        """_get_months genera la lista de periodos hasta n_mis."""
        mobs = self._mobs()
        assert mobs._get_months(3) == [3, 6, 9, 12]
        assert mobs._get_months(4) == [4, 8, 12]
        assert mobs._get_months(20) == [12]

    def test_get_months_accepts_strings(self):
        assert self._mobs()._get_months("6") == [6, 12]


# ---------------------------------------------------------------------------
# credit_card_mobs — la tabla mobs_preparation trae ``mob`` precalculado
# (sin fec_apertura): seniority = mob + tfrom. Con fec_apertura se usa la
# query de seniority compartida con liabilities.
# ---------------------------------------------------------------------------
class TestCreditCardMobs:
    def _mobs(self):
        return Mobs.__new__(Mobs)

    def test_mob_column_path(self, spark, tmp_path):
        df = spark.createDataFrame(
            [(1, 5.0, "2026-05-01", "2026-08-01", 3.0),
             (1, 8.0, "2026-08-01", "2026-08-01", 0.0),
             (2, 2.0, "2026-08-01", "2026-08-01", 0.0)],
            ["numcliente", "mob", "mis_dt", "vintage_date", "tfrom"])
        df.write.parquet(str(tmp_path / "credit_card_processed_data_historic"))
        out = self._mobs().credit_card_mobs(
            str(tmp_path), ["credit_card_processed_data_historic"], spark)
        res = {(r.numcliente, r.mobs_credit_card) for r in out.collect()}
        assert res == {(1, 8), (2, 2)}

    def test_fec_apertura_path(self, spark, tmp_path):
        df = spark.createDataFrame(
            [(1, "2026-01-01", "2026-08-01", "2026-08-01", 0.0),
             (1, "2026-01-01", "2026-05-01", "2026-08-01", 3.0)],
            ["numcliente", "fec_apertura", "tfrom_date",
             "vintage_date", "tfrom"])
        df.write.parquet(str(tmp_path / "cc_legacy"))
        out = self._mobs().credit_card_mobs(
            str(tmp_path), ["cc_legacy"], spark)
        res = {(r.numcliente, r.mobs_credit_card) for r in out.collect()}
        # months_between(ago, ene)=7 > months_between(may, ene)=4
        assert res == {(1, 7)}
