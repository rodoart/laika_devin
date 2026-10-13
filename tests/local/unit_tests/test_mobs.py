"""
Tests de ``libs.mobs`` — imputación de MOBs
(``PreviousFixerPayrollIncome.payroll_mob_imputation``,
``PreviousFixerPayrollStreaks.payroll_mob_imputation``) y helpers
(``_get_months``, ``remove_suffix``).
"""
import pytest

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
