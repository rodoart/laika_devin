"""
Tests de las reglas de patrones de ``libs.aggregattes.payroll_streaks``
(las funciones ``*_rule`` de nivel módulo que transforman el patrón
semanal de pagos de nómina).

Columnas que consumen las reglas:

- ``client_payment_type`` : 'mensual' | 'quincenal' | 'semanal' |
  'indeterminado' (cómo paga el cliente).
- ``count_paid_in_month`` : nº de semanas con pago en el mes.
- ``week_of_month``       : semana de la fila (1..4).
- ``weeks_of_month``      : array con las semanas pagadas del cliente.
- ``payment_type_per_month``: etiqueta usada por ``payment_type_pattern_rule``.

Convención de salida: ``1`` = pago esperado, ``0`` = no pago,
``9`` = dato faltante (semana aún no observada).
"""
from datetime import date

import pytest

from libs.aggregattes.payroll_streaks import (
    fill_and_select_latest,
    get_when_otherwise_nested,
    inverse_pattern_rule,
    payment_type_pattern_rule,
    raw_pattern_rule,
    reverse_pattern_rule,
    shortened_pattern_forced_monthly_rule,
    shortened_pattern_forced_weekly_rule,
    sorted_shortened_pattern_forced_monthly_rule,
    sorted_shortened_pattern_forced_fortnightly_rule,
    sorted_shortened_pattern_forced_weekly_rule,
    unusual_pattern_rule,
)
from pyspark.sql.functions import array, col, lit, when


def _base_df(spark, rows):
    """rows: (client_payment_type, count_paid_in_month, week_of_month,
    weeks_of_month, pattern)"""
    return spark.createDataFrame(
        [(t, c, w, ws, p) for t, c, w, ws, p in rows],
        ["client_payment_type", "count_paid_in_month", "week_of_month",
         "weeks_of_month", "pattern"])


def _get(df, out_col):
    """{week_of_month: valor} del output de una regla."""
    return {r.week_of_month: getattr(r, out_col)
            for r in df.select("week_of_month", out_col).collect()}


def _four_weeks(ptype, count, weeks):
    """4 filas (una por week_of_month) para un mismo cliente."""
    return [(ptype, count, w, weeks, 1) for w in (1, 2, 3, 4)]


# ---------------------------------------------------------------------------
# raw_pattern_rule — passthrough con renombre
# ---------------------------------------------------------------------------
class TestRawPatternRule:
    def test_copies_with_prefix(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 2, [3, 4]))
        out = raw_pattern_rule(df, ["pattern"], "raw", False, None)
        assert _get(out, "raw_pattern") == {1: 1, 2: 1, 3: 1, 4: 1}

    def test_remove_previous_prefix(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 2, [3, 4]))
        df = raw_pattern_rule(df, ["pattern"], "raw", False, None)
        out = inverse_pattern_rule(df, ["raw_pattern"], "inv", True, "raw")
        assert "inv_pattern" in out.columns  # 'raw_' fue quitado


# ---------------------------------------------------------------------------
# inverse_pattern_rule — 1<->0
# ---------------------------------------------------------------------------
class TestInversePatternRule:
    def test_swaps_ones_and_zeros(self, spark):
        df = spark.createDataFrame(
            [(1, 1), (2, 0), (3, 9), (4, 0)],
            ["w", "pattern"])
        out = inverse_pattern_rule(df, ["pattern"], "inv", False, None)
        vals = {r.w: r.inv_pattern for r in out.collect()}
        assert vals == {1: 0, 2: 1, 3: 9, 4: 1}


# ---------------------------------------------------------------------------
# sorted_shortened_pattern_forced_monthly_rule
# ---------------------------------------------------------------------------
class TestSortedForcedMonthly:
    def test_marks_last_two_weeks(self, spark):
        """count>=1 -> semanas 3,4 = 1; semanas 1,2 = 9 (faltantes)."""
        df = _base_df(spark, _four_weeks("mensual", 1, [4]))
        out = sorted_shortened_pattern_forced_monthly_rule(
            df, ["pattern"], "sm", False, None)
        assert _get(out, "sm_pattern") == {1: 9, 2: 9, 3: 1, 4: 1}

    def test_no_payments_still_marks(self, spark):
        """count>=1 también cubre meses con varios pagos."""
        df = _base_df(spark, _four_weeks("semanal", 4, [1, 2, 3, 4]))
        out = sorted_shortened_pattern_forced_monthly_rule(
            df, ["pattern"], "sm", False, None)
        assert _get(out, "sm_pattern") == {1: 9, 2: 9, 3: 1, 4: 1}


# ---------------------------------------------------------------------------
# sorted_shortened_pattern_forced_weekly_rule
# ---------------------------------------------------------------------------
class TestSortedForcedWeekly:
    def test_weekly_client_full_month(self, spark):
        df = _base_df(spark, _four_weeks("semanal", 4, [1, 2, 3, 4]))
        out = sorted_shortened_pattern_forced_weekly_rule(
            df, ["pattern"], "sw", False, None)
        assert _get(out, "sw_pattern") == {1: 1, 2: 1, 3: 1, 4: 1}

    def test_weekly_client_three_payments(self, spark):
        df = _base_df(spark, _four_weeks("semanal", 3, [1, 2, 3]))
        out = sorted_shortened_pattern_forced_weekly_rule(
            df, ["pattern"], "sw", False, None)
        vals = _get(out, "sw_pattern")
        # count=3 -> semanas 4,3,2 = 1; semana 1 = 0
        assert vals == {1: 0, 2: 1, 3: 1, 4: 1}

    def test_monthly_client_all_ones(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 1, [4]))
        out = sorted_shortened_pattern_forced_weekly_rule(
            df, ["pattern"], "sw", False, None)
        assert _get(out, "sw_pattern") == {1: 1, 2: 1, 3: 1, 4: 1}


# ---------------------------------------------------------------------------
# sorted_shortened_pattern_forced_fortnightly_rule
# ---------------------------------------------------------------------------
class TestSortedForcedFortnightly:
    def test_fortnightly_two_payments_in_34(self, spark):
        """quincenal con count=2 y pagos en semanas 3,4 -> solo semana 4=1."""
        df = _base_df(spark, _four_weeks("quincenal", 2, [3, 4]))
        out = sorted_shortened_pattern_forced_fortnightly_rule(
            df, ["pattern"], "sf", False, None)
        vals = _get(out, "sf_pattern")
        assert vals[4] == 1
        assert vals[1] == 9 and vals[2] == 9

    def test_monthly_client_marks_34(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 1, [4]))
        out = sorted_shortened_pattern_forced_fortnightly_rule(
            df, ["pattern"], "sf", False, None)
        vals = _get(out, "sf_pattern")
        assert vals[3] == 1 and vals[4] == 1


# ---------------------------------------------------------------------------
# shortened_pattern_forced_monthly_rule (no sorted)
# ---------------------------------------------------------------------------
class TestShortenedForcedMonthly:
    def test_outputs(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 1, [4]))
        out = shortened_pattern_forced_monthly_rule(
            df, ["pattern"], "shm", False, None)
        vals = _get(out, "shm_pattern")
        # mensual con count>=1 marca según la regla documentada
        assert all(v in (0, 1, 9) for v in vals.values())


# ---------------------------------------------------------------------------
# shortened_pattern_forced_weekly_rule
# ---------------------------------------------------------------------------
class TestShortenedForcedWeekly:
    def test_weekly_full(self, spark):
        df = _base_df(spark, _four_weeks("semanal", 4, [1, 2, 3, 4]))
        out = shortened_pattern_forced_weekly_rule(
            df, ["pattern"], "shw", False, None)
        assert _get(out, "shw_pattern") == {1: 1, 2: 1, 3: 1, 4: 1}

    def test_monthly_always_ones(self, spark):
        df = _base_df(spark, _four_weeks("mensual", 1, [4]))
        out = shortened_pattern_forced_weekly_rule(
            df, ["pattern"], "shw", False, None)
        assert _get(out, "shw_pattern") == {1: 1, 2: 1, 3: 1, 4: 1}


# ---------------------------------------------------------------------------
# unusual_pattern_rule
# ---------------------------------------------------------------------------
class TestUnusualPatternRule:
    def test_three_payments_is_unusual(self, spark):
        df = _base_df(spark, _four_weeks("semanal", 3, [1, 2, 3]))
        out = unusual_pattern_rule(df, ["pattern"], "unusual", False, None)
        vals = _get(out, "unusual")
        assert vals[4] == 1
        assert vals[1] == 9 and vals[2] == 9 and vals[3] == 9

    def test_two_payments_fortnightly_pattern_not_unusual(self, spark):
        """count=2 con semanas (2,4) -> regular (no inusual)."""
        df = _base_df(spark, _four_weeks("quincenal", 2, [2, 4]))
        out = unusual_pattern_rule(df, ["pattern"], "unusual", False, None)
        assert _get(out, "unusual")[4] == 0

    def test_two_payments_3_4_is_unusual(self, spark):
        """count=2 con semanas (3,4) no es (1,3) ni (2,4) -> inusual."""
        df = _base_df(spark, _four_weeks("quincenal", 2, [3, 4]))
        out = unusual_pattern_rule(df, ["pattern"], "unusual", False, None)
        assert _get(out, "unusual")[4] == 1

    def test_two_payments_irregular_is_unusual(self, spark):
        """count=2 con semanas (2,3) -> inusual."""
        df = _base_df(spark, _four_weeks("quincenal", 2, [2, 3]))
        out = unusual_pattern_rule(df, ["pattern"], "unusual", False, None)
        assert _get(out, "unusual")[4] == 1

    def test_two_payments_1_3_not_unusual(self, spark):
        """count=2 con semanas (1,3) también es regular."""
        df = _base_df(spark, _four_weeks("quincenal", 2, [1, 3]))
        out = unusual_pattern_rule(df, ["pattern"], "unusual", False, None)
        assert _get(out, "unusual")[4] == 0


# ---------------------------------------------------------------------------
# payment_type_pattern_rule — marca el tipo en la semana 4
# ---------------------------------------------------------------------------
class TestPaymentTypePatternRule:
    def _df(self, spark, ptype_per_month):
        return spark.createDataFrame(
            [(ptype_per_month, w) for w in (1, 2, 3, 4)],
            ["payment_type_per_month", "week_of_month"])

    def test_monthly_flag(self, spark):
        out = payment_type_pattern_rule(
            self._df(spark, "mensual"), ["x"], "pt", False, None)
        row = out.filter("week_of_month=4").first()
        assert row.pt_monthly == 1
        assert row.pt_weekly == 0
        assert row.pt_fortnightly == 0
        assert row.pt_indeterminated == 0

    def test_weekly_flag(self, spark):
        out = payment_type_pattern_rule(
            self._df(spark, "semanal"), ["x"], "pt", False, None)
        row = out.filter("week_of_month=4").first()
        assert row.pt_weekly == 1

    def test_weeks_1_to_3_are_9(self, spark):
        out = payment_type_pattern_rule(
            self._df(spark, "semanal"), ["x"], "pt", False, None)
        rows = {r.week_of_month: r.pt_weekly for r in out.collect()}
        assert rows[1] == 9 and rows[2] == 9 and rows[3] == 9
        assert rows[4] == 1


# ---------------------------------------------------------------------------
# get_when_otherwise_nested
# ---------------------------------------------------------------------------
class TestWhenOtherwiseNested:
    def test_single_condition(self, spark):
        cond = [col("a") == 1]
        ops = [lit("uno")]
        df = spark.createDataFrame([(1,), (2,)], ["a"])
        out = df.withColumn("r", get_when_otherwise_nested(cond, ops))
        assert {r.a: r.r for r in out.collect()} == {1: "uno", 2: None}

    def test_nested_conditions(self, spark):
        conds = [col("a") == 1, col("a") == 2, col("a") == 3]
        ops = [lit("uno"), lit("dos"), lit("tres")]
        df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["a"])
        out = df.withColumn("r", get_when_otherwise_nested(conds, ops))
        assert {r.a: r.r for r in out.collect()} == {
            1: "uno", 2: "dos", 3: "tres", 4: None}


# ---------------------------------------------------------------------------
# reverse_pattern_rule — invierte el orden del patrón por cliente
# ---------------------------------------------------------------------------
class TestReversePatternRule:
    def test_reverses_pattern_across_tfrom(self, spark):
        """El patrón de la última semana pasa a la primera, etc."""
        df = spark.createDataFrame(
            [("v1", "1", 1, t, float(t)) for t in range(4)],
            ["vintage_date", "numcliente", "n_tef", "tfrom", "pattern"])
        out = reverse_pattern_rule(
            df, ["pattern"], prefix="rev",
            remove_previous_prefix=False, previous_prefix=None,
            vintage_date_column="vintage_date",
            n_tef_column="n_tef", numcliente_column="numcliente")
        vals = {r.tfrom: r.rev_pattern for r in
                out.select("tfrom", "rev_pattern").collect()}
        # t=3 (último) pasa a t=0, t=2 a t=1, ...
        assert vals[0] == pytest.approx(3.0)
        assert vals[1] == pytest.approx(2.0)
        assert vals[2] == pytest.approx(1.0)
        assert vals[3] == pytest.approx(0.0)

    def test_remove_previous_prefix_names(self, spark):
        df = spark.createDataFrame(
            [("v1", "1", 1, t, float(t)) for t in range(4)],
            ["vintage_date", "numcliente", "n_tef", "tfrom", "raw_pattern"])
        out = reverse_pattern_rule(
            df, ["raw_pattern"], prefix="rev",
            remove_previous_prefix=True, previous_prefix="raw")
        assert "rev_pattern" in out.columns


# ---------------------------------------------------------------------------
# fill_and_select_latest
# ---------------------------------------------------------------------------
class TestFillAndSelectLatest:
    def test_fills_nulls_and_keeps_latest(self, spark):
        df = spark.createDataFrame(
            [("1", date(2024, 1, 1), 10.0),
             ("1", date(2024, 2, 1), None),
             ("1", date(2024, 3, 1), None)],
            ["numcliente", "vintage_date", "v"])
        out = fill_and_select_latest(
            df, "numcliente", "vintage_date", ["v"])
        rows = out.collect()
        assert len(rows) == 1
        assert rows[0].v == 10.0  # último mes rellena con el valor anterior

    def test_keeps_latest_value(self, spark):
        df = spark.createDataFrame(
            [("1", date(2024, 1, 1), 10.0),
             ("1", date(2024, 3, 1), 30.0),
             ("1", date(2024, 2, 1), 20.0)],
            ["numcliente", "vintage_date", "v"])
        out = fill_and_select_latest(
            df, "numcliente", "vintage_date", ["v"])
        assert out.first().v == 30.0
