"""
Tests de ``libs.weeks`` — utilidades de año/semana (funciones puras) y
los retaggers semanales sobre DataFrames.

Los casos de ``tag_weeks`` se construyen sobre rangos de fechas
concretos para comprobar que ``week_of_month`` siempre queda en 1..4 y
``tfrom`` en 0..n_mis-1.
"""
from datetime import date

import pytest

from libs.weeks import (
    YearWeek,
    WeekOfMonth,
    WeekOfMonthFourWeeksForced,
    WeekOfMonthPayroll,
    WeeklyTFromParquetReTagger,
    add_weeks_to_yearweek,
    change_date_first_monday_of_the_week_column,
    change_date_to_first_monday_of_the_month_column,
    date_to_week_of_year_pyspark_consistent,
    date_to_yearweek,
    is_yearweek_complete_in_month,
    weeks_difference,
    yearweek_to_date,
)


# ---------------------------------------------------------------------------
# Funciones puras de fecha <-> semana
# ---------------------------------------------------------------------------
class TestDateWeekFunctions:
    def test_date_to_week_of_year(self):
        # 2024-01-01 es lunes -> semana ISO 1
        assert date_to_week_of_year_pyspark_consistent(date(2024, 1, 1)) == 1
        # 2024-12-31 cae en semana 1 de 2025 en ISO (pero date.isocalendar
        # devuelve la semana ISO del propio año-calendario: 1)
        assert date_to_week_of_year_pyspark_consistent(date(2024, 6, 15)) == 24

    def test_date_to_yearweek(self):
        yw = date_to_yearweek(date(2024, 6, 15))
        assert yw == YearWeek(year=2024, week_of_year=24)

    def test_yearweek_to_date_monday(self):
        # la semana ISO 1 de 2024 empieza el lunes 2024-01-01
        assert yearweek_to_date(YearWeek(2024, 1)) == date(2024, 1, 1)
        # la semana 24 empieza el lunes 2024-06-10
        assert yearweek_to_date(YearWeek(2024, 24)) == date(2024, 6, 10)

    def test_add_weeks_positive(self):
        yw = add_weeks_to_yearweek(YearWeek(2024, 1), 2)
        assert yw == date_to_yearweek(date(2024, 1, 15))

    def test_add_weeks_negative(self):
        yw = add_weeks_to_yearweek(YearWeek(2024, 10), -3)
        assert yw == date_to_yearweek(date(2024, 2, 12))

    def test_add_weeks_crosses_year(self):
        yw = add_weeks_to_yearweek(YearWeek(2024, 52), 2)
        assert yw.year == 2025
        assert yw.week_of_year == 2

    def test_is_yearweek_complete_in_month(self):
        # La semana del 2024-06-10 al 2024-06-16 está completa en junio
        assert is_yearweek_complete_in_month(YearWeek(2024, 24)) is True
        # La semana que cruza abril/mayo no está completa
        yw = date_to_yearweek(date(2024, 4, 29))
        assert is_yearweek_complete_in_month(yw) is False


# ---------------------------------------------------------------------------
# Helpers estáticos de WeekOfMonth (nivel DataFrame)
# ---------------------------------------------------------------------------
class TestWeekOfMonthStatics:
    def test_day_of_week_starting_in_monday(self, spark):
        df = spark.createDataFrame(
            [(date(2024, 6, 10),),   # lunes -> 1
             (date(2024, 6, 16),),   # domingo -> 7
             (date(2024, 6, 12),)],  # miércoles -> 3
            ["d"])
        out = WeekOfMonth.day_of_week_starting_in_monday(df, "d", "dow")
        vals = {r.dow for r in out.collect()}
        assert vals == {1, 7, 3}

    def test_start_and_end_date_of_week(self, spark):
        from pyspark.sql.types import StructType, StructField, DateType, IntegerType
        df = spark.createDataFrame(
            [(date(2024, 6, 12), 3)],  # miércoles
            StructType([StructField("d", DateType()),
                        StructField("dow", IntegerType())]))
        out = WeekOfMonth.start_date_of_week(df, "d", "dow", "sow")
        out = WeekOfMonth.end_date_of_week(out, "d", "dow", "eow")
        row = out.first()
        assert row.sow == date(2024, 6, 10)  # lunes
        assert row.eow == date(2024, 6, 16)  # domingo

    def test_first_monday_of_month(self, spark):
        # junio 2024 empieza en sábado: el lunes de esa semana (27-may) cae
        # fuera del mes -> la función devuelve el día 1 (clamp al inicio de mes)
        df = spark.createDataFrame([("2024-06-15",)], ["d"])
        out = change_date_to_first_monday_of_the_month_column(df, "d", "dow")
        assert out.first().first_monday == date(2024, 6, 1)

    def test_first_monday_of_month_when_first_is_monday(self, spark):
        # abril 2024 empieza en lunes -> devuelve el propio día 1
        df = spark.createDataFrame([("2024-04-15",)], ["d"])
        out = change_date_to_first_monday_of_the_month_column(df, "d", "dow")
        assert out.first().first_monday == date(2024, 4, 1)

    def test_first_monday_of_week(self, spark):
        from pyspark.sql.types import StructType, StructField, DateType, IntegerType
        df = spark.createDataFrame(
            [(date(2024, 6, 12), 3)],
            StructType([StructField("d", DateType()),
                        StructField("dow", IntegerType())]))
        out = change_date_first_monday_of_the_week_column(df, "d", "dow")
        assert out.first().first_monday == date(2024, 6, 10)

    def test_weeks_difference_same_week(self, spark):
        df = spark.createDataFrame(
            [("2024-06-10", 24, "2024-06-12", 24)],
            ["d1", "w1", "d2", "w2"])
        out = weeks_difference(df, "d1", "w1", "d2", "w2")
        assert out.first().weeks_diff == 0

    def test_weeks_difference(self, spark):
        df = spark.createDataFrame(
            [("2024-01-01", 1, "2024-03-04", 10)],
            ["d1", "w1", "d2", "w2"])
        out = weeks_difference(df, "d1", "w1", "d2", "w2")
        assert out.first().weeks_diff == 9

    def test_weeks_difference_cross_year(self, spark):
        df = spark.createDataFrame(
            [("2023-12-25", 52, "2024-01-08", 2)],
            ["d1", "w1", "d2", "w2"])
        out = weeks_difference(df, "d1", "w1", "d2", "w2")
        # (2024-2023)*52 + (2-52) = 2
        assert out.first().weeks_diff == 2


# ---------------------------------------------------------------------------
# WeekOfMonth* (taggers de semana de mes)
# ---------------------------------------------------------------------------
def _dates_df(spark, dates):
    return spark.createDataFrame([(d,) for d in dates], ["fecha"])


class TestWeekOfMonthTaggers:
    def test_week_of_month_is_abstract(self, spark):
        # WeekOfMonth define el esqueleto; _new_week_of_month lo implementan
        # las subclases (FourWeeksForced / Payroll)
        tagged = WeekOfMonth(_dates_df(spark, [date(2024, 6, 10)]), "fecha")
        with pytest.raises(NotImplementedError):
            tagged.tag_weeks()

    def test_four_weeks_forced_always_four(self, spark):
        # con el tagger forzado, un mes completo solo produce 4 semanas;
        # new_week_of_year = (mes-1)*4 + wom -> junio produce 21..24
        days = [date(2024, 6, d) for d in range(1, 31)]
        tagged = WeekOfMonthFourWeeksForced(
            _dates_df(spark, days), "fecha").tag_weeks()
        woy = {r.new_week_of_year for r in
               tagged.select("new_week_of_year").distinct().collect()}
        assert woy == {21, 22, 23, 24}

    def test_payroll_weeks_range(self, spark):
        # regla payroll: w1=días 1-7, w2=8-15, w3=16-21, w4=22-fin;
        # new_week_of_year = 20 + wom en junio
        days = [date(2024, 6, d) for d in range(1, 31)]
        tagged = WeekOfMonthPayroll(
            _dates_df(spark, days), "fecha").tag_weeks()
        rows = {r.fecha: r.new_week_of_year for r in
                tagged.select("fecha", "new_week_of_year").collect()}
        assert rows[date(2024, 6, 1)] == 21
        assert rows[date(2024, 6, 7)] == 21
        assert rows[date(2024, 6, 8)] == 22
        assert rows[date(2024, 6, 15)] == 22
        assert rows[date(2024, 6, 16)] == 23
        assert rows[date(2024, 6, 21)] == 23
        assert rows[date(2024, 6, 22)] == 24
        assert rows[date(2024, 6, 30)] == 24

    def test_payroll_day_of_week(self, spark):
        # new_day_of_week = día dentro de la semana payroll (1-based)
        days = [date(2024, 6, d) for d in (1, 7, 8, 15, 16, 21, 22, 30)]
        tagged = WeekOfMonthPayroll(
            _dates_df(spark, days), "fecha").tag_weeks()
        rows = {r.fecha: r.new_day_of_week for r in
                tagged.select("fecha", "new_day_of_week").collect()}
        assert rows[date(2024, 6, 1)] == 1
        assert rows[date(2024, 6, 8)] == 1
        assert rows[date(2024, 6, 16)] == 1
        assert rows[date(2024, 6, 22)] == 1
        assert rows[date(2024, 6, 30)] == 9

    def test_tag_weeks_output_columns(self, spark):
        tagged = WeekOfMonthPayroll(
            _dates_df(spark, [date(2024, 6, 10)]), "fecha").tag_weeks()
        assert sorted(tagged.columns) == [
            "fecha", "new_day_of_week", "new_week_of_year"]


# ---------------------------------------------------------------------------
# WeeklyTFromParquetReTagger — retaggea extract a semanas
# ---------------------------------------------------------------------------
class TestWeeklyTFromReTagger:
    def _build_extract(self, spark, path, rows, fecha_col="fecha"):
        df = spark.createDataFrame(rows, ["numcliente", "vintage_date",
                                          fecha_col, "valor"])
        df.write.mode("overwrite").parquet(path)
        return df

    def test_standard_tfrom(self, spark, tmp_path):
        """tfrom standard: semanas ISO hacia atrás desde el último día
        del mes vintage (30-jun-2024 = semana ISO 26)."""
        extracted = tmp_path / "extract"
        out = tmp_path / "weekly" / "t1"
        rows = [
            ("1", date(2024, 6, 30), date(2024, 6, 24), 10.0),  # wk26 -> t0
            ("1", date(2024, 6, 30), date(2024, 6, 17), 20.0),  # wk25 -> t1
            ("1", date(2024, 6, 30), date(2024, 6, 10), 30.0),  # wk24 -> t2
        ]
        self._build_extract(spark, extracted.as_posix(), rows)
        tagger = WeeklyTFromParquetReTagger(
            date_column_for_weekly_tfrom="fecha",
            tfrom_method="standard",
            type_of_date_column="daily",
            extracted_path=extracted.as_posix(),
            output_path=out.as_posix(),
            n_mis=4,
            sqlContext=spark,
            is_dynamic=False)
        tagger.run()
        result = spark.read.parquet(out.as_posix())
        tfroms = {r.fecha: r.tfrom for r in
                  result.select("fecha", "tfrom").collect()}
        assert tfroms[date(2024, 6, 24)] == 0
        assert tfroms[date(2024, 6, 17)] == 1
        assert tfroms[date(2024, 6, 10)] == 2

    def test_force_4_weeks_tfrom(self, spark, tmp_path):
        """tfrom force_4_weeks: meses de 4 semanas hacia atrás."""
        extracted = tmp_path / "extract"
        out = tmp_path / "weekly" / "t1"
        rows = [
            ("1", date(2024, 6, 30), date(2024, 6, 24), 1.0),
            ("1", date(2024, 6, 30), date(2024, 5, 27), 2.0),
        ]
        self._build_extract(spark, extracted.as_posix(), rows)
        tagger = WeeklyTFromParquetReTagger(
            date_column_for_weekly_tfrom="fecha",
            tfrom_method="force_4_weeks",
            type_of_date_column="daily",
            extracted_path=extracted.as_posix(),
            output_path=out.as_posix(),
            n_mis=4,
            sqlContext=spark,
            is_dynamic=False)
        tagger.run()
        result = spark.read.parquet(out.as_posix())
        assert "tfrom" in result.columns
        tfroms = {r.fecha: r.tfrom for r in
                  result.select("fecha", "tfrom").collect()}
        # un mes atrás = 4 tfroms (meses forzados a 4 semanas)
        assert tfroms[date(2024, 6, 24)] == 0
        assert tfroms[date(2024, 5, 27)] == 4

    def test_payroll_tfrom(self, spark, tmp_path):
        """tfrom payroll: semanas payroll (1-7/8-15/16-21/22-fin);
        un mes atrás = 4 tfroms."""
        extracted = tmp_path / "extract"
        out = tmp_path / "weekly" / "t1"
        rows = [
            ("1", date(2024, 6, 30), date(2024, 6, 22), 1.0),  # w4 junio -> t0
            ("1", date(2024, 6, 30), date(2024, 5, 25), 2.0),  # w4 mayo -> t4
        ]
        self._build_extract(spark, extracted.as_posix(), rows)
        tagger = WeeklyTFromParquetReTagger(
            date_column_for_weekly_tfrom="fecha",
            tfrom_method="payroll",
            type_of_date_column="daily",
            extracted_path=extracted.as_posix(),
            output_path=out.as_posix(),
            n_mis=4,
            sqlContext=spark,
            is_dynamic=False)
        tagger.run()
        result = spark.read.parquet(out.as_posix())
        tfroms = {r.fecha: r.tfrom for r in
                  result.select("fecha", "tfrom").collect()}
        assert tfroms[date(2024, 6, 22)] == 0
        assert tfroms[date(2024, 5, 25)] == 4

    def test_asserts_on_bad_methods(self, spark, tmp_path):
        with pytest.raises(AssertionError):
            WeeklyTFromParquetReTagger(
                date_column_for_weekly_tfrom="f",
                tfrom_method="bad_method",
                type_of_date_column="daily",
                extracted_path="/x", output_path="/y",
                n_mis=4, sqlContext=spark)
        with pytest.raises(AssertionError):
            WeeklyTFromParquetReTagger(
                date_column_for_weekly_tfrom="f",
                tfrom_method="standard",
                type_of_date_column="bad_type",
                extracted_path="/x", output_path="/y",
                n_mis=4, sqlContext=spark)
