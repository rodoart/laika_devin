"""
Tests de ``libs.extract`` — la lógica de rangos de fechas, construcción
de queries y obtención de vintages del extract (sin tocar Hive: se
prueban los métodos puros/pandas/Spark-local).
"""
from datetime import date

import pytest

from libs.extract import Extract


def _extract_stub(spark):
    """Instancia de Extract sin ejecutar el extract real."""
    pivot = spark.createDataFrame(
        [("1", date(2024, 3, 31)), ("2", date(2024, 3, 31)),
         ("3", date(2024, 2, 29))],
        ["numcliente", "vintage_date"])
    return Extract(
        pivot_table=pivot,
        working_path="/w",
        n_mis=4,
        sources_cat={},
        source="test",
        sqlContext=spark)


# ---------------------------------------------------------------------------
# _get_vintages_dates
# ---------------------------------------------------------------------------
class TestVintages:
    def test_distinct_desc_sorted(self, spark):
        ex = _extract_stub(spark)
        vintages = ex._get_vintages_dates(
            ex.pivot_table, "vintage_date", "master", spark)
        assert vintages == [date(2024, 3, 31), date(2024, 2, 29)]


# ---------------------------------------------------------------------------
# _range_dates / _range_vintages
# ---------------------------------------------------------------------------
class TestDateRanges:
    def test_range_dates(self, spark):
        ex = _extract_stub(spark)
        vintages = [date(2024, 3, 31), date(2024, 2, 29)]
        dates = ex._range_dates(vintages, retraso=1, n_mis=4)
        # cubre desde min_vintage - (n_mis+retraso-1) meses hasta max_vintage
        assert dates[0] == "2023-10-31"   # 2024-02-29 - 4 meses
        assert dates[-1] == "2024-03-31"  # max vintage
        # son fin-de-mes consecutivos
        assert len(dates) == 6

    def test_range_dates_retraso_2(self, spark):
        ex = _extract_stub(spark)
        vintages = [date(2024, 3, 31)]
        dates = ex._range_dates(vintages, retraso=2, n_mis=3)
        assert dates[-1] == "2024-02-29"  # max vintage - (retraso-1) meses

    def test_range_vintages(self, spark):
        ex = _extract_stub(spark)
        vintages = [date(2024, 3, 31)]
        rng = ex._range_vintages(vintages, retraso=1, n_mis=4)
        # (min, max): max = vintage - 1 mes; min = vintage - (1+4-1) = -4 meses
        assert rng == [("2023-11-30", "2024-02-29")]


# ---------------------------------------------------------------------------
# _create_queries
# ---------------------------------------------------------------------------
class TestCreateQueries:
    def test_yyyymm_tables(self, spark):
        ex = _extract_stub(spark)
        queries, tables = ex._create_queries(
            ["2024-01-31", "2024-02-29"], "tabla_XXXX", "db",
            "YYYYMM", "mis", "*")
        assert tables == ["tabla_202401", "tabla_202402"]
        assert queries[0] == "SELECT * FROM db.tabla_202401"
        assert len(queries) == 2

    def test_yyyym_tables(self, spark):
        ex = _extract_stub(spark)
        _, tables = ex._create_queries(
            ["2024-01-31"], "tabla_XXXX", "db", "YYYYM", "mis", "*")
        assert tables == ["tabla_20241"]

    def test_yymm_tables(self, spark):
        ex = _extract_stub(spark)
        _, tables = ex._create_queries(
            ["2024-01-31"], "tabla_XXXX", "db", "YYMM", "mis", "*")
        assert tables == ["tabla_2401"]

    def test_mmmyy_tables(self, spark):
        ex = _extract_stub(spark)
        _, tables = ex._create_queries(
            ["2024-03-31"], "tabla_XXXX", "db", "mmmYY", "mis", "*")
        assert tables == ["tabla_mar2024"]

    def test_mm_yyyy_tables(self, spark):
        ex = _extract_stub(spark)
        _, tables = ex._create_queries(
            ["2024-03-31"], "tabla_XXXX", "db", "MM_YYYY", "mis", "*")
        assert tables == ["tabla_03_2024"]

    def test_partitioned_table(self, spark):
        ex = _extract_stub(spark)
        queries, tables = ex._create_queries(
            ["2024-01-31", "2024-02-29", "2024-03-31"], "tabla", "db",
            "", "mis_col", "a, b")
        # n-1 queries de rango [d_k, d_k+1)
        assert len(queries) == 2
        assert "mis_col >= '2024-01-31' and mis_col < '2024-02-29'" in queries[0]
        assert tables == ["tabla_202401", "tabla_202402"]
