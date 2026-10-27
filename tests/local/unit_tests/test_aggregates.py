"""
Tests de ``libs.aggregattes`` — el comportamiento compartido de las
clases ``*Aggregate`` (``_save_parquet``, ``_create_groupby_query``) y
el ``aggregate()`` end-to-end de ``ChannelsAggregate`` y
``BureauCohortAggregate`` sobre tablas pequeñas.
"""
import pytest

from libs.aggregattes.channels import ChannelsAggregate
from libs.aggregattes.bureau_cohort import BureauCohortAggregate
from libs.aggregattes.product_data import ProductDataAggregate
from libs.aggregattes.corporate import CorporateAggregate


# ---------------------------------------------------------------------------
# _save_parquet (comportamiento común de todos los agregados)
# ---------------------------------------------------------------------------
class TestSaveParquet:
    def test_writes_parquet(self, spark, tmp_path):
        agg = ChannelsAggregate(str(tmp_path), {}, spark)
        df = spark.createDataFrame([(1, 2.0)], ["a", "b"])
        out = tmp_path / "out"
        agg._save_parquet(df, out.as_posix())
        assert (out / "_SUCCESS").exists()
        back = spark.read.parquet(out.as_posix())
        assert back.count() == 1

    def test_overwrite_mode(self, spark, tmp_path):
        out = tmp_path / "out"
        agg = ChannelsAggregate(str(tmp_path), {}, spark)
        agg._save_parquet(
            spark.createDataFrame([(1,)], ["x"]), out.as_posix())
        agg._save_parquet(
            spark.createDataFrame([(1,), (2,)], ["x"]), out.as_posix())
        assert spark.read.parquet(out.as_posix()).count() == 2


# ---------------------------------------------------------------------------
# ChannelsAggregate.aggregate — query SQL + flags/sums
# ---------------------------------------------------------------------------
class TestChannelsAggregate:
    def test_aggregate_end_to_end(self, spark, tmp_path):
        """Lee <prev>/<table_name>, corre query_str, aplica flags/sums
        y guarda en <head>/groupby."""
        tbl = tmp_path / "extract" / "ch_table"
        spark.createDataFrame(
            [("1", "v1", 0, 2, 0),
             ("1", "v1", 1, 1, 3)],
            ["numcliente", "vintage_date", "tfrom", "a", "b"]
        ).write.mode("overwrite").parquet(tbl.as_posix())

        agg_dict = {
            "table_name": "ch_table",
            "query_str": ("select numcliente, vintage_date, tfrom, "
                          "a, b from channels"),
            "query_dict": {
                "flags": {"flag_ab": ["a", "b"]},
                "sums": {"sum_ab": ["a", "b"]},
            },
        }
        agg = ChannelsAggregate(
            previous_path=(tmp_path / "extract").as_posix(),
            agg_dict=agg_dict, sqlContext=spark)
        agg.aggregate()

        out = spark.read.parquet((tmp_path / "groupby").as_posix())
        rows = {r.tfrom: r.asDict() for r in out.collect()}
        # flags: greatest(a,b); sums: a+b
        assert rows[0]["flag_ab"] == 2 and rows[0]["sum_ab"] == 2
        assert rows[1]["flag_ab"] == 3 and rows[1]["sum_ab"] == 4
        # nulos rellenados a 0
        assert out.filter("a is null").count() == 0


# ---------------------------------------------------------------------------
# BureauCohortAggregate._create_groupby_query
# ---------------------------------------------------------------------------
class TestBureauCohortQuery:
    def test_create_groupby_query(self, spark):
        agg = BureauCohortAggregate("/p", {}, spark)
        query_dict = {
            "agg": ["sum", "max"],
            "variable": ["x", "y"],
            "feature": ["fx", "fy"],
        }
        q = agg._create_groupby_query(
            query_dict, ["numcliente", "vintage_date"], ["x", "y"])
        assert "GROUP BY numcliente,vintage_date" in q
        assert "sum(case when x> 0" in q
        assert "max(case when y> 0" in q
        assert "FROM tempBureau" in q

    def test_filters_variables_not_in_columns(self, spark):
        agg = BureauCohortAggregate("/p", {}, spark)
        query_dict = {
            "agg": ["sum", "sum"],
            "variable": ["x", "z"],
            "feature": ["fx", "fz"],
        }
        q = agg._create_groupby_query(
            query_dict, ["id"], ["x"])  # 'z' no está en columnas
        assert "x> 0" in q
        assert "z> 0" not in q


# ---------------------------------------------------------------------------
# ProductDataAggregate._create_groupby_query
# ---------------------------------------------------------------------------
class TestProductDataQuery:
    def test_query_uses_tempview(self, spark):
        agg = ProductDataAggregate("/p", {}, 4, 2, spark)
        query_dict = {"agg": ["sum"], "variable": ["x"], "feature": ["fx"]}
        q = agg._create_groupby_query(
            query_dict, ["numcliente"], "tempProd")
        assert "FROM tempProd" in q
        assert "GROUP BY numcliente" in q


# ---------------------------------------------------------------------------
# BaseAggregate._nested_when + CorporateAggregate.aggregate
# ---------------------------------------------------------------------------
class TestNestedWhen:
    def test_chains_multiple_pairs(self, spark):
        """``otherwise()`` solo puede aplicarse una vez sobre un when —
        encadenar pares con ``.otherwise(when(...))`` lanza
        IllegalArgumentException (crash del mail de corporate). La
        cadena debe construirse con ``.when()``."""
        from pyspark.sql.functions import col
        expr = CorporateAggregate._nested_when(
            [(col("a") == 0, 1), (col("a") == 1, 2), (col("a") == 2, 3)],
            default=-99999)
        df = spark.createDataFrame([(0,), (1,), (2,), (7,)], ["a"])
        out = df.withColumn("cat", expr)
        assert [r.cat for r in out.orderBy("a").collect()] == \
            [1, 2, 3, -99999]

    def test_corporate_aggregate_end_to_end(self, spark, tmp_path):
        """``aggregate()`` evalúa los 4 cat_*_seg: 6-15 pares por
        columna (seg × nivel) — exactamente el IllegalArgumentException
        del mail."""
        tbl = tmp_path / "extract" / "corp_table"
        spark.createDataFrame(
            [("PENSIONADOS", 0, 3, 1, 2, "ind_a"),
             ("GOBIERNO", 1, 0, 0, 4, "ind_b"),
             ("OTRO", 1, 4, 7, 9, "ind_c")],
            ["seg", "pct_ind_i2", "pct_ind_i5", "pct_iv2", "pct_iv5",
             "f_group_industry"]
        ).write.mode("overwrite").parquet(tbl.as_posix())

        agg_dict = {
            "table_name": "corp_table",
            "conditions": [],
            "new_columns": ["cat_ind_i2_seg", "cat_iv2_seg",
                            "cat_ind_i5_seg", "cat_iv5_seg"],
            "drop_columns": ["f_group_industry", "seg"],
        }
        agg = CorporateAggregate(
            previous_path=(tmp_path / "extract").as_posix(),
            agg_dict=agg_dict, sqlContext=spark)
        agg.aggregate()

        out = spark.read.parquet(
            (tmp_path / "groupby" / "corp_table").as_posix())
        rows = [r.asDict() for r in out.collect()]
        # PENSIONADOS: grupo 0 -> nivel+1; GOBIERNO: grupo 1; resto: 2
        pens = next(r for r in rows if r["cat_ind_i2_seg"] == 1)
        gob = next(r for r in rows if r["cat_ind_i2_seg"] == 4)
        otro = next(r for r in rows if r["cat_ind_i2_seg"] == 6)
        assert pens["cat_ind_i5_seg"] == 4 and pens["cat_iv2_seg"] == 2
        assert gob["cat_ind_i5_seg"] == 6 and gob["cat_iv5_seg"] == 10
        assert otro["cat_ind_i5_seg"] == 15
        assert otro["cat_iv2_seg"] == -99999  # pct_iv2=7 fuera de rango
        assert "seg" not in out.columns
        assert "f_group_industry" not in out.columns
