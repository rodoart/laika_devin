"""
Tests de ``libs.generate_pivot`` (long -> wide), ``libs.colums_2_vector``
(empaquetado final en ``features_vector``) y el dispatcher
``libs.group_by.GroupBy._obtain_object``.
"""
import pytest

from libs.generate_pivot import GeneratePivot
from libs.colums_2_vector import ColumnsToVector
from libs.group_by import GroupBy


# ---------------------------------------------------------------------------
# GeneratePivot
# ---------------------------------------------------------------------------
class TestGeneratePivot:
    def test_pivot_data_aux(self, spark, tmp_path):
        """La tabla larga (id, tfrom, vars...) pasa a ancho con
        <var>_<tfrom> por columna."""
        src = tmp_path / "long_data"
        spark.createDataFrame(
            [("1", "v1", 0, 10.0, 20.0),
             ("1", "v1", 1, 30.0, 40.0)],
            ["numcliente", "vintage_date", "tfrom", "a", "b"]
        ).write.mode("overwrite").parquet(src.as_posix())

        gp = GeneratePivot(src.as_posix(), {"source": "test"}, spark)
        gp.pivot_data_aux(src.as_posix(), ["numcliente", "vintage_date", "tfrom"])

        out = spark.read.parquet((tmp_path / "pivot").as_posix())
        assert out.count() == 1
        row = out.first().asDict()
        assert row["a_0"] == 10.0 and row["a_1"] == 30.0
        assert row["b_0"] == 20.0 and row["b_1"] == 40.0

    def test_pivot_data(self, spark, tmp_path):
        """pivot_data con source != corporate escribe en <head>/pivot."""
        src = tmp_path / "data_in"
        spark.createDataFrame(
            [("1", "v1", 0, 5.0), ("1", "v1", 1, 7.0)],
            ["numcliente", "vintage_date", "tfrom", "x"]
        ).write.mode("overwrite").parquet(src.as_posix())

        gp = GeneratePivot(
            previous_path=src.as_posix(),
            pivot_dict={"source": "s15",
                        "pivot_key": ["numcliente", "vintage_date", "tfrom"]},
            sqlContext=spark)
        gp.pivot_data()
        out = spark.read.parquet((tmp_path / "pivot").as_posix())
        row = out.first().asDict()
        assert row["x_0"] == 5.0 and row["x_1"] == 7.0


# ---------------------------------------------------------------------------
# ColumnsToVector
# ---------------------------------------------------------------------------
class TestColumnsToVector:
    def test_run_assembles_vector(self, spark, tmp_path):
        """Genera <write_path>/vector_assembler con numcliente, vintage
        y features_vector."""
        feats = tmp_path / "feats"
        spark.createDataFrame(
            [("1", "2024-01", "id1", 1.0, 2.0, 3.0)],
            ["numcliente", "vintage_date", "id_msf_tools_v2_12315",
             "f1", "f2", "f3"]
        ).write.mode("overwrite").parquet(feats.as_posix())

        c2v = ColumnsToVector(
            write_path=tmp_path.as_posix(),
            v2c_dict={"source": "s15", "prev_path": "feats",
                      "table_name": "t"},
            sqlContext=spark)
        c2v.run()

        out = spark.read.parquet((tmp_path / "vector_assembler").as_posix())
        # ``vintage`` es columna de particion -> Spark la mueve al final
        # del esquema al releer el parquet particionado
        assert set(out.columns) == {"numcliente", "vintage",
                                    "features_vector"}
        row = out.first()
        assert list(row.features_vector.toArray()) == [1.0, 2.0, 3.0]
        assert row.numcliente == "1"
        assert row.vintage == "2024-01"


# ---------------------------------------------------------------------------
# GroupBy dispatcher
# ---------------------------------------------------------------------------
class TestGroupByDispatcher:
    def _gb(self, source):
        return GroupBy(source=source, previous_path="/p", agg_dict={},
                       sqlContext=None, n_mis=4, period=2)

    @pytest.mark.parametrize("source,cls_name", [
        ("financiera", "FinancieraAggregate"),
        ("channels", "ChannelsAggregate"),
        ("product_data", "ProductDataAggregate"),
        ("corporate", "CorporateAggregate"),
        ("profundidad", "ProfundidadAggregate"),
        ("pnl", "PNLAggregate"),
        ("pnl_new", "PNLNewAggregate"),
        ("pnl_cohort", "PNLCohortAggregate"),
        ("laika", "LaikaAggregate"),
        ("suguru", "SuguruAggregate"),
        ("s15_nf", "S15NFAggregate"),
        ("s15", "S15Aggregate"),
        ("morkitara", "MorkitaraAggregate"),
        ("atms", "AtmsAggregate"),
        ("giros_credito", "MacrogirosAggregate"),
        ("bureau_cohort", "BureauCohortAggregate"),
        ("bureau_pull", "BureauPullAggregate"),
        ("payroll_income", "PayrollIncomeAggregate"),
        ("payroll_streaks", "PayrollStreaksAggregate"),
    ])
    def test_obtain_object(self, source, cls_name):
        assert self._gb(source)._obtain_object().__name__ == cls_name

    def test_liabilities_source(self):
        cls = self._gb("liabilities")._obtain_object()
        assert cls.__name__ == "LiabilitiesPullAggregate"

    def test_rev_balance_source(self):
        cls = self._gb("rev_balance")._obtain_object()
        assert cls.__name__ == "RevBalanceAggregate"
