"""
Tests de ``libs.missing_treatment`` — estrategias de relleno de nulos
(``fill_value``, ``fill_stat``, ``fill_snap``, ``fill_historic``).

Las funciones de nivel módulo usadas para el cálculo de MOBs
(``get_numeric_columns``, ``count_non_null_values``,
``get_last_contiguous_non_null_value``, ``contiguous_mobs``,
``discontiguous_mobs``) viven ahora en ``libs.mobs.mobs`` y se prueban
en ``test_mobs.py``.
"""
import pytest
from pyspark.sql import functions as F

from libs.missing_treatment import MissingTreatment


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
# run_mt: regresión cross-table con partitionOverwriteMode=dynamic
# ---------------------------------------------------------------------------
class TestRunMtDynamicOverwrite:
    def test_temp_checkpoint_no_contamina_entre_tablas(self, spark, tmp_path):
        """Bug prod: en cluster la sesión nace con
        partitionOverwriteMode=dynamic. El temp_0 compartido retenía las
        particiones de la tabla anterior que la siguiente no cubría
        (asigna tfrom {0,4} vs nomina {2,6}) -> el output de nomina
        salía con schema de asigna (perdía ``importe``/``fecha``).
        """
        conf = "spark.sql.sources.partitionOverwriteMode"
        prev = spark.conf.get(conf, "static")
        spark.conf.set(conf, "dynamic")
        try:
            extract_dir = tmp_path / "extract"
            asigna = spark.createDataFrame(
                [(1, "2026-06-01", 0.0), (2, "2026-06-01", 4.0)],
                ["numcliente", "vintage_date", "tfrom"])
            nomina = spark.createDataFrame(
                [(3, "2026-06-01", 2.0, 100.5), (4, "2026-06-01", 6.0, None)],
                ["numcliente", "vintage_date", "tfrom", "importe"])
            for name, df in (("asigna_historic", asigna),
                             ("nomina_historic", nomina)):
                df.write.mode("overwrite") \
                    .partitionBy("vintage_date", "tfrom") \
                    .parquet(str(extract_dir / name))

            mt_dict = {
                "missing_treatment": {
                    "fill_variables": {"variable": ["importe"],
                                       "metodo": ["real0"]},
                    "fill_methods": {
                        "id_variables": ["snapshot"],
                        "metodo": ["fill_value"],
                        "grouped": [""],
                        "valor": ["Real 0"],
                    },
                }
            }
            mt = MissingTreatment(year="202401", mt_dict=mt_dict,
                                  source="src", n_mis=4, n_threads=1)
            mt.run_mt(str(extract_dir),
                      ["asigna_historic", "nomina_historic"],
                      str(tmp_path), spark)

            out_dir = (tmp_path / "202401" / "src" / "missing_treatment"
                       / "nomina_historic")
            out = spark.read.parquet(str(out_dir))
            assert "importe" in out.columns
            rows = {r.numcliente: r for r in out.collect()}
            assert sorted(rows) == [3, 4]
            assert rows[3].importe == 100.5
            assert rows[4].importe == 0.0   # fill_value real0
        finally:
            spark.conf.set(conf, prev)
