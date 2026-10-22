"""Tests del reporte de corrida (``libs.notifications``).

El builder es puro (sin Spark); las helpers ``*_stats`` usan el fixture
``spark`` de ``conftest.py``.
"""
import re
from datetime import datetime

import pytest

from libs.notifications import (build_run_report, count_distinct_clients,
                                feature_stats)


class _Step:
    """Step mínimo con la API que consume el reporte."""

    def __init__(self, stage, status="computed", dur=1.0):
        self.stage = stage
        self.last_status = status
        self.action_duration = dur
        self.output_path = f"/w/202601/src/{stage}"
        self.write_path = "/w/202601/src"


CC = {"year_id": "202601", "n_mis": 6}
T0 = datetime(2026, 10, 22, 15, 0, 0)


def _report(status, chain, **kw):
    args = dict(source="src", chain=chain, status=status, started_at=T0,
                finished_at=T0, config_custom=CC)
    args.update(kw)
    return build_run_report(**args)


class TestBuildRunReport:
    def test_success_subject_and_table(self):
        chain = [_Step("extract", "computed", 7.3),
                 _Step("fe", "reloaded", 0.2)]
        subject, body = _report(
            "success", chain, pivot_clients=5, final_clients=3,
            final_columns=42,
            trans_list=["sum_mt"], feature_detail=[("sum_mt", 10)])
        assert subject.startswith("[OK] Datapull src")
        assert "feat cols" in subject
        assert "computed" in body and "reloaded" in body
        assert "Clientes en pivot" in body
        assert "Clientes en pivot</b> 5<" in body

    def test_failed_marks_step_and_escapes_traceback(self):
        chain = [_Step("extract", "computed", 7.3),
                 _Step("groupby", "failed", 4.0),
                 _Step("fe", "pending", 0.0)]
        subject, body = _report(
            "failed", chain,
            error_traceback="RuntimeError: <boom>&")
        assert "failed at groupby" in subject
        assert 'class="failed"' in body
        assert "&lt;boom&gt;&amp;" in body
        assert "<boom>" not in body

    def test_failed_without_failed_step(self):
        subject, _ = _report("failed", [_Step("extract", "pending", 0)])
        assert "failed at" not in subject


class TestStatsHelpers:
    def test_count_distinct_clients(self, spark, tmp_path):
        p = tmp_path / "out"
        spark.createDataFrame(
            [("c1", "v"), ("c1", "v"), ("c2", "v")],
            ["numcliente", "x"]).write.parquet(str(p))
        assert count_distinct_clients(spark, str(p)) == 2

    def test_count_missing_path(self, spark, tmp_path):
        assert count_distinct_clients(
            spark, str(tmp_path / "nope")) is None

    def test_feature_stats_skips_keys_and_missing(self, spark, tmp_path):
        fdir = tmp_path / "features"
        spark.createDataFrame(
            [("c1", "v", 1.0, 2.0)],
            ["numcliente", "vintage_date", "sum_mt_a_4w", "sum_mt_b_4w"]
        ).write.parquet(str(fdir / "sum_mt_features"))
        total, detail = feature_stats(
            spark, str(fdir), ["sum_mt", "no_salio_mt"])
        assert total == 2
        assert detail == [("sum_mt", 2)]
