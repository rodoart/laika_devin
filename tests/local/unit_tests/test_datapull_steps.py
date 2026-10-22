"""
Tests de ``libs.datapull_steps`` — la construcción de la cadena
``previous_step`` por fuente (``build_source_chain``), el propagado de
``previous_path`` y la lógica de recarga dinámica de los steps.

Se usa un ``FakeDataPull`` con los atributos mínimos
(``config_custom``/``config_rigid``) para no construir el objeto real.
"""
import pytest

from libs.datapull_steps import (
    STAGE_STEP_CLASSES,
    Cols2VecStep,
    DatapullStageStep,
    ExtractStep,
    FeStep,
    GroupByStep,
    MtStep,
    PivotingStep,
    RatiosStep,
    VaStep,
    WeeksStep,
    MobsStep,
    build_source_chain,
)
from libs.paths import PathsConfig
from libs.steps import Step


# ---------------------------------------------------------------------------
# Fixtures: config mínima + fake datapull
# ---------------------------------------------------------------------------
@pytest.fixture
def paths_config(tmp_path):
    """paths.json de test — réplica del layout real de config/paths.json."""
    import json
    stages = {
        "extract":  {"path": "extract",            "keep_or_delete": "keep"},
        "weeks":    {"path": "weekly_extract",     "keep_or_delete": "delete"},
        "mt":       {"path": "missing_treatment",  "keep_or_delete": "delete"},
        "groupby":  {"path": "groupby",            "keep_or_delete": "keep"},
        "pivoting": {"path": "pivot",              "keep_or_delete": "delete"},
        "ratios":   {"path": "ratios",             "keep_or_delete": "keep"},
        "fe":       {"path": "features",           "keep_or_delete": "keep"},
        "mobs":     {"path": "mobs/imputate_mobs", "keep_or_delete": "keep"},
        "va":       {"path": "vector_assembler",   "keep_or_delete": "keep"},
        "cols2vec": {"path": "vector_assembler",   "keep_or_delete": "keep"},
    }
    cfg_path = tmp_path / "paths.json"
    cfg_path.write_text(json.dumps({"stages": stages}))
    return PathsConfig(str(cfg_path))


class FakeDataPull:
    """DataPull mínimo para construir la cadena de steps."""

    def __init__(self, working_path, steps_list, source_dict_extra=None):
        self.config_custom = {
            "working_path": working_path,
            "year_id": "202601",
            "n_mis": 4,
            "n_threads": 1,
            "write_mode": "overwrite",
            "period": 2,
            "pivot_path": "/pivot",
            "lista_trans": [],
            "lista_trans_per_source": None,
        }
        src = {
            "steps_list": steps_list,
            "extract": {"table_name": ["t1"]},
            "group_by": {},
            "pivoting": {},
            "fe": {},
            "cols2vec": {},
        }
        if source_dict_extra:
            src.update(source_dict_extra)
        self.config_rigid = {"payroll_streaks": src}
        self.sqlContext = None
        self.calls = []  # registro de _perform_* invocados

    # stubs de los _perform_* / _check_*
    def _check_extract(self, path, extract_dict):
        return []
    def _check_missing(self, path, tables):
        return []
    def _check_files(self, path):
        return []
    def _check_mobs(self, path, files):
        return []
    def _fe_performed(self, path, trans):
        return []
    def _get_time_window(self, n_mis, period):
        return [2, 4]
    def _perform_extract(self, *a, **kw):
        self.calls.append(("extract", a, kw))
    def _perform_weeks(self, *a):
        self.calls.append(("weeks", a))
    def _perform_mt(self, *a):
        self.calls.append(("mt", a))
    def _perform_groupby(self, **kw):
        self.calls.append(("groupby", kw))
    def _perform_pivoting(self, *a):
        self.calls.append(("pivoting", a))
    def _perform_fe(self, *a):
        self.calls.append(("fe", a))
    def _create_mobs_extract(self):
        self.calls.append(("mobs_extract",))
    def _perform_mobs(self, *a):
        self.calls.append(("mobs", a))
    def _perform_va(self, **kw):
        self.calls.append(("va", kw))
    def _perform_cols2vec(self, *a):
        self.calls.append(("cols2vec", a))


# ---------------------------------------------------------------------------
# build_source_chain
# ---------------------------------------------------------------------------
class TestBuildSourceChain:
    def test_chain_order(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(),
                          ["extract", "weeks", "mt", "groupby"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        # caminar la cadena hacia atrás
        names = []
        node = last
        while node is not None:
            names.append(node.step_name)
            node = node.previous_step
        assert names == ["payroll_streaks_groupby", "payroll_streaks_mt",
                         "payroll_streaks_weeks", "payroll_streaks_extract"]

    def test_step_filter(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(),
                          ["extract", "weeks", "mt", "groupby"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config,
                                  step_filter=["extract", "groupby"])
        assert isinstance(last, GroupByStep)
        assert isinstance(last.previous_step, ExtractStep)

    def test_step_classes_mapping(self):
        """Cada etapa del mapeo tiene una clase registrada."""
        for stage in ("extract", "weeks", "mt", "groupby", "pivoting",
                      "ratios", "fe", "mobs", "va", "cols2vec"):
            assert stage in STAGE_STEP_CLASSES

    def test_output_hive_config(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["extract"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config)
        cfg = last.output_hive["extract"]
        assert cfg["table_or_hdfs"].endswith(
            "202601/payroll_streaks/extract")
        assert cfg["keep_or_delete"] == "keep"

    def test_historic_tables(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["extract"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config)
        assert last._historic_tables == ["t1_historic"]


# ---------------------------------------------------------------------------
# Ejecución de la cadena (recarga vs. recomputo)
# ---------------------------------------------------------------------------
class TestChainExecution:
    def test_execute_calls_performs_in_order(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(),
                          ["extract", "weeks", "mt"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        last.execute()
        assert [c[0] for c in dp.calls] == ["extract", "weeks", "mt"]

    def test_n_mis_adjustments(self, tmp_path, paths_config):
        """weeks sube n_mis +1 en extract y lo restaura tras correr."""
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "weeks"])
        original_n_mis = dp.config_custom["n_mis"]
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        last.execute()
        assert dp.config_custom["n_mis"] == original_n_mis

    def test_dynamic_reload_skips_perform(self, tmp_path, paths_config,
                                          spark):
        """is_dynamic + salida existente -> no llama _perform_*."""
        # precreamos la salida de extract para que _already_done sea True
        extract_out = (tmp_path / "202601" / "payroll_streaks" / "extract")
        (extract_out / "t1_historic").mkdir(parents=True)
        (extract_out / "t1_historic" / "_SUCCESS").write_text("")
        dp = FakeDataPull(tmp_path.as_posix(), ["extract"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=True)
        last.execute()
        assert dp.calls == []  # se recargó, no computó

    def test_previous_path_propagates(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "groupby"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        last.execute()
        # groupby recibió el previous_path del extract
        gb_call = [c for c in dp.calls if c[0] == "groupby"][0][1]
        assert gb_call["previous_path"].endswith("/extract")

    def test_va_passes_previous_path_through(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "va"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        last.execute()
        # va escribe vector_assembler pero previous_path sigue siendo el de entrada
        va_call = [c for c in dp.calls if c[0] == "va"][0][1]
        assert va_call["path_features"].endswith("/extract")

    def test_groupby_output_path(self, tmp_path, paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["groupby"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        assert last.output_path.endswith("payroll_streaks/groupby")


# ---------------------------------------------------------------------------
# Comportamientos específicos de etapas
# ---------------------------------------------------------------------------
class TestStageSpecifics:
    def test_extract_n_mis_bump_is_scoped(self, tmp_path, paths_config):
        """Extract con 'weeks' en steps_list usa n_mis+1 durante la llamada
        y lo restaura después (determinismo al reanudar)."""
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "weeks"])
        seen = []
        orig = dp._perform_extract
        def spy(*a, **kw):
            seen.append(dp.config_custom["n_mis"])
            return orig(*a, **kw)
        dp._perform_extract = spy
        step = ExtractStep(datapull=dp, source="payroll_streaks",
                           paths=paths_config, is_dynamic=False)
        step._run(None)
        assert seen == [5]                      # bump aplicado dentro
        assert dp.config_custom["n_mis"] == 4   # restaurado fuera

    def test_extract_n_mis_unchanged_without_weeks(self, tmp_path,
                                                   paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["extract"])
        step = ExtractStep(datapull=dp, source="payroll_streaks",
                           paths=paths_config, is_dynamic=False)
        step._run(None)
        assert dp.config_custom["n_mis"] == 4

    def test_weeks_n_mis_bump_is_scoped(self, tmp_path, paths_config):
        """Weeks usa n_mis+1 durante la llamada y lo restaura después."""
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "weeks"])
        seen = []
        orig = dp._perform_weeks
        def spy(*a, **kw):
            seen.append(dp.config_custom["n_mis"])
            return orig(*a, **kw)
        dp._perform_weeks = spy
        step = WeeksStep(datapull=dp, source="payroll_streaks",
                         paths=paths_config, is_dynamic=False)
        step._run("/x/extract")
        assert seen == [5]
        assert dp.config_custom["n_mis"] == 4

    def test_groupby_payroll_income_n_mis_plus_two(self, tmp_path,
                                                   paths_config):
        """Groupby de payroll_income recibe n_mis+2 sin mutar el config."""
        dp = FakeDataPull(tmp_path.as_posix(), ["groupby"])
        dp.config_rigid["payroll_income"] = dict(
            dp.config_rigid["payroll_streaks"])
        captured = {}
        def spy(**kw):
            captured.update(kw)
        dp._perform_groupby = spy
        step = GroupByStep(datapull=dp, source="payroll_income",
                           paths=paths_config, is_dynamic=False)
        step._run("/x/missing_treatment")
        assert captured["n_mis"] == 6
        assert dp.config_custom["n_mis"] == 4

    def test_ratios_corporate_next_path_is_features(self, tmp_path,
                                                    paths_config):
        dp = FakeDataPull(tmp_path.as_posix(), ["ratios"])
        dp.config_rigid["corporate"] = dict(dp.config_rigid["payroll_streaks"])
        step = RatiosStep(datapull=dp, source="corporate",
                          paths=paths_config, is_dynamic=False)
        # _next_path de corporate apunta a features
        assert step._next_path("/x").endswith("/corporate/features")

    def test_tmp_paths_include_weekly_extract(self, tmp_path, paths_config):
        """La salida delete de weekly_extract viaja a tmp_paths."""
        dp = FakeDataPull(tmp_path.as_posix(), ["extract", "weeks"])
        last = build_source_chain(dp, "payroll_streaks",
                                  paths=paths_config, is_dynamic=False)
        last.execute()
        all_tmp = last.collect_tmp_paths()
        assert any("weekly_extract" in p for p in all_tmp)
