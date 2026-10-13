"""
Tests del framework :class:`libs.steps.Step` — ejecución encadenada,
``cached_property``, decoradores ``dynamic_parquet*`` (reload vs.
recompute) y recolección/borrado de ``tmp_paths``.
"""
import logging

import pytest

from libs.steps import (
    Step,
    cached_property,
    collect_step_output,
    dynamic_parquet,
    dynamic_parquet_with_path,
    inherit_parent_step_attributes,
    run_substep_and_collect,
)


# ---------------------------------------------------------------------------
# cached_property
# ---------------------------------------------------------------------------
class TestCachedProperty:
    class _Dummy:
        calls = 0

        @cached_property
        def value(self):
            type(self).calls += 1
            return type(self).calls * 10

    def test_computes_once(self):
        d = self._Dummy()
        assert d.value == 10
        assert d.value == 10  # segundo acceso: cache
        assert self._Dummy.calls == 1

    def test_cache_per_instance(self):
        self._Dummy.calls = 0
        a, b = self._Dummy(), self._Dummy()
        _ = a.value
        assert b.value == 20  # instancia nueva -> recalcula

    def test_class_access_returns_descriptor(self):
        assert isinstance(self._Dummy.__dict__["value"], cached_property)


# ---------------------------------------------------------------------------
# Step: construcción y ejecución
# ---------------------------------------------------------------------------
class _Counter(Step):
    """Step mínimo que escribe un valor en output_parameters."""

    def step_action(self):
        self.output_parameters[self.step_name]["ran"] = True
        return self.output_parameters


class TestStepBasics:
    def test_default_step_name_format(self):
        s = _Counter()
        assert s.step_name.startswith("__counter_")

    def test_explicit_step_name(self):
        s = _Counter(step_name="mi_paso")
        assert s.step_name == "mi_paso"
        assert str(s) == "Step(mi_paso)"
        assert repr(s) == "Step(mi_paso)"

    def test_step_action_not_implemented(self):
        with pytest.raises(NotImplementedError):
            Step().execute()

    def test_execute_runs_action(self):
        s = _Counter(step_name="a")
        s.execute()
        assert s.output_parameters["a"]["ran"] is True

    def test_previous_chain_executes_in_order(self):
        order = []

        class _Rec(_Counter):
            def __init__(self, tag, **kw):
                self.tag = tag
                super().__init__(**kw)

            def step_action(self):
                order.append(self.tag)
                return super().step_action()

        s1 = _Rec("first", step_name="s1")
        s2 = _Rec("second", step_name="s2", previous_step=s1)
        s3 = _Rec("third", step_name="s3", previous_step=s2)
        s3.execute()
        assert order == ["first", "second", "third"]

    def test_previous_list_executes_all(self):
        ran = []

        class _Rec(_Counter):
            def __init__(self, tag, **kw):
                self.tag = tag
                super().__init__(**kw)

            def step_action(self):
                ran.append(self.tag)
                return super().step_action()

        p1 = _Rec("p1", step_name="p1")
        p2 = _Rec("p2", step_name="p2")
        end = _Rec("end", step_name="end", previous_step=[p1, p2])
        end.execute()
        assert ran == ["p1", "p2", "end"]

    def test_input_parameters_merged_from_previous(self):
        class _Producer(Step):
            def step_action(self):
                self.output_parameters[self.step_name]["k"] = "v"
                return self.output_parameters

        class _Consumer(Step):
            def step_action(self):
                # las salidas del step previo se mezclan en input_parameters
                merged = dict(self.input_parameters)
                self.output_parameters[self.step_name]["seen"] = merged
                return self.output_parameters

        p = _Producer(step_name="prod")
        c = _Consumer(step_name="cons", previous_step=p)
        c.execute()
        assert "prod" in c.input_parameters or "k" in str(c.input_parameters)

    def test_execute_logs_format(self, caplog):
        with caplog.at_level(logging.INFO, logger="libs.steps"):
            _Counter(step_name="mi_step").execute()
        messages = [r.message for r in caplog.records]
        assert "Executing step: mi_step" in messages
        assert any(m.startswith("Step finished: mi_step") for m in messages)


# ---------------------------------------------------------------------------
# tmp_paths: recolección y borrado
# ---------------------------------------------------------------------------
class TestTmpPaths:
    def test_collect_from_output_hive_delete(self):
        s = _Counter(output_hive={
            "out1": {"table_or_hdfs": "/tmp/x1", "keep_or_delete": "delete"},
            "out2": {"table_or_hdfs": "/tmp/x2", "keep_or_delete": "keep"},
            "out3": "/tmp/x3",
        })
        assert s.collect_tmp_paths() == ["/tmp/x1"]

    def test_collect_from_tmp_paths_attr(self):
        s = _Counter()
        s.tmp_paths = ["/tmp/a", "/tmp/b"]
        assert set(s.collect_tmp_paths()) == {"/tmp/a", "/tmp/b"}

    def test_collect_recursive_over_previous(self):
        s1 = _Counter(output_hive={
            "o": {"table_or_hdfs": "/tmp/up", "keep_or_delete": "delete"}})
        s2 = _Counter(previous_step=s1, output_hive={
            "o": {"table_or_hdfs": "/tmp/down", "keep_or_delete": "delete"}})
        assert set(s2.collect_tmp_paths()) == {"/tmp/up", "/tmp/down"}

    def test_collect_deduplicates(self):
        s = _Counter()
        s.tmp_paths = ["/tmp/a", "/tmp/a"]
        assert s.collect_tmp_paths() == ["/tmp/a"]

    def test_delete_tmp_paths_removes_dirs(self, tmp_path):
        d = tmp_path / "inter"
        d.mkdir()
        (d / "f.parquet").write_text("x")
        s = _Counter(output_hive={
            "o": {"table_or_hdfs": d.as_posix(),
                  "keep_or_delete": "delete"}})
        deleted = s.delete_tmp_paths()
        assert deleted == [d.as_posix()]
        assert d.exists() is False

    def test_delete_tmp_paths_skip_missing(self, tmp_path):
        s = _Counter()
        s.tmp_paths = [tmp_path.as_posix() + "/ghost"]
        assert s.delete_tmp_paths() == []  # solo warning
        with pytest.raises(Exception):
            s.delete_tmp_paths(skip_missing=False)


# ---------------------------------------------------------------------------
# Decoradores dynamic_parquet*
# ---------------------------------------------------------------------------
class _DynamicStep(Step):
    """Step con un método decorado por dynamic_parquet."""

    def __init__(self, spark, out_path, **kw):
        self._spark = spark
        self._out_path = out_path
        self.compute_calls = 0
        super().__init__(
            sqlContext=spark,
            output_hive={"out": {"table_or_hdfs": out_path,
                                 "keep_or_delete": "keep"}},
            **kw)

    @dynamic_parquet("out")
    def data(self):
        self.compute_calls += 1
        return self.sqlContext.createDataFrame(
            [(1, "a"), (2, "b")], ["id", "v"])

    @dynamic_parquet("tmp_del")
    def data_to_delete(self):
        self.compute_calls += 1
        return self.sqlContext.createDataFrame([(1,)], ["x"])


class TestDynamicParquet:
    def test_computes_and_saves_when_missing(self, spark, tmp_path):
        out = tmp_path / "out"
        s = _DynamicStep(spark, out.as_posix(), step_name="dyn")
        df = s.data()
        assert s.compute_calls == 1
        assert out.exists() and (out / "_SUCCESS").exists()
        assert sorted(r.v for r in df.collect()) == ["a", "b"]

    def test_reloads_when_present(self, spark, tmp_path):
        out = tmp_path / "out"
        s = _DynamicStep(spark, out.as_posix(), step_name="dyn", is_dynamic=True)
        _ = s.data()
        assert s.compute_calls == 1
        _ = s.data()  # segunda llamada -> recarga de disco
        assert s.compute_calls == 1

    def test_is_dynamic_false_recomputes(self, spark, tmp_path):
        out = tmp_path / "out"
        s = _DynamicStep(spark, out.as_posix(), step_name="dyn", is_dynamic=False)
        _ = s.data()
        _ = s.data()
        assert s.compute_calls == 2

    def test_delete_output_registers_tmp_path(self, spark, tmp_path):
        out = tmp_path / "out"
        s = _DynamicStep(spark, out.as_posix(), step_name="dyn")
        s.output_hive["tmp_del"] = {
            "table_or_hdfs": (tmp_path / "del").as_posix(),
            "keep_or_delete": "delete"}
        _ = s.data_to_delete()
        assert (tmp_path / "del").as_posix() in s.tmp_paths

    def test_with_path_decorator(self, spark, tmp_path):
        target = tmp_path / "adhoc"

        class _S(Step):
            def __init__(self, **kw):
                super().__init__(sqlContext=spark, **kw)
                self.calls = 0

            @dynamic_parquet_with_path(target.as_posix())
            def data(self):
                self.calls += 1
                return self.sqlContext.createDataFrame([(9,)], ["n"])

        s = _S(step_name="adhoc_step")
        df = s.data()
        assert s.calls == 1
        assert target.exists()
        _ = s.data()
        assert s.calls == 1  # recarga
        assert df.count() == 1


# ---------------------------------------------------------------------------
# Helpers de colección / herencia
# ---------------------------------------------------------------------------
class TestCollectHelpers:
    def test_collect_step_output(self):
        s = _Counter(step_name="c")
        out = collect_step_output(s, "/some/path", "previous_path")
        assert out["c"]["previous_path"] == "/some/path"

    def test_run_substep_and_collect(self):
        class _Sub(Step):
            def step_action(self):
                self.output_parameters[self.step_name]["prev"] = "/sub/out"
                return self.output_parameters

        parent = _Counter(step_name="parent")
        sub = _Sub(step_name="sub")
        run_substep_and_collect(parent, sub, "upstream")
        assert parent.output_parameters["parent"]["upstream"] == {"prev": "/sub/out"}

    def test_inherit_parent_step_attributes(self):
        parent = _Counter(step_name="p",
                          input_parameters={"a": 1},
                          input_hive={"i": "/in"},
                          output_hive={"o": "/out"},
                          is_dynamic=False)
        child = _Counter(step_name="ch")
        inherit_parent_step_attributes(child, parent)
        assert child.input_parameters == {"a": 1}
        assert child.input_hive == {"i": "/in"}
        assert child.output_hive == {"o": "/out"}
        assert child.is_dynamic is False
        assert child.parent is parent
