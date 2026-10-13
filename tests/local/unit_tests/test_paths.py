"""
Tests de ``libs.paths.PathsConfig`` — el cargador del config interno de
paths (``config/paths.json``) que mapea etapas a rutas relativas.
"""
import json

import pytest

from libs.paths import PathsConfig


@pytest.fixture
def paths_config(tmp_path):
    cfg = {
        "stages": {
            "extract": {"path": "extract", "keep_or_delete": "keep"},
            "groupby": {"path": "groupby", "keep_or_delete": "keep"},
            "weekly_extract": {"path": "weekly_extract",
                               "keep_or_delete": "delete"},
        }
    }
    cfg_path = tmp_path / "paths.json"
    cfg_path.write_text(json.dumps(cfg))
    return PathsConfig(str(cfg_path))


class TestPathsConfig:
    def test_source_root_posix(self, paths_config):
        root = paths_config.source_root("/work", "202601", "src")
        assert root == "/work/202601/src"

    def test_stage_path(self, paths_config):
        p = paths_config.stage_path("/work", "202601", "src", "groupby")
        assert p == "/work/202601/src/groupby"

    def test_stage_config_shape(self, paths_config):
        cfg = paths_config.stage_config("/work", "202601", "src", "extract")
        assert cfg["table_or_hdfs"] == "/work/202601/src/extract"
        assert cfg["keep_or_delete"] == "keep"

    def test_stage_config_delete_flag(self, paths_config):
        cfg = paths_config.stage_config(
            "/work", "202601", "src", "weekly_extract")
        assert cfg["keep_or_delete"] == "delete"

    def test_missing_stage_raises_keyerror(self, paths_config):
        with pytest.raises(KeyError):
            paths_config.stage_path("/w", "y", "s", "no_such_stage")

    def test_default_repo_config_loads(self):
        """El paths.json del repo carga sin error y tiene las etapas base."""
        cfg = PathsConfig()  # config/paths.json del repo
        for stage in ("extract", "groupby", "pivoting", "fe"):
            assert stage in cfg.stages

    def test_env_override(self, tmp_path, monkeypatch):
        cfg_path = tmp_path / "custom_paths.json"
        cfg_path.write_text(json.dumps({"stages": {"x": {"path": "y"}}}))
        monkeypatch.setenv("LAIKA_PATHS_CONFIG", str(cfg_path))
        cfg = PathsConfig()
        assert "x" in cfg.stages
