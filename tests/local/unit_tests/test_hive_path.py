"""
Tests de ``libs.hive_path`` — el objeto-path del datapull con backend
dual (HDFS / FS local). En este módulo ``LAIKA_FS_MODE`` lleva el modo de
plataforma detectado (``windows``/``unix``/``wsl``, ver conftest), así que
todo opera sobre ``tmp_path``.
"""
import pytest

from libs.hive_path import HivePath, is_hdfs_path, os_mode


# ---------------------------------------------------------------------------
# os_mode
# ---------------------------------------------------------------------------
class TestOsMode:
    def test_env_override(self, monkeypatch):
        for mode in ("unix", "wsl", "windows"):
            monkeypatch.setenv("APP_OS_MODE", mode)
            assert os_mode() == mode

    def test_default_is_valid(self, monkeypatch):
        monkeypatch.delenv("APP_OS_MODE", raising=False)
        monkeypatch.delenv("APP_MODE", raising=False)
        monkeypatch.delenv("LAIKA_OS_MODE", raising=False)
        assert os_mode() in ("unix", "wsl", "windows")


# ---------------------------------------------------------------------------
# is_hdfs_path
# ---------------------------------------------------------------------------
class TestIsHdfsPath:
    def test_hdfs_scheme_is_hdfs(self):
        assert is_hdfs_path("hdfs:///user/x/file") is True

    def test_windows_path_is_local(self):
        assert is_hdfs_path("C:/foo/bar") is False
        assert is_hdfs_path("C:\\foo\\bar") is False

    def test_file_scheme_is_local(self):
        assert is_hdfs_path("file:/data/x") is False
        assert is_hdfs_path("file:///data/x") is False

    @pytest.mark.parametrize("mode", ["local", "windows", "unix", "wsl"])
    def test_local_modes_env_force_local(self, monkeypatch, mode):
        monkeypatch.setenv("LAIKA_FS_MODE", mode)
        monkeypatch.delenv("APP_MODE", raising=False)
        assert is_hdfs_path("/some/posix/path") is False

    @pytest.mark.parametrize("mode", ["hdfs", "cluster"])
    def test_hdfs_modes_env_force_hdfs(self, monkeypatch, mode):
        monkeypatch.setenv("LAIKA_FS_MODE", mode)
        monkeypatch.delenv("APP_MODE", raising=False)
        assert is_hdfs_path("/some/posix/path") is True


# ---------------------------------------------------------------------------
# Operaciones básicas de ficheros / directorios
# ---------------------------------------------------------------------------
class TestBasicOps:
    def test_exists_missing(self, tmp_path):
        p = HivePath(tmp_path.as_posix() + "/nope")
        assert p.exists() is False

    def test_mkdir_and_is_dir(self, tmp_path):
        d = HivePath(tmp_path.as_posix() + "/subdir")
        d.mkdir()
        assert d.is_dir() is True
        assert d.exists() is True

    def test_mkdir_parents(self, tmp_path):
        d = HivePath(tmp_path.as_posix() + "/a/b/c")
        d.mkdir(parents=True)
        assert d.is_dir() is True

    def test_mkdir_no_parents_fails(self, tmp_path):
        d = HivePath(tmp_path.as_posix() + "/x/y")
        with pytest.raises(FileNotFoundError):
            d.mkdir()

    def test_mkdir_exist_ok(self, tmp_path):
        d = HivePath(tmp_path.as_posix())
        d.mkdir(exist_ok=True)  # ya existe -> no lanza con exist_ok
        with pytest.raises(FileExistsError):
            d.mkdir()

    def test_touch_creates_file(self, tmp_path):
        f = HivePath(tmp_path.as_posix() + "/file.txt")
        f.touch()
        assert f.is_file() is True
        assert f.is_dir() is False

    def test_touch_exist_ok_false(self, tmp_path):
        f = HivePath(tmp_path.as_posix() + "/file.txt")
        f.touch()
        with pytest.raises(FileExistsError):
            f.touch(exist_ok=False)

    def test_iterdir(self, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "b.txt").write_text("y")
        sub = tmp_path / "sub"
        sub.mkdir()
        names = sorted(h.name for h in HivePath(tmp_path.as_posix()).iterdir())
        assert names == ["a.txt", "b.txt", "sub"]

    def test_iterdir_empty_and_missing(self, tmp_path):
        assert list(HivePath(tmp_path.as_posix()).iterdir()) == []
        # directorio inexistente -> backend local devuelve lista vacía
        assert list(HivePath(tmp_path.as_posix() + "/nope").iterdir()) == []

    def test_listdirs_and_listfiles(self, tmp_path):
        (tmp_path / "f1.txt").write_text("x")
        (tmp_path / "f2.txt").write_text("y")
        (tmp_path / "d1").mkdir()
        hive = HivePath(tmp_path.as_posix())
        assert sorted(h.name for h in hive.listfiles()) == ["f1.txt", "f2.txt"]
        assert [h.name for h in hive.listdirs()] == ["d1"]

    def test_rm_file(self, tmp_path):
        f = HivePath(tmp_path.as_posix() + "/f.txt")
        f.touch()
        f.rm()
        assert f.exists() is False

    def test_rm_on_dir_raises(self, tmp_path):
        with pytest.raises(TypeError):
            HivePath(tmp_path.as_posix()).rm()

    def test_rmdir_recursive(self, tmp_path):
        d = tmp_path / "tree"
        (d / "inner").mkdir(parents=True)
        (d / "inner" / "f.txt").write_text("x")
        HivePath(d.as_posix()).rmdir(recursive=True)
        assert d.exists() is False

    def test_rmdir_on_file_raises(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("x")
        with pytest.raises(TypeError):
            HivePath(f.as_posix()).rmdir(recursive=True)

    def test_mv_directory(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        (src / "f.txt").write_text("x")
        dest = tmp_path / "dest"
        HivePath(src.as_posix()).mv(dest.as_posix())
        assert (dest / "src" / "f.txt").exists()
        assert not src.exists()

    def test_rename(self, tmp_path):
        f = tmp_path / "old.txt"
        f.write_text("x")
        new = HivePath(f.as_posix()).rename("new.txt")
        assert (tmp_path / "new.txt").exists()
        assert not f.exists()
        assert new.name == "new.txt"


# ---------------------------------------------------------------------------
# _SUCCESS / parquets
# ---------------------------------------------------------------------------
class TestParquetHelpers:
    def _make_parquet_dir(self, root, name, success=True):
        d = root / name
        d.mkdir(parents=True)
        if success:
            (d / "_SUCCESS").write_text("")
        (d / "part-00000.parquet").write_bytes(b"PAR1")
        return d

    def test_is_successful_true(self, tmp_path):
        self._make_parquet_dir(tmp_path, "out")
        assert HivePath((tmp_path / "out").as_posix()).is_successful()

    def test_is_successful_false_without_success(self, tmp_path):
        self._make_parquet_dir(tmp_path, "out", success=False)
        assert HivePath((tmp_path / "out").as_posix()).is_successful() is False

    def test_is_successful_missing_dir(self, tmp_path):
        assert HivePath((tmp_path / "nope").as_posix()).is_successful() is False

    def test_is_parquet_alias(self, tmp_path):
        self._make_parquet_dir(tmp_path, "out")
        assert HivePath((tmp_path / "out").as_posix()).is_parquet()

    def test_listparquets(self, tmp_path):
        self._make_parquet_dir(tmp_path, "p1")
        self._make_parquet_dir(tmp_path, "nested/p2")
        (tmp_path / "plain").mkdir()
        found = sorted(
            h.name for h in HivePath(tmp_path.as_posix()).listparquets())
        assert found == ["p1", "p2"]

    def test_glob(self, tmp_path):
        self._make_parquet_dir(tmp_path, "p1")
        self._make_parquet_dir(tmp_path, "p2")
        found = sorted(
            h.name for h in HivePath(tmp_path.as_posix()).glob("part-*"))
        assert found == ["part-00000.parquet", "part-00000.parquet"]

    def test_glob_empty_pattern_raises(self, tmp_path):
        with pytest.raises(ValueError):
            list(HivePath(tmp_path.as_posix()).glob(""))

    def test_sorted_by_time(self, tmp_path):
        import time
        a = tmp_path / "a.txt"
        a.write_text("1")
        time.sleep(0.02)
        b = tmp_path / "b.txt"
        b.write_text("2")
        names = [h.name for h in
                 HivePath(tmp_path.as_posix()).iterdir(sorted_by_time=True)]
        assert names.index("a.txt") < names.index("b.txt")


# ---------------------------------------------------------------------------
# Wrappers de libs.steps (compatibilidad)
# ---------------------------------------------------------------------------
class TestStepsWrappers:
    def test_path_exists(self, tmp_path):
        from libs.steps import path_exists
        assert path_exists(tmp_path.as_posix()) is True
        assert path_exists(tmp_path.as_posix() + "/nope") is False

    def test_path_listdir(self, tmp_path):
        from libs.steps import path_listdir
        (tmp_path / "x").mkdir()
        assert path_listdir(tmp_path.as_posix()) == ["x"]
        assert path_listdir(tmp_path.as_posix() + "/nope") == []

    def test_path_delete_dir(self, tmp_path):
        from libs.steps import path_delete
        d = tmp_path / "d"
        (d / "i").mkdir(parents=True)
        path_delete(d.as_posix())
        assert d.exists() is False

    def test_path_delete_file(self, tmp_path):
        from libs.steps import path_delete
        f = tmp_path / "f.txt"
        f.write_text("x")
        path_delete(f.as_posix())
        assert f.exists() is False
