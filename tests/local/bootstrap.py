"""
Bootstrap for local testing of the datapull code.

Creates a local SparkSession matching the production Spark version
(Spark 3.3.2, see run_fs.sh). The code uses `X | Y` / `type[X]`
annotations, so it requires Python >= 3.10 (run_fs.sh's citiconda39
cannot literally be a 3.9 interpreter); we test on Python 3.10.
It also fixes the `libs.aggregates` -> `libs.aggregattes` package
alias used by payroll_streaks.py.

Parameters (environment variables):
    LAIKA_TEST_WORKDIR : root dir for test inputs/outputs
                         (default: <repo>/tests/local/workdir)
    HADOOP_HOME        : dir containing bin/winutils.exe (Windows only,
                         default: <repo>/tests/local/hadoop)
"""
import os
import sys
import types
import importlib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WORKDIR = PROJECT_ROOT / "tests" / "local" / "workdir"
DEFAULT_HADOOP_HOME = PROJECT_ROOT / "tests" / "local" / "hadoop"

WORKDIR = Path(os.environ.get("LAIKA_TEST_WORKDIR", str(DEFAULT_WORKDIR)))
# Posix-flavoured view: production code does PosixPath(...).parent/.joinpath
# and feeds the result to spark.read.parquet -> keep forward slashes.
POSIX_WORKDIR = WORKDIR.as_posix()

# PosixPath() cannot be instantiated on Windows; PurePosixPath has the same
# path API and is what the code actually needs (string building only).
import pathlib
pathlib.PosixPath = pathlib.PurePosixPath

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", str(DEFAULT_HADOOP_HOME))
    # hadoop.dll must be visible to the JVM (native IO for local FS writes)
    _hadoop_bin = Path(os.environ["HADOOP_HOME"]) / "bin"
    os.environ["PATH"] = str(_hadoop_bin) + os.pathsep + os.environ.get("PATH", "")

# Spark workers must use this same interpreter (avoids the WindowsApps stub)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def install_numpy_shims() -> None:
    """missing_treatment.py does `from numpy import source`; np.source was a
    helper removed in numpy>=1.24. Shim it so the module imports."""
    import numpy as np
    if not hasattr(np, "source"):
        np.source = lambda *a, **k: None


def install_typing_shims() -> None:
    """feature_engineering.py annotates with `LiteralString` (typing only has it
    on Python>=3.11). Backport it from typing_extensions."""
    import typing
    if not hasattr(typing, "LiteralString"):
        from typing_extensions import LiteralString
        typing.LiteralString = LiteralString


def install_aggregates_alias() -> None:
    """payroll_streaks.py imports `from ..aggregates.payroll_income` but the real
    package is `aggregattes`. Register an alias module so both names resolve."""
    pi = importlib.import_module("libs.aggregattes.payroll_income")
    pkg = types.ModuleType("libs.aggregates")
    pkg.__path__ = []  # mark as package
    pkg.payroll_income = pi
    sys.modules["libs.aggregates"] = pkg
    sys.modules["libs.aggregates.payroll_income"] = pi


def get_spark(app_name: str = "laika_local_test"):
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
    )
    if os.name == "nt":
        _hadoop_bin = (Path(os.environ["HADOOP_HOME"]) / "bin").as_posix()
        builder = builder.config(
            "spark.driver.extraJavaOptions", f'-Djava.library.path="{_hadoop_bin}"')
    return builder.getOrCreate()
