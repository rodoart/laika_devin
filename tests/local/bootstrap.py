"""
Bootstrap for local testing of the datapull code.

Creates a local SparkSession matching the production Spark version
(Spark 3.3.2, see run_fs.sh). The code uses `X | Y` / `type[X]`
annotations, so it requires Python >= 3.10 (run_fs.sh's citiconda39
cannot literally be a 3.9 interpreter); we test on Python 3.10.

Parameters (environment variables):
    LAIKA_TEST_WORKDIR : root dir for test inputs/outputs
                         (default: <repo>/tests/local/workdir)
    HADOOP_HOME        : dir containing bin/winutils.exe (Windows only,
                         default: <repo>/tests/local/hadoop)
"""
import os
import sys
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
# SOLO en Windows: en unix/wsl PosixPath es la clase real (con .exists(),
# .resolve(), ...) y sustituirla por PurePosixPath rompe pytest/pathlib.
if os.name == "nt" or os.environ.get("APP_OS_MODE") == "windows":
    import pathlib
    pathlib.PosixPath = pathlib.PurePosixPath

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if os.name == "nt" or os.environ.get("APP_OS_MODE") == "windows":
    os.environ.setdefault("HADOOP_HOME", str(DEFAULT_HADOOP_HOME))
    # hadoop.dll must be visible to the JVM (native IO for local FS writes)
    _hadoop_bin = Path(os.environ["HADOOP_HOME"]) / "bin"
    os.environ["PATH"] = str(_hadoop_bin) + os.pathsep + os.environ.get("PATH", "")

# Spark workers must use this same interpreter (avoids the WindowsApps stub)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


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
    if os.environ.get("HADOOP_HOME"):
        _hadoop_bin = (Path(os.environ["HADOOP_HOME"]) / "bin").as_posix()
        builder = builder.config(
            "spark.driver.extraJavaOptions", f'-Djava.library.path="{_hadoop_bin}"')
    return builder.getOrCreate()
