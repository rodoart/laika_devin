"""
Fixtures y bootstrap compartido para la suite de pruebas unitarias.

Puntos clave:

- ``bootstrap`` del harness local aplica los shims necesarios
  (``PosixPath`` -> ``PurePosixPath`` en Windows, alias
  ``libs.aggregates`` -> ``libs.aggregattes``, ``hadoop.dll`` en PATH,
  ``PYSPARK_PYTHON`` = intérprete del env).
- ``LAIKA_FS_MODE=local`` fuerza a :class:`HivePath` a usar el backend
  de filesystem local (sin tocar HDFS).
- La sesión de Spark es **session-scoped**: una sola JVM para toda la
  suite (``local[2]``, sin UI, 4 shuffle partitions).

Run:
    C:/Users/Usuario/miniconda3/envs/laika_datapull_py310/python.exe \\
        -m pytest tests/local/unit_tests -v
"""
import os
import sys

import pytest

# --- bootstrap del harness local (shims + sys.path) -------------------------
TESTS_LOCAL = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if TESTS_LOCAL not in sys.path:
    sys.path.insert(0, TESTS_LOCAL)

import bootstrap  # noqa: E402

bootstrap.install_aggregates_alias()
bootstrap.install_numpy_shims()
bootstrap.install_typing_shims()

# Fuerza backend local en HivePath para todos los tests.
os.environ["LAIKA_FS_MODE"] = "local"


@pytest.fixture(scope="session")
def spark():
    """SparkSession compartida para toda la suite."""
    session = bootstrap.get_spark("laika_unit_tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def workdir(tmp_path_factory) -> str:
    """Directorio temporal raíz (POSIX) para outputs de los tests."""
    path = tmp_path_factory.mktemp("laika_ut")
    return path.as_posix()
