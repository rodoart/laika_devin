"""
Builder de :class:`~pyspark.sql.SparkSession` para el datapull: sesiones de
clúster (YARN) o locales, con reintentos de creación.

Portado desde ``framework/libs/data_engineering_toolbox/context/__init__.py``.

En modo clúster se leen las variables de entorno ``PYSPARK_*`` (cola,
puerto de la UI, directorios de staging/logs/warehouse, archivo de venv y
jars opcionales); el modo local se selecciona con ``master="local[*]"``
(o con la variable ``LAIKA_SPARK_MASTER``).
"""
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

#: Ruta de entrada para prefijar con ``hdfs://`` si hace falta.
PathLike = Union[str, Path]


class SparkSessionBuilder:
    """Construye la SparkSession del pipeline (YARN o local) con reintentos.

    Variables de entorno consumidas en modo YARN:

    ============================  ==========================================
    ``PYSPARK_QUEUE``             Cola YARN (sin el prefijo ``root.``)
    ``PYSPARK_PORT``              Puerto de la Spark UI del driver
    ``PYSPARK_STAGING_DIR_HDFS``  Directorio de staging de YARN
    ``PYSPARK_LOGS_DIR_HDFS``     Directorio de event-log
    ``PYSPARK_WAREHOUSE_DIR_HDFS`` Warehouse de Hive
    ``PYSPARK_CHECKPOINTS_DIR_HDFS``  Directorio de checkpoints (opcional)
    ``PYSPARK_VENV_TAR_GZ_HDFS``  Archivo venv empaquetado ``"#env"`` (opcional)
    ``PYSPARK_JARS_HDFS``         Jars extra separados por comas (opcional)
    ============================  ==========================================

    El nombre de la app sale de ``app_name`` o de la variable ``APP_NAME``.

    Parameters
    ----------
    app_name:
        Nombre de la aplicación Spark.
    extra_conf:
        Configs extra de Spark (``spark.*``/``hive.*``) aplicadas al final,
        por encima de las defaults.
    master:
        ``"yarn"`` para el clúster; ``"local[*]"`` (o ``LAIKA_SPARK_MASTER``)
        para ejecución local.
    venv_python_version:
        ``"<major>.<minor>"`` del venv empaquetado (para el ``PYTHONPATH``
        de los executors); por defecto la versión de Python actual.
    """

    #: Pausa entre reintentos de creación de la sesión (segundos).
    __RETRY_DELAY_SECONDS: float = 0.01
    #: Número máximo de intentos antes de rendirse.
    __MAX_RETRIES: int = 5

    def __init__(self, app_name: Optional[str] = None,
                 extra_conf: Optional[Dict[str, Any]] = None,
                 master: str = "yarn",
                 venv_python_version: Optional[str] = None) -> None:
        self.app_name: str = app_name or os.environ.get("APP_NAME", "spark-app")
        self.extra_conf: Dict[str, Any] = extra_conf or {}
        self.master: str = os.environ.get("LAIKA_SPARK_MASTER", master)
        self.venv_python_version: str = (venv_python_version
            or f"{sys.version_info.major}.{sys.version_info.minor}")

    @staticmethod
    def hdfs_uri(p: PathLike) -> str:
        """Prefija ``hdfs://`` para que Spark no caiga en ``file:/``."""
        # as_posix(): Path stringify con "\" en Windows; las URIs hdfs son
        # siempre posix independientemente del OS del driver.
        s = p.as_posix() if isinstance(p, Path) else str(p)
        return s if s.startswith("hdfs://") else f"hdfs://{s}"

    def build(self) -> SparkSession:
        """Construye (o reutiliza) la SparkSession según ``self.master``.

        Returns
        -------
        SparkSession
            Sesión configurada. En modo local solo aplica ``appName``,
            ``master`` y ``extra_conf``; en YARN aplica además toda la
            configuración de cola, recursos, staging/event-log/warehouse,
            archivo de venv y jars.
        """
        if self.master.startswith("local"):
            builder = (SparkSession.builder
                .appName(self.app_name)
                .config("spark.master", self.master))
            for key, value in self.extra_conf.items():
                builder = builder.config(key, value)
            return self._get_or_create_with_retries(builder)

        env_site_packages = f"./env/lib/python{self.venv_python_version}/site-packages"
        builder = (SparkSession.builder
            .appName(self.app_name)
            .config("spark.yarn.queue", f"root.{os.environ['PYSPARK_QUEUE']}")
            .config("spark.ui.port", str(os.environ['PYSPARK_PORT']))
            .config("spark.master", self.master)
            .config("spark.submit.deployMode", "client")
            .config("spark.driver.allowMultipleContexts", "True")
            .config("spark.dynamicAllocation.maxExecutors", "200")
            .config("spark.default.parallelism", "120")
            .config("spark.sql.shuffle.partitions", "120")
            .config("spark.executor.instances", "40")
            .config("spark.executor.cores", "5")
            .config("spark.executor.memory", "16g")
            .config("spark.executor.memoryOverhead", "2000m")
            .config("spark.driver.memory", "12g")
            .config("spark.network.timeout", "7200s")
            .config("spark.yarn.stagingDir", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", self.hdfs_uri(os.environ["PYSPARK_LOGS_DIR_HDFS"]))
            .config("spark.history.fs.logDirectory", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            .config("spark.sql.warehouse.dir", self.hdfs_uri(os.environ["PYSPARK_WAREHOUSE_DIR_HDFS"]))
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .enableHiveSupport()
        )
        # Venv empaquetado distribuido a los executors via YARN archives.
        venv_archive = os.environ.get("PYSPARK_VENV_TAR_GZ_HDFS")
        if venv_archive:
            builder = (builder
                .config("spark.yarn.dist.archives", self.hdfs_uri(venv_archive) + "#env")
                .config("spark.executorEnv.PYTHONPATH", env_site_packages)
                .config("spark.yarn.appMasterEnv.PYTHONPATH", env_site_packages))
        jars = os.environ.get("PYSPARK_JARS_HDFS")
        if jars:
            builder = builder.config("spark.jars", ",".join(
                self.hdfs_uri(j) for j in jars.split(",") if j.strip()))
        for key, value in self.extra_conf.items():
            builder = builder.config(key, value)

        spark = self._get_or_create_with_retries(builder)

        checkpoints_dir = os.environ.get("PYSPARK_CHECKPOINTS_DIR_HDFS")
        if checkpoints_dir:
            spark.sparkContext.setCheckpointDir(self.hdfs_uri(checkpoints_dir))
        return spark

    def _get_or_create_with_retries(self, builder: "SparkSession.Builder"
                                    ) -> SparkSession:
        """``builder.getOrCreate()`` con hasta ``__MAX_RETRIES`` intentos."""
        last_exception: Optional[Exception] = None
        for attempt in range(1, self.__MAX_RETRIES + 1):
            try:
                logger.info("Creating SparkSession (attempt %s/%s)",
                            attempt, self.__MAX_RETRIES)
                spark = builder.getOrCreate()
                logger.info("SparkSession created")
                return spark
            except Exception as e:
                last_exception = e
                logger.exception("SparkSession creation failed (attempt %s/%s)",
                                 attempt, self.__MAX_RETRIES)
                if attempt < self.__MAX_RETRIES:
                    time.sleep(self.__RETRY_DELAY_SECONDS)
                else:
                    logger.error("Max retries reached creating SparkSession")
                    raise
        raise last_exception  # type: ignore[misc]
