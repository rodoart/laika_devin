"""
Framework :class:`Step` para la ETL datapull: ejecución encadenada,
propiedades cacheadas por instancia y decoradores ``dynamic_*`` de
escritura/recarga perezosa a disco.

Portado desde ``framework/libs/framework/__init__.py``, recortado a lo que
necesita el datapull: salidas parquet planas (sin tablas Hive
particionadas). Todo el acceso al filesystem (verificación/borrado de
parquets) pasa por :class:`libs.hive_path.HivePath`, que funciona tanto
sobre HDFS como sobre un FS local.

Las salidas de las etapas se declaran en ``output_hive[stage]
["table_or_hdfs"]``; las entradas marcadas ``"keep_or_delete": "delete"``
se registran como parquets intermedios y se borran con
:meth:`Step.delete_tmp_paths`.

:meth:`Step.execute` loguea ``Executing step: <name>`` /
``Step finished: <name>``, formato que ``supervisor.py`` parsea para
seguir el progreso y reiniciar el proceso (muerto o zombi) — en la
reanudación, los parquets ya escritos se recargan si ``is_dynamic`` es
``True``.
"""
import logging
import re
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

try:
    from inflection import underscore
except ImportError:
    def underscore(name: str) -> str:
        """CamelCase -> camel_case (sustituto de ``inflection.underscore``).

        Mismas reglas que inflection: acrónimos seguidos de palabra se
        separan, y solo se inserta '_' entre minúscula/dígito y mayúscula
        (un '_' o mayúscula inicial no genera doble '_').
        """
        s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

from pyspark.sql import DataFrame, SparkSession

from libs.hive_path import HivePath, is_hdfs_path

logger = logging.getLogger(__name__)

# Ruta aceptada por los helpers (string, pathlib o HivePath).
PathLike = Union[str, Path, HivePath]
# Declaración de una salida/entrada en `input_hive`/`output_hive`: o una
# ruta directa, o el dict de config con `table_or_hdfs`/`keep_or_delete`.
HiveEntry = Union[str, Path, Dict[str, Any]]


###############################################################################
# Helpers de paths (wrappers finos sobre HivePath, mantenidos para callers)
###############################################################################

def path_exists(path: PathLike) -> bool:
    """``True`` si ``path`` existe (HDFS o FS local, según la ruta)."""
    return HivePath(path).exists()


def path_listdir(path: PathLike) -> List[str]:
    """Nombres de las entradas de ``path`` (lista vacía si no es un dir)."""
    hive = HivePath(path)
    if not hive.is_dir():
        return []
    return [h.name for h in hive.iterdir()]


def path_delete(path: PathLike, skip_trash: bool = True) -> None:
    """Borra ``path`` recursivamente (dir) o como fichero.

    Parameters
    ----------
    path:
        Ruta a eliminar.
    skip_trash:
        En HDFS añade ``-skipTrash``; irrelevante en FS local.
    """
    hive = HivePath(path)
    if hive.is_dir():
        hive.rmdir(recursive=True, skip_trash=skip_trash)
    elif hive.is_file():
        hive.rm()


###############################################################################
# cached_property
###############################################################################

class cached_property:
    """Propiedad cacheada a nivel de instancia.

    Calcula ``func(instance)`` la primera vez que se accede y guarda el
    resultado en ``instance.__dict__[_<nombre>_cache]``, de modo que los
    accesos posteriores devuelven el valor memoizado. Usada por el
    framework (y por los agregadores, p. ej. ``PayrollIncomeAggregate``)
    para materializar DataFrames intermedios una sola vez.
    """

    def __init__(self, func: Callable[[Any], Any]) -> None:
        self.func: Callable[[Any], Any] = func
        self.cache_name: str = f"_{func.__name__}_cache"

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        if self.cache_name not in instance.__dict__:
            instance.__dict__[self.cache_name] = self.func(instance)
        return instance.__dict__[self.cache_name]


###############################################################################
# Step
###############################################################################

class Step:
    """Unidad de ETL: ejecuta :meth:`step_action` tras resolver
    ``previous_step`` y acumula salidas en ``output_parameters``.

    Parameters (todos opcionales, vía ``kwargs``)
    ------------------------------------------
    previous_step : Step | List[Step]
        Step(s) que deben ejecutarse antes que este; sus
        ``output_parameters`` se mezclan dentro de
        ``self.input_parameters``.
    input_linux / output_linux : Dict[str, PathLike]
        Rutas en el FS local (no usadas por el datapull, mantenidas por
        paridad con el framework).
    input_hive / output_hive : Dict[str, HiveEntry]
        Entradas y salidas en HDFS. Una salida puede ser una ruta directa
        o ``{"table_or_hdfs": path, "keep_or_delete": "keep|delete"}``;
        las marcadas ``delete`` se acumulan en ``tmp_paths``.
    input_parameters : Dict[str, Any]
        Parámetros de entrada, mezclados con las salidas del step
        anterior antes de :meth:`step_action`.
    step_name : str
        Nombre mostrado en los logs y usado por ``supervisor.py``.
    sqlContext : SparkSession
        Sesión Spark usada por los decoradores ``dynamic_*``.
    is_dynamic : bool
        ``True`` (default): los outputs persistidos se recargan de disco
        en lugar de recomputarse.

    Attributes
    ----------
    tmp_paths : List[str]
        Paths intermedios pendientes de borrar.
    output_parameters : Dict[str, Dict[str, Any]]
        ``{step_name: {key: valor}}`` con las salidas que consume el
        step siguiente.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args: Tuple[Any, ...] = args
        self.previous_step: Union["Step", List["Step"], None] = \
            kwargs.get("previous_step", None)
        self.input_linux: Dict[str, PathLike] = kwargs.get("input_linux", {})
        self.input_hive: Dict[str, HiveEntry] = kwargs.get("input_hive", {})
        self.output_linux: Dict[str, PathLike] = kwargs.get("output_linux", {})
        self.output_hive: Dict[str, HiveEntry] = kwargs.get("output_hive", {})
        self.input_parameters: Dict[str, Any] = kwargs.get("input_parameters", {})
        self.step_name: str = kwargs.get("step_name", self._make_step_name())
        self.output_parameters: Dict[str, Dict[str, Any]] = {self.step_name: {}}
        self.sqlContext: Optional[SparkSession] = kwargs.get(
            "sqlContext", getattr(self, "sqlContext", None))
        self.is_dynamic: bool = kwargs.get("is_dynamic", True)
        self.tmp_paths: List[str] = []
        self._decorated_cache: Dict[str, DataFrame] = {}

    def _make_step_name(self) -> str:
        """Nombre único ``<clase>_<n>`` a partir de un contador global por clase."""
        global_var_name = f"{underscore(self.__class__.__name__)}_counter"
        globals()[global_var_name] = globals().get(global_var_name, 0) + 1
        return f"{underscore(self.__class__.__name__)}_{globals()[global_var_name]}"

    def __str__(self) -> str:
        return f"Step({self.step_name})"

    def __repr__(self) -> str:
        return f"Step({self.step_name})"

    @cached_property
    def _previous_step_output_parameters_loaded(self) -> Dict[str, Any]:
        """Ejecuta ``previous_step`` (single o lista) y mezcla sus salidas.

        Returns
        -------
        Dict[str, Any]
            ``output_parameters`` de los steps previos (``{}`` si no hay).
        """
        if self.previous_step:
            if isinstance(self.previous_step, list):
                output_params: Dict[str, Any] = {}
                for step in self.previous_step:
                    step.execute()
                    output_params.update(step.output_parameters)
                return output_params
            else:
                self.previous_step.execute()
            return self.previous_step.output_parameters
        return {}

    def step_action(self) -> Any:
        """Lógica del step; las subclases deben implementarla."""
        raise NotImplementedError("Subclasses should implement this method.")

    def _iter_previous_steps(self) -> List["Step"]:
        """``previous_step`` normalizado a lista."""
        if isinstance(self.previous_step, list):
            return list(self.previous_step)
        return [self.previous_step] if self.previous_step else []

    def collect_tmp_paths(self, _visited: Optional[Set[int]] = None) -> List[str]:
        """Recoge recursivamente los paths intermedios borrables.

        Incluye ``tmp_paths`` propios, salidas marcadas
        ``keep_or_delete="delete"``, substeps anidados (buscados en
        ``self.__dict__``) y toda la cadena ``previous_step``
        (deduplicado y seguro ante ciclos).

        Parameters
        ----------
        _visited:
            Conjunto interno de ``id()`` ya visitados; no pasarlo desde
            fuera.

        Returns
        -------
        List[str]
            Paths únicos, en orden de descubrimiento.
        """
        if _visited is None:
            _visited = set()
        if id(self) in _visited:
            return []
        _visited.add(id(self))

        paths = [str(p) for p in self.tmp_paths]
        paths += [
            str(cfg["table_or_hdfs"]) for cfg in self.output_hive.values()
            if isinstance(cfg, dict)
            and cfg.get("keep_or_delete") == "delete"
        ]
        for value in self.__dict__.values():
            nested: List["Step"] = []
            if isinstance(value, Step):
                nested = [value]
            elif isinstance(value, (list, tuple)):
                nested = [v for v in value if isinstance(v, Step)]
            for step in nested:
                paths += step.collect_tmp_paths(_visited)
        for prev in self._iter_previous_steps():
            paths += prev.collect_tmp_paths(_visited)
        return list(dict.fromkeys(paths))

    def delete_tmp_paths(self, skip_missing: bool = True) -> List[str]:
        """Borra todos los parquets intermedios de la cadena.

        Parameters
        ----------
        skip_missing:
            ``True`` (default): una ruta inexistente solo loguea warning;
            ``False`` relanza la excepción.

        Returns
        -------
        List[str]
            Paths efectivamente borrados.
        """
        deleted: List[str] = []
        for path_str in self.collect_tmp_paths():
            try:
                HivePath(path_str).rmdir(recursive=True, skip_trash=True)
                deleted.append(path_str)
                logger.info("Deleted intermediate parquet: %s", path_str)
            except Exception as e:
                if not skip_missing:
                    raise
                logger.warning("Could not delete %s: %s", path_str, e)
        return deleted

    def execute(self) -> Any:
        """Resuelve ``previous_step``, mezcla ``input_parameters`` y ejecuta
        :meth:`step_action`.

        Emite los logs ``Executing step:``/``Step finished:`` que parsea
        ``supervisor.py`` y mide la duración del step.
        """
        logger.info("Executing step: %s", self.step_name)
        logger.debug("Input parameters: %s", self.input_parameters)
        _start = time.time()
        if self.previous_step:
            self.input_parameters.update(self._previous_step_output_parameters_loaded)
        result = self.step_action()
        logger.info("Step finished: %s (%.1fs)", self.step_name, time.time() - _start)
        return result


###############################################################################
# Escritura de parquets particionados por vintage / tfrom
###############################################################################

def partition_columns(columns: Iterable[str]) -> List[str]:
    """Columnas de particionado temporales presentes en ``columns``.

    Regla: una columna de vintage (``vintage_date`` en los frames con
    pivot unido; ``vintage`` en el pivot preparado y en las salidas
    finales que renombran la columna) y, si el df es long, ``tfrom``.
    Los frames sin columnas temporales (``*_features`` de fe/mobs, que
    solo llevan ``id_msf`` + features) quedan sin particionar.

    ``tfrom_date`` no entra en la regla automática: solo interesa como
    partición en ``<table>_full`` del extract (poda el filtro
    ``tfrom_date.between`` de ``_create_cohorts``) y se pasa explícito
    vía ``save_parquet(..., partition_cols=...)``.
    """
    cols = list(columns)
    parts = [c for c in ("vintage_date", "vintage") if c in cols][:1]
    if "tfrom" in cols:
        parts.append("tfrom")
    return parts


def save_parquet(df: DataFrame, path: PathLike, mode: str = "overwrite",
                 partition_cols: Optional[Iterable[str]] = None) -> None:
    """Escribe ``df`` como parquet en ``path``, particionando por las
    columnas temporales que tenga.

    Por defecto particiona por ``vintage_date``/``vintage`` y ``tfrom``
    según :func:`partition_columns`; con ``partition_cols`` se fuerza una
    lista concreta (p.ej. ``["tfrom_date"]`` en ``<table>_full``) o se
    desactiva con ``[]``. Las columnas de partición siguen apareciendo al
    releer el parquet (Spark las infiere por nombre de directorio).
    """
    parts = list(partition_cols) if partition_cols is not None \
        else partition_columns(df.columns)
    if parts and not df.head(1):
        # partitionBy sobre un df vacío no genera ni part-files ni
        # _common_metadata -> el parquet queda ILEGIBLE ("Unable to
        # infer schema"). Sin particionar sí quedan part-files vacíos
        # con schema y la recarga funciona.
        parts = []
    # overwrite debe ser TOTAL: en el cluster la sesión nace con
    # partitionOverwriteMode=dynamic (session.py) y sin este pin un
    # write "overwrite" solo reemplaza las particiones presentes en el
    # df — sobreviven particiones ajenas (p.ej. el temp_0 compartido
    # del mt mezclaba asigna+nomina). El único writer dinámico real es
    # extract._write_month_partitions, que maneja el conf él mismo.
    conf = "spark.sql.sources.partitionOverwriteMode"
    spark = df.sparkSession
    prev = spark.conf.get(conf, "static")
    spark.conf.set(conf, "static")
    try:
        writer = df.write.mode(mode)
        if parts:
            writer = writer.partitionBy(*parts)
        writer.parquet(str(path))
    finally:
        spark.conf.set(conf, prev)


def read_parquet_many(sqlContext: SparkSession,
                      paths: Iterable[PathLike]) -> DataFrame:
    """Lee varias raíces parquet y las une por nombre de columna.

    ``read.parquet(*paths)`` falla cuando cada raíz está particionada
    ("Conflicting directory structures detected"): Spark exige la misma
    estructura relativa bajo todas las rutas. Leyendo cada raíz por
    separado cada una descubre sus particiones, y ``unionByName`` une
    por nombre (las columnas de partición se reordenan al final del
    esquema al releer, así que un ``union`` posicional sería frágil).
    """
    dfs = [sqlContext.read.parquet(str(p)) for p in paths]
    if not dfs:
        raise ValueError("read_parquet_many: lista de paths vacía")
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df, allowMissingColumns=True)
    return out


###############################################################################
# Decoradores dinámicos: escribir a disco / recargar de disco
###############################################################################

def _reload_or_recompute_parquet(self, func: Callable,
                                 path: PathLike, label: str,
                                 save: Optional[Callable[[DataFrame], None]] = None
                                 ) -> DataFrame:
    """Lee ``path`` como parquet si ``is_dynamic``; si falla, ejecuta ``func``
    y lo guarda.

    Implementación común de los decoradores ``dynamic*`` (paridad con el
    ``_reload_or_recompute_parquet`` del framework).

    ``save`` personaliza la escritura (default:
    ``df.write.mode("overwrite").parquet(path)``); las clases agregado lo
    usan para respetar ``self.mode`` vía ``self._save_parquet``.
    """
    try:
        if not self.is_dynamic:
            raise Exception
        result = self.sqlContext.read.parquet(str(path))
        logger.info("%s data reloaded from %s", label.capitalize(), path)
    except Exception as e:
        logger.warning("Could not load %s from %s due to %s. Recomputing...",
                       label, path, e)
        computed = func(self)
        if save is not None:
            save(computed)
        else:
            save_parquet(computed, str(path))
        result = self.sqlContext.read.parquet(str(path))
        logger.info("%s data saved on %s", label.capitalize(), path)
    return result


def dynamic(path_key: str) -> Callable:
    """Decorador recarga-o-recomputo para intermedios de clases *agregado*.

    Adaptación del ``dynamic`` del framework al protocolo de los
    agregadores del datapull (``Payroll*Aggregate``, ``Weeks*``,
    ``FeatureEngineering``, ``PreviousFixer*``), que no son ``Step``:

    * la ruta viene de ``self.hdfs[path_key]`` (dict plano de paths, no el
      config ``output_hive`` de los steps);
    * la escritura usa ``self._save_parquet(df, path)`` si la clase lo
      define (respeta ``self.mode``); si no, ``mode("overwrite")``;
    * requiere ``self.is_dynamic`` y ``self.sqlContext``.

    Parameters
    ----------
    path_key:
        Clave de ``self.hdfs`` con la ruta del parquet intermedio.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> DataFrame:
            path = self.hdfs[path_key]
            save_parquet = getattr(self, "_save_parquet", None)
            return _reload_or_recompute_parquet(
                self, lambda s: func(s, *args, **kwargs), path, path_key,
                save=(lambda df: save_parquet(df, path))
                if save_parquet else None)
        return wrapper
    return decorator


def dynamic_parquet_with_path(path: PathLike) -> Callable:
    """Decorador recarga-o-recomputo para un parquet sin particionar con
    ``path`` explícito.

    Usado para intermedios ad-hoc dentro de un step (la ruta no vive en
    ``output_hive``).
    """
    def decorator(func: Callable[[Step], DataFrame]) -> Callable[[Step], DataFrame]:
        @wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> DataFrame:
            path_key = str(path).split("/")[-1]
            return _reload_or_recompute_parquet(
                self, lambda s: func(s, *args, **kwargs), path, path_key)
        return wrapper
    return decorator


def dynamic_parquet(path_key: str) -> Callable:
    """Decorador recarga-o-recomputo ligado a ``self.output_hive[path_key]``.

    Lee ``table_or_hdfs`` del config ``output_hive`` del step; cuando el
    config marca ``keep_or_delete == "delete"``, la ruta se registra en
    ``self.tmp_paths`` para su borrado posterior.

    Parameters
    ----------
    path_key:
        Clave de ``output_hive`` que contiene la ruta de destino.
    """
    def decorator(func: Callable[[Step], DataFrame]) -> Callable[[Step], DataFrame]:
        @wraps(func)
        def wrapper(self, *args: Any, **kwargs: Any) -> DataFrame:
            output_hive = self.output_hive
            cfg = output_hive[path_key]
            path = cfg["table_or_hdfs"] if isinstance(cfg, dict) else cfg
            if isinstance(cfg, dict) and cfg.get("keep_or_delete") == "delete":
                self.tmp_paths.append(path)
            return _reload_or_recompute_parquet(
                self, lambda s: func(s, *args, **kwargs), path, path_key)
        return wrapper
    return decorator


###############################################################################
# Helpers de recolección de salidas
###############################################################################

def collect_step_output(step: Step, output: Any, output_key: str) -> Dict[str, Any]:
    """Guarda una salida materializada en los ``output_parameters`` del step.

    Returns
    -------
    Dict[str, Any]
        Los ``output_parameters`` del step tras la escritura.
    """
    step.output_parameters[step.step_name][output_key] = output
    return step.output_parameters


def run_substep_and_collect(parent: Step, substep: Step,
                            output_key: str) -> Dict[str, Any]:
    """Ejecuta un substep terminal y eleva su salida a los parámetros del padre."""
    substep.step_action()
    parent.output_parameters[parent.step_name][output_key] = (
        substep.output_parameters[substep.step_name]
    )
    return parent.output_parameters


def inherit_parent_step_attributes(substep: Step, parent: Step) -> None:
    """Copia (por referencia) los atributos compartidos del padre al substep.

    El substep hereda ``input_parameters``, ``input_hive``, ``output_hive``,
    ``sqlContext``, ``is_dynamic`` y un puntero ``parent``.
    """
    substep.input_parameters = parent.input_parameters
    substep.input_hive = parent.input_hive
    substep.output_hive = parent.output_hive
    substep.sqlContext = parent.sqlContext
    substep.is_dynamic = parent.is_dynamic
    substep.parent = parent
