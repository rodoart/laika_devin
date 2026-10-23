"""
Project: Adobe Clickstream Aggregation
Developer: Devin
Update Date: 2026
Version: 0.1

Agregador ``groupby`` de las fuentes Adobe (``adobe_bancanet`` /
``adobe_mobile``).

Lee el ``<table>_historic`` re-etiquetado por ``weeks`` (tfrom semanal,
``tfrom_date`` = lunes de la semana, ``file_date`` = día real del hit) y
produce el long frame del groupby mezclando **cuatro dominios de tfrom**
en la misma tabla — ``GeneratePivot`` emite una columna por par
``(variable, tfrom)``, así que cada dominio produce su propia familia de
features:

| Dominio      | tfrom          | Variables (prefijo bnt_/mob_)                        | Features resultantes |
|--------------|----------------|----------------------------------------------------|-----------------------|
| weekly       | semana 0..n*4  | ``<p>_<flag>`` (0/1), ``<p>_<flag>_cnt``, ``<p>_<tvar>_<stat>``, ``<p>_hits`` | serie semanal -> transforms ``*_mt`` / ``*_onz_mt`` (rachas semanales) |
| fortnightly  | quincena 0..   | mismas vars con sufijo ``_q``                        | rollup de 2 semanas   |
| last_days    | dias 1..14,15..| ``<p>_<var>_ld``                                     | ``var_ld_<w>`` = actividad en los ultimos ``w`` dias |
| daily_seq    | dia 1..seq_days| ``<p>_<flag>_d``                                     | vector binario ultimos N dias -> rachas diarias via onz |

Los flags de producto y los tiempos de sesión (``T_inicio``, ``T_vista``,
``T_session``) se calculan aquí — post join con pivot, como en el pipeline
original de ``adobe_source`` — y sus reglas viven en
``configuration.json -> <source>.group_by`` (nada hardcodeado):

- ``flags``: ``{nombre_flag: [ {col, op, value}, ... ]}`` — AND de
  condiciones. ``op``: ``eq``, ``contains``, ``startswith``, ``endswith``,
  ``contains_any``, ``endswith_any``, ``startswith_any``.
- ``time_vars`` / ``time_stats``: variables y agregaciones de tiempo.
- ``domains.{weekly,fortnightly,last_days,daily_seq}``: activación y
  parámetros de cada dominio.
- ``window_end``: ``"last_day"`` (default, consistente con el ancla de
  ``weeks``) o ``"vintage"`` para el cálculo de ``last_days``.

Bug del original corregido: ``T_vista`` quedaba ``null`` en el primer hit
y 0 en el resto (``when(isNull, T_vista).otherwise(0)``); aquí
``coalesce(T_vista, 0)`` -> primer hit = 0, resto = delta real.
"""
import os
import logging
from pathlib import PurePosixPath as PosixPath
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, count, datediff, expr, floor, lag, last_day, lit,
    max as spark_max, mean, min as spark_min, stddev, sum as spark_sum,
    to_date, to_timestamp, unix_timestamp, when)
from pyspark.sql.functions import first as spark_first, last as spark_last

from .base import BaseAggregate
from ..steps import dynamic, path_delete, read_parquet_many

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

#: Agregaciones disponibles para ``time_stats`` / flags.
_STAT_FUNCS = {
    "sum": spark_sum,
    "mean": mean,
    "min": spark_min,
    "max": spark_max,
    "stddev": stddev,
    "count": count,
}

#: Operadores de condición para las reglas de flags del config.
def _flag_condition(rule: Dict[str, Any]) -> Column:
    """Traduce una regla ``{col, op, value}`` del config a un ``Column``."""
    c = col(rule["col"])
    op = rule["op"]
    value = rule["value"]
    if op == "eq":
        return c == value
    if op == "contains":
        return c.contains(value)
    if op == "startswith":
        return c.startswith(value)
    if op == "endswith":
        return c.endswith(value)
    if op in ("contains_any", "endswith_any", "startswith_any"):
        fn = {"contains_any": Column.contains,
              "endswith_any": Column.endswith,
              "startswith_any": Column.startswith}[op]
        conds = [fn(c, v) for v in value]
        out = conds[0]
        for cond in conds[1:]:
            out = out | cond
        return out
    raise ValueError(f"Unsupported flag condition op: {op}")


class AdobeAggregate(BaseAggregate):
    """Agregador de fuentes Adobe Clickstream (bancanet / mobile).

    Parameters
    ----------
    previous_path : str
        Path del paso anterior (``weekly_extract`` con tfrom semanal).
    agg_dict : Dict[str, Any]
        ``config_rigid[<source>].group_by`` — flags, dominios, stats.
    sqlContext : SparkSession
        Sesión de Spark.
    tables : List[str]
        Tablas ``*_historic`` de entrada (relativas a ``previous_path``).
    mode : str
        Modo de escritura, default ``'overwrite'``.
    is_dynamic : bool
        Recarga intermedios con ``_SUCCESS`` si True.
    """

    _key_cols = ["numcliente", "vintage_date", "tfrom", "tfrom_date"]

    def __init__(self,
                 previous_path: str,
                 agg_dict: Dict[str, Any],
                 sqlContext: SparkSession,
                 tables: Optional[List[str]] = None,
                 mode: str = "overwrite",
                 is_dynamic: bool = True) -> None:
        super().__init__(previous_path, agg_dict, sqlContext, mode)
        self.tables = tables or []
        self.is_dynamic = is_dynamic
        self.prefix = self.agg_dict.get("prefix", "adobe")
        self.flag_specs: Dict[str, List[Dict[str, Any]]] = \
            self.agg_dict.get("flags", {})
        self.flag_names = sorted(self.flag_specs.keys())
        self.time_vars: List[str] = self.agg_dict.get(
            "time_vars", ["T_inicio", "T_vista", "T_session"])
        self.time_stats: List[str] = self.agg_dict.get(
            "time_stats", ["sum", "mean", "min", "max", "stddev"])
        self.domains = self.agg_dict.get("domains", {})
        self.window_end_mode = self.agg_dict.get("window_end", "last_day")
        # paths
        head = str(PosixPath(self.previous_path).parent)
        self.hdfs = {
            "aggregate": str(PosixPath(head).joinpath("groupby")),
            "hits": str(PosixPath(head).joinpath("tmp/adobe_hits")),
        }

    # ------------------------------------------------------------------
    # helpers de naming
    # ------------------------------------------------------------------
    def _v(self, name: str) -> str:
        """Nombre de variable con prefijo de fuente (``bnt_pil_view``)."""
        return f"{self.prefix}_{name.lower()}"

    def _historic_df(self) -> DataFrame:
        """Union de las ``*_historic`` del extract de esta fuente."""
        assert self.tables, "AdobeAggregate requires 'tables'"
        paths = [str(PosixPath(self.previous_path).joinpath(t))
                 if t.endswith("_historic")
                 else str(PosixPath(self.previous_path).joinpath(t + "_historic"))
                 for t in self.tables]
        return read_parquet_many(self.sqlContext, paths)

    def _flag_exprs(self) -> List[Column]:
        """Columnas 0/1 por flag según las reglas AND del config."""
        exprs = []
        for flag in self.flag_names:
            conds = [_flag_condition(rule) for rule in self.flag_specs[flag]]
            cond = conds[0]
            for c in conds[1:]:
                cond = cond & c
            exprs.append(when(cond, 1).otherwise(0).alias(flag))
        return exprs

    # ------------------------------------------------------------------
    # Paso 1: hits enriquecidos (T_* + flags) — equiv. Checkpoint_01
    # ------------------------------------------------------------------
    @dynamic("hits")
    def _hits(self) -> DataFrame:
        """Hit-level enriquecido: tiempos de sesión + flags de producto.

        Reproduce el Checkpoint_01 del pipeline original pero sobre el
        ``_historic`` (ya join con pivot y con tfrom semanal). ``T_vista``
        usa ``coalesce`` en vez del ``when`` invertido del original.
        """
        df = self._historic_df()
        df = df.withColumn("date_time",
                           to_timestamp(col("date_time"), "yyyy-MM-dd HH:mm:ss"))
        session = Window.partitionBy("session_id").orderBy("date_time")
        session_full = session.rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        ts = unix_timestamp(col("date_time"))
        df = df.withColumn("T_inicio", ts - unix_timestamp(
            spark_first("date_time").over(session_full)))
        df = df.withColumn("T_vista", ts - unix_timestamp(
            lag("date_time", 1).over(session)))
        df = df.withColumn("T_vista", when(col("T_vista").isNull(), 0)
                           .otherwise(col("T_vista")))
        df = df.withColumn("T_session", unix_timestamp(
            spark_last("date_time").over(session_full)) - unix_timestamp(
            spark_first("date_time").over(session_full)))
        df = df.select("*", *self._flag_exprs())
        # file_date a fecha para el calculo de last_days en el aggregate
        df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM-dd"))
        return df

    # ------------------------------------------------------------------
    # Dominios de tfrom
    # ------------------------------------------------------------------
    def _weekly_exprs(self, suffix: str = "") -> List[Column]:
        """Agregaciones de un periodo (semana o quincena).

        Flags: ``max`` -> var binaria (onz) y ``sum`` -> ``<flag>_cnt``.
        T_*: cada stat configurada -> ``<tvar>_<stat>``. ``count`` -> hits.
        """
        aggs: List[Column] = []
        for flag in self.flag_names:
            aggs.append(spark_max(col(flag)).alias(
                self._v(flag) + suffix))
            aggs.append(spark_sum(col(flag)).alias(
                self._v(flag + "_cnt") + suffix))
        for tvar in self.time_vars:
            for stat in self.time_stats:
                aggs.append(_STAT_FUNCS[stat](col(tvar)).alias(
                    self._v(f"{tvar}_{stat}") + suffix))
        aggs.append(count(lit(1)).alias(self._v("hits") + suffix))
        return aggs

    def _weekly(self, df: DataFrame) -> DataFrame:
        """Dominio semanal: un row por (cliente, vintage, semana)."""
        return df.groupBy("numcliente", "vintage_date", "tfrom")\
            .agg(*self._weekly_exprs(),
                 spark_min("tfrom_date").alias("tfrom_date"))

    def _fortnightly(self, df: DataFrame) -> Optional[DataFrame]:
        """Dominio quincenal: rollup de pares de semanas (``_q``)."""
        cfg = self.domains.get("fortnightly", {})
        if not cfg.get("enabled", True):
            return None
        df_f = df.withColumn("tfrom", floor(col("tfrom") / 2))
        return df_f.groupBy("numcliente", "vintage_date", "tfrom")\
            .agg(*self._weekly_exprs(suffix="_q"),
                 spark_min("tfrom_date").alias("tfrom_date"))

    def _with_last_days(self, df: DataFrame) -> DataFrame:
        """``last_days`` = dias desde el hit hasta el fin de la ventana
        del vintage (``last_day(vintage_date)`` por consistencia con el
        ancla de ``weeks``; configurable con ``window_end``)."""
        end = last_day(to_date(col("vintage_date"))) \
            if self.window_end_mode == "last_day" else to_date(col("vintage_date"))
        return df.withColumn("last_days", datediff(end, col("file_date")) + 1)

    def _last_days(self, df: DataFrame) -> Optional[DataFrame]:
        """Dominio "últimos N días": una fila por (cliente, ventana w)
        con ``tfrom = w`` y variables ``*_ld`` — reproduce
        ``views_metrics``/``time_metrics`` de adobe_source."""
        cfg = self.domains.get("last_days", {})
        windows = cfg.get("windows", [])
        if not cfg.get("enabled", True) or not windows:
            return None
        flag_stats = cfg.get("flag_stats", ["sum"])
        time_stats = cfg.get("time_stats", ["sum", "mean"])
        df_ld = self._with_last_days(df)
        frames = []
        for w in windows:
            aggs: List[Column] = []
            for flag in self.flag_names:
                for stat in flag_stats:
                    aggs.append(_STAT_FUNCS[stat](col(flag)).alias(
                        self._v(f"{flag}_ld" if stat == "sum"
                                else f"{flag}_ld_{stat}")))
            for tvar in self.time_vars:
                for stat in time_stats:
                    aggs.append(_STAT_FUNCS[stat](col(tvar)).alias(
                        self._v(f"{tvar}_ld" if stat == "sum"
                                else f"{tvar}_ld_{stat}")))
            aggs.append(count(lit(1)).alias(self._v("hits_ld")))
            frame = df_ld.filter(col("last_days") <= w)\
                .groupBy("numcliente", "vintage_date").agg(*aggs)\
                .withColumn("tfrom", lit(w).cast("double"))
            frames.append(frame)
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f, allowMissingColumns=True)
        return out

    def _daily_seq(self, df: DataFrame) -> Optional[DataFrame]:
        """Dominio "secuencia diaria": por cada flag, el vector 0/1 de
        los últimos ``seq_days`` días con ``tfrom = índice de día`` —
        permite rachas **diarias** reales via ``*_onz_mt``."""
        cfg = self.domains.get("daily_seq", {})
        seq_days = int(cfg.get("days", 14))
        if not cfg.get("enabled", True) or seq_days <= 0:
            return None
        df_ld = self._with_last_days(df).filter(col("last_days") <= seq_days)
        aggs = [spark_max(col(flag)).alias(self._v(f"{flag}_d"))
                for flag in self.flag_names]
        return df_ld.groupBy("numcliente", "vintage_date",
                             col("last_days").alias("tfrom"))\
            .agg(*aggs)

    # ------------------------------------------------------------------
    def aggregate(self) -> DataFrame:
        """Une todos los dominios en el long frame del groupby."""
        df_hits = self._hits()
        frames = [self._weekly(df_hits)]
        for frame in (self._fortnightly(df_hits),
                      self._last_days(df_hits),
                      self._daily_seq(df_hits)):
            if frame is not None:
                frames.append(frame)
        # unionByName exige tipos iguales: tfrom llega int (weekly),
        # int (fort/dseq) o float (ld) -> normalizar a double
        frames = [f.withColumn("tfrom", col("tfrom").cast("double"))
                  for f in frames]
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f, allowMissingColumns=True)
        self._save_parquet(out, self.hdfs["aggregate"])
        return out
