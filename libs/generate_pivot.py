# -*- coding: utf-8 -*-
"""
Project: Data Pull Framework (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2023-01-04
Version: 0.1

Clase y funciones para transponer datos de tabla — pasar de formato largo
(long) a formato ancho (wide).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, explode, struct, lit, concat_ws
from pyspark.sql.types import IntegerType
import os
import logging
from typing import Any, Dict, List, Optional

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

class GeneratePivot:
    """Transpone una tabla de datos (long -> wide).

    El resultado es una fila por ``(numcliente, vintage_date)`` y una
    columna por combinación ``<feature>_<tfrom>``.

    Parameters
    ----------
    previous_path : str
        Path donde se leen los datos.
    pivot_dict : Dict[str, Any]
        Parámetros que necesita la clase (``source``, ``pivot_key``,
        ``table_name``).
    sqlContext : SparkSession
        Tu sqlContext.
    mode : str, optional
        Modo de escritura del parquet, por defecto ``'overwrite'``.
    pivot_clients : DataFrame, optional
        Si se pasa, el wide se completa con left join contra esta malla
        ``(numcliente, vintage_date)``: todos los clientes del pivot
        aparecen en la salida y sus celdas de features sin dato quedan
        en 0 (los features de ventana se imputan a -99999 vía mobs).
    """

    def __init__(self, previous_path: str, pivot_dict: Dict[str, Any],
                 sqlContext: SparkSession, mode: str = "overwrite",
                 pivot_clients: Optional[DataFrame] = None) -> None:
        self.previous_path: str = previous_path
        self.pivot_dict: Dict[str, Any] = pivot_dict
        self.sqlContext: SparkSession = sqlContext
        self.mode: str = mode
        self.pivot_clients: Optional[DataFrame] = pivot_clients

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """Guarda datos como parquet en la ubicación indicada.

        Parameters
        ----------
        df_save : DataFrame
            Datos a guardar.
        path : str
            Path donde almacenar los datos.
        """
        df_save.write.mode(self.mode).parquet(path)

    def pivot_data(self) -> None:
        """Pasa de formato largo a ancho y guarda en ``pivot``.

        Explota las columnas distintas de ``pivot_key`` en pares
        ``(key, value)``, construye la clave de columna ``<key>_<tfrom>``
        y pivotea por ella con ``avg``. En ``bureau_cohort`` guarda bajo
        ``pivot/<table_name>``.
        """
        source = self.pivot_dict["source"]
        if source in ("corporate", "clocks", "bureau_pull", "bureau_cohort", "payroll_income"):
            read_path = os.path.join(
                self.previous_path, self.pivot_dict["table_name"])
        else:
            read_path = self.previous_path
        #P
        df_in = self.sqlContext.read.parquet(read_path)
        group_by = self.pivot_dict["pivot_key"]
        cols, dtypes = zip(*[(c, t)
            for (c, t) in df_in.dtypes if c not in group_by])
        kvs = explode(array(
            *[struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        # Fuentes con dominios de tfrom mezclados (adobe_*): las filas de un
        # dominio traen null en las vars del resto; sin este filtro el
        # pivot crearía columnas ``<var>_<tfrom>`` all-null que el fe
        # rellenaría como features constantes.
        if self.pivot_dict.get("drop_null_values"):
            df_long = df_long.filter(col("kvs.value").isNotNull())
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        # keep_pivot_clients: ningún cliente del pivot puede perderse —
        # los sin actividad llegan con features a 0 y el mobs les imputa
        # -99999 en las ventanas (es la base de la imputación)
        if self.pivot_clients is not None:
            feat_cols = [c for c in df_wide.columns
                         if c not in ("numcliente", "vintage_date")]
            df_wide = self.pivot_clients.join(
                df_wide, ["numcliente", "vintage_date"], "left"
            ).fillna(0, subset=feat_cols)
        head, _ = os.path.split(self.previous_path)

        if source in ("bureau_cohort"):
            self._save_parquet(df_wide, os.path.join(head, "pivot",self.pivot_dict["table_name"]))
            df_wide.unpersist()
        else:
            self._save_parquet(df_wide, os.path.join(head, "pivot"))
            df_wide.unpersist()

    def pivot_data_aux(self, path: str, group_by: List[str]) -> None:
        """Versión auxiliar de long -> wide sobre un path arbitrario.

        Parameters
        ----------
        path : str
            Path del que leer los datos.
        group_by : List[str]
            Columnas excluidas del proceso de transposición (las claves).
        """
        df_in = self.sqlContext.read.parquet(path)
        cols, dtypes = zip(*[(c, t)
            for (c, t) in df_in.dtypes if c not in group_by])
        kvs = explode(array(
            *[struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        head, _ = os.path.split(path)
        df_wide.write.mode("overwrite").parquet(os.path.join(head, "pivot"))
        df_wide.unpersist()
