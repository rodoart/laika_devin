# -*- coding: utf-8 -*-
"""
Project: Data Pull Framework (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2023-01-04
Version: 0.1

Clase y funciones para empaquetar las features en un vector
(``VectorAssembler`` de Spark ML) — producto final del datapull.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, explode, struct, lit, concat_ws
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
import os
from typing import Any, Dict

from .steps import save_parquet


class ColumnsToVector:
    """Ensambla las columnas de features en un ``features_vector``.

    Parameters
    ----------
    write_path : str
        Path de trabajo de la fuente (se lee ``prev_path`` y se escribe
        ``vector_assembler`` dentro de él).
    v2c_dict : Dict[str, Any]
        Parámetros que necesita la clase (``source``, ``prev_path``,
        ``table_name``).
    sqlContext : SparkSession
        Tu sqlContext.
    mode : str, optional
        Modo de escritura del parquet, por defecto ``'overwrite'``.
    """

    def __init__(self, write_path: str, v2c_dict: Dict[str, Any],
                 sqlContext: SparkSession, mode: str = "overwrite") -> None:
        self.write_path: str = write_path
        self.v2c_dict: Dict[str, Any] = v2c_dict
        self.sqlContext: SparkSession = sqlContext
        self.mode: str = mode

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """Guarda datos como parquet en la ubicación indicada.

        Parameters
        ----------
        df_save : DataFrame
            Datos a guardar.
        path : str
            Path donde almacenar los datos.
        """
        save_parquet(df_save, path, self.mode)

    def run(self) -> None:
        """Ejecuta el empaquetado: lee las features, las pone en un
        ``features_vector`` y guarda ``vector_assembler``.

        Solo conserva ``numcliente``, ``vintage`` y ``features_vector``
        en la salida final.
        """
        source = self.v2c_dict["source"]
        print("V2C: "+source)

        if source in ("bureau_cohort"):
            read_path = os.path.join(self.write_path, self.v2c_dict["prev_path"], self.v2c_dict["table_name"])
        else:
            read_path = os.path.join(self.write_path, self.v2c_dict["prev_path"])

        df_in = self.sqlContext.read.parquet(read_path)
        df_in = df_in.na.fill(0)
        feature_columns = [c for c in df_in.columns if c not in ["numcliente","vintage_date", "id_msf_tools_v2_12315"]]

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_vector")
        final_features_df = assembler.transform(df_in)
        final_features_df = final_features_df.select("numcliente","vintage_date", "features_vector")
        final_features_df = final_features_df.withColumnRenamed("vintage_date","vintage")
        save_parquet(final_features_df,
                     os.path.join(self.write_path, "vector_assembler"),
                     self.mode)
