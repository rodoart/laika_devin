"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2026-03-24
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import greatest, expr, col, broadcast, month, when, expr, lit, create_map, lower, array, explode, struct, concat_ws, sum, max, min, when, col, count
from pyspark.sql.types import IntegerType, DoubleType
from itertools import chain
from typing import Any, Dict, List
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class MorkitaraAggregate:
    """
    Class to aggregate PNL Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    def __init__(self, previous_path: str, agg_dict: Dict[str, Any],
        sqlContext: SparkSession, mode: str = "overwrite") -> None:
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data

        Returns
        -------
        None
        """
        df_save.write.mode(self.mode).parquet(path)

    def aggregate(self) -> None:
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        historic = self.sqlContext.read.parquet(reading_path).drop("inf_dt","tfrom_date")
        columns_to_sum = self.agg_dict['columns_to_sum']
        columns_pivot = self.agg_dict['columns_pivot']
        group_by_key = self.agg_dict['group_by_key']
        logging.info(f"Columns Pivot: {columns_pivot}")
        grouped_data = historic.groupBy(*group_by_key).agg(
            *[sum(colu).alias(colu) for colu in columns_to_sum],
            max("sdo_maximo_mes").alias("sdo_maximo_tot"),
            min("sdo_minimo_mes").alias("sdo_minimo_tot"),
            *[sum(when(col('snapshot') == 'cheques', col(colu)).otherwise(0)).alias(f"{colu}_cheques") for colu in columns_pivot],
            *[sum(when(col('snapshot') == 'maestra', col(colu)).otherwise(0)).alias(f"{colu}_maestra") for colu in columns_pivot],
            *[sum(when(col('snapshot') == 'perfiles', col(colu)).otherwise(0)).alias(f"{colu}_perfiles") for colu in columns_pivot],
            *[sum(when(col('snapshot') == 'perfiles_ejecutivo', col(colu)).otherwise(0)).alias(f"{colu}_perfiles_ejecutivo") for
            colu in columns_pivot], #CAMBIARRRRRRR
            *[sum(when(col('snapshot') == 'otros', col(colu)).otherwise(0)).alias(f"{colu}_otros") for colu in columns_pivot],
            count(col('snapshot')).alias("num_product_debito")
        )

        grouped_data = grouped_data.withColumn('sdo_diff_min_max', col('sdo_maximo_tot') - col('sdo_minimo_tot')) \
            .withColumn('sdo_ratio_min_max', when(col('sdo_maximo_tot') != 0, col('sdo_minimo_tot') / col
            ('sdo_maximo_tot')).otherwise(-1)) \
            .withColumn('sdo_diff_min_max_oa', col('sdo_maximo_mes') - col('sdo_minimo_mes')) \
            .withColumn('sdo_ratio_min_max_oa', when(col('sdo_maximo_mes') != 0, col('sdo_minimo_mes') / col
            ('sdo_maximo_mes')).otherwise(-1)) \
            .withColumn('ratio_cgo_abo_cnt', when(col('abocnt') != 0, col('cgocnt') / col('abocnt')).otherwise
            (-1)) \
            .withColumn('ratio_cgo_abo_imp', when(col('aboimp') != 0, col('cgoimp') / col('aboimp')).otherwise
            (-1))

        df_out = grouped_data.fillna(0).drop("snapshot")
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_out, os.path.join(head, "groupby"))
