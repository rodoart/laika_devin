"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2026-03-24
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql.functions import greatest, expr, col, broadcast, month, when, expr, lit, create_map, lower, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from itertools import chain
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class LaikaAggregate:
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

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite") -> None:
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save, path) -> None:
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
        df_out = self.sqlContext.read.parquet(reading_path).filter("tfrom==0").drop("vintage", "tfrom_date","tfrom","first_faper",
        "last_faper","inf_dt")
        df_out = df_out.fillna(0)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_out, os.path.join(head, "groupby"))
