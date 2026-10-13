"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import greatest, expr, col, broadcast, month, when, expr, lit, create_map, lower, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from itertools import chain
from typing import Any, Dict, List
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class CorporateAggregate:
    """
    Class to aggregate Corporate Data

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
        Class Constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def aggregate(self) -> None:
        """
        Process to aggregate corporate data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        corporate = self.sqlContext.read.parquet(os.path.join(
            self.previous_path, self.agg_dict['table_name']))
        conditions = self.agg_dict["conditions"]
        logging.debug("Variables en corporate[%d]: %s",len(corporate.columns),corporate.columns)
        p = col('seg').isin('PENSIONADOS')
        g = col('seg').isin('GOBIERNO')
        ind_i2_0 = col('pct_ind_i2') == 0
        ind_i2_1 = col('pct_ind_i2') == 1
        ind_i5_0 = col('pct_ind_i5') == 0
        ind_i5_1 = col('pct_ind_i5') == 1
        ind_i5_2 = col('pct_ind_i5') == 2
        ind_i5_3 = col('pct_ind_i5') == 3
        ind_i5_4 = col('pct_ind_i5') == 4
        iv_i2_0 = col('pct_iv2') == 0
        iv_i2_1 = col('pct_iv2') == 1
        iv_i5_0 = col('pct_iv5') == 0
        iv_i5_1 = col('pct_iv5') == 1
        iv_i5_2 = col('pct_iv5') == 2
        iv_i5_3 = col('pct_iv5') == 3
        iv_i5_4 = col('pct_iv5') == 4
        new_columns = self.agg_dict["new_columns"]
        for column in new_columns:
            if column == "cat_ind_i2_seg":
                corporate = corporate.withColumn("cat_ind_i2_seg", when(p & ind_i2_0,1).otherwise(when(p & ind_i2_1,2).otherwise(when
                (g & ind_i2_0,3).otherwise(when(g & ind_i2_1,4).otherwise(when(~(p | g) & ind_i2_0,5).otherwise(when(~(p | g) &
                ind_i2_1,6).otherwise(-99999)))))))
            if column == "cat_iv2_seg":
                corporate = corporate.withColumn("cat_iv2_seg", when(p & iv_i2_0,1).otherwise(when(p & iv_i2_1,2).otherwise(when(g &
                iv_i2_0,3).otherwise(when(g & iv_i2_1,4).otherwise(when(~(p | g) & iv_i2_0,5).otherwise(when(~(p | g) & iv_i2_1,6).
                otherwise(-99999)))))))
            if column == "cat_ind_i5_seg":
                corporate = corporate.withColumn("cat_ind_i5_seg", when(p & ind_i5_0,1).otherwise(when(p & ind_i5_1,2).otherwise(when
                (p & ind_i5_2,3).otherwise(when(p & ind_i5_3,4).otherwise(when(p & ind_i5_4,5).otherwise(when(g & ind_i5_0,6).
                otherwise(when(g & ind_i5_1,7).otherwise(when(g & ind_i5_2,8).otherwise(when(g & ind_i5_3,9).otherwise(when(g &
                ind_i5_4,10).otherwise(when(~(p | g) & ind_i5_0,11).otherwise(when(~(p | g) & ind_i5_1,12).otherwise(when(~(p | g) &
                ind_i5_2,13).otherwise(when(~(p | g) & ind_i5_3,14).otherwise(when(~(p | g) & ind_i5_4,15).otherwise
                (-99999))))))))))))))))
            if column == "cat_iv5_seg":
                corporate = corporate.withColumn("cat_iv5_seg", when(p & iv_i5_0,1).otherwise(when(p & iv_i5_1,2).otherwise(when(p &
                iv_i5_2,3).otherwise(when(p & iv_i5_3,4).otherwise(when(p & iv_i5_4,5).otherwise(when(g & iv_i5_0,6).otherwise(when
                (g & iv_i5_1,7).otherwise(when(g & iv_i5_2,8).otherwise(when(g & iv_i5_3,9).otherwise(when(g & iv_i5_4,10).otherwise
                (when(~(p | g) & iv_i5_0,11).otherwise(when(~(p | g) & iv_i5_1,12).otherwise(when(~(p | g) & iv_i5_2,13).otherwise
                (when(~(p | g) & iv_i5_3,14).otherwise(when(~(p | g) & iv_i5_4,15).otherwise(-99999))))))))))))))))
        drop_columns = self.agg_dict["drop_columns"]
        for column in drop_columns:
            corporate = corporate.drop(column)
        path_save = os.path.join(self.previous_path.replace(
            "extract", "groupby"), self.agg_dict["table_name"])
        self._save_parquet(corporate, path_save)

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
