"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
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


class PNLAggregate:
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

    def _create_groupby_query(self, query_dict, group_by, filter_columns) -> str:
        """
        Creates the SQL aggregation sentence to group the PNL data

        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = ",".join(["{0}({1}) as {1}".format(
            item["agg"], item["variable"]) for _, item in agg_df.iterrows() if item['variable'] in filter_columns])
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempPnl GROUP BY "+group_str
        return query_str

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
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempPnl")
        filter_columns = df_in.columns
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'], filter_columns)
        df_out = self.sqlContext.sql(query_str)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_out, os.path.join(head, "groupby"))
