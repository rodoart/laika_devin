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


class ChannelsAggregate:
    """
    Class to aggregate Channels Data

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
        channels_df = self.sqlContext.read.parquet(reading_path)
        channels_df.createOrReplaceTempView("channels")
        total = self.sqlContext.sql(self.agg_dict["query_str"])
        total = total.na.fill(0)

        aggregate_dict = self.agg_dict['query_dict']
        for op in aggregate_dict:
            for new_column in aggregate_dict[op]:
                if op == "flags":
                    total = total.withColumn(new_column, greatest(
                    *aggregate_dict[op][new_column]))
                if op == "sums":
                    total = total.withColumn(new_column, expr(
                    "+".join(aggregate_dict[op][new_column])))

        head, _ = os.path.split(self.previous_path)
        self._save_parquet(total, os.path.join(head, "groupby"))
