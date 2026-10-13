"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Any, Dict, List
from .base import BaseAggregate
import logging

class BureauPullAggregate(BaseAggregate):
    """
    Class to aggregate Bureau Pull Data

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

    def _create_groupby_query(self, query_dict: Dict[str, Any], group_by: List[str], filter_columns: List[str]) -> str:
        """
        Creates the SQL aggregation sentence to group the bureau pull data

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
        low_filtered_columns = [col.lower() for col in filter_columns]
        features = (','.join(['{0}(case when {1}> 0 then {1} else 0 end ) as {2}'.
                    format(item['agg'], item['variable'], item['feature'])
                    for _, item in agg_df.iterrows() if item['variable'].lower() in low_filtered_columns]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempBureau GROUP BY "+group_str
        return query_str

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

        reading_path = self._table_path()
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempBureau")
        filter_columns = df_in.columns
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'], filter_columns)
        #print(query_str)
        logging.debug("Bureau Pull Query String: %s", query_str)
        df_out = self.sqlContext.sql(query_str)
        df_gcb = self.getCostumerBureau(
            df_out, self.agg_dict['query_dict'], self.sqlContext)
        self._save_parquet(df_gcb, self._sibling_path(
            "groupby", self.agg_dict['table_name']))
