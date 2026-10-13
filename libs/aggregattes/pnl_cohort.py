"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2026-03-24
Version: 0.1

Class and Functions required to aggregate (general)
"""

from .base import BaseAggregate

class PNLCohortAggregate(BaseAggregate):
    """
    Class to aggregate PNL cohort data

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

    def aggregate(self) -> None:
        """
        Process to aggregate PNL cohort data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        reading_path = self._table_path()
        df_out = self.sqlContext.read.parquet(reading_path).filter("tfrom==0").drop("inf_dt","tfrom_date")
        df_out = df_out.fillna(0)
        df_out.createOrReplaceTempView("tempPnl")
        filter_columns = df_out.columns
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'], filter_columns)
        df_out_f = self.sqlContext.sql(query_str)
        self._save_parquet(df_out_f, self._sibling_path("groupby"))
