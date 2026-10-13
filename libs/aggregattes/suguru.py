"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2026-03-24
Version: 0.1

Class and Functions required to aggregate (general)
"""

from .base import BaseAggregate

class SuguruAggregate(BaseAggregate):
    """
    Class to aggregate

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
        Process to aggregate bureau pull data.

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
        self._save_parquet(df_out, self._sibling_path("groupby"))
