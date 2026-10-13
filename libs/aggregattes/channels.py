"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

from pyspark.sql.functions import greatest, expr
from .base import BaseAggregate

class ChannelsAggregate(BaseAggregate):
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
        self._save_parquet(total, self._sibling_path("groupby"))
