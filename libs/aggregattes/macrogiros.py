"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import pandas as pd
from typing import Any, Dict, List
from .base import BaseAggregate

class MacrogirosAggregate(BaseAggregate):
    """
    Class to aggregate Macrogiros Data

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

    def _create_groupby_query(self, query_dict: Dict[str, Any], group_by: List[str]) -> str:
        """
        Creates the SQL aggregation sentence to group the macrogiros data.

        Parameters
        ----------
        query_dict: Dictionary
            Macrogiro Concept- Macrogiros Abbreviation pair
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict.items(), columns=[
            'giros', 'giros_colname'])

        features = (','.join(["sum(case when purchase > 0 and c2 = '{0}' then purchase else 0 end ) as {1},sum(case when purchase > 0 and c2 = '{0}' then purchasetx else 0 end ) as {1}_cnt ".
                    format(item['giros'], item['giros_colname'])
                    for _, item in agg_df.iterrows()]))
        features = features + ",sum(case when purchase > 0 then purchase else 0 end ) as tot_pur, sum(case when purchase > 0 then purchasetx else 0 end ) as no_trans"
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM macrogiros GROUP BY "+group_str
        return query_str

    def aggregate(self) -> None:
        """
        Process to aggregate Macrogiros data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        reading_path = self._table_path()
        macrogiros = self.sqlContext.read.parquet(reading_path)
        macrogiros = macrogiros.na.fill("sin clasificar", subset=["c2"])
        macrogiros.createOrReplaceTempView("macrogiros")

        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        self._save_parquet(df_out, self._sibling_path("groupby"))
