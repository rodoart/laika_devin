"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql.functions import col
from typing import Any, Dict, List
from .base import BaseAggregate

class LiabilitiesPullAggregate(BaseAggregate):
    """
    Class to aggregate Liabilities Data

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

    def _create_groupby_query(self, agg_dict: Dict[str, Any], liab: str, filter_col: List[str]) -> str:
        """
        Creates the SQL aggregation sentence to group the liabilities data

        Parameters
        ----------
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)
        liab: string
            Liabilities Product Source

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=agg_dict["query_dict"])
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}({1}) as {1}'.
                    format(item['agg'], item['variable'])
                    for _, item in agg_df.iterrows() if item['variable'] in filter_col]))
        group_str = ",".join(agg_dict["group_by_key"])
        query_str = "SELECT "+group_str+","+features + \
            " FROM df_agg_temp_df_" + liab + " GROUP BY "+group_str
        return query_str

    def _create_groupby_query_pagomatico(self, agg_dict: Dict[str, Any], filter_col: List[str]) -> str:
        """
        Creates the SQL aggregation sentence to group the liabilities data

        Parameters
        ----------
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)
        filter_col: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(data=agg_dict["PGMT_QUERY_DICT"])
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}({1}) as {1}'.
                    format(item['agg'], item['variable'])
                    for _, item in agg_df.iterrows() if item['variable'] in filter_col]))
        group_str = ",".join(agg_dict["group_by_key"])
        query_str = "SELECT "+group_str+","+features + \
            " FROM df_agg_temp_pgom" + " GROUP BY "+group_str
        return query_str

    def aggregate(self) -> None:
        """
        Process to aggregate Liabilities data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Get the auxliar path
        aux_path = self._sibling_path("auxiliar/outputs")
        maestra = self.sqlContext.read.parquet(
            os.path.join(aux_path, "maestra"))
        maestra = maestra.withColumn("numcliente", col(
            "numcliente").cast("string"))
        selected_columns = self.agg_dict['sel_cols']
        selected_group = self.agg_dict['group_by_key']
        sel_colums = [col(column) for column in (
            selected_group + selected_columns)]
        maestra = maestra.select(*sel_colums)
        onus_capt_snap = self.sqlContext.read.parquet(
            self._table_path())
        sel_vars = [col(column) for column in (
            ["numcliente"] + self.agg_dict["pgom_cols"])]
        df_pgom = onus_capt_snap.select(sel_vars)
        df_maes = maestra.filter('producto=="MAESTRA"')
        df_perf = maestra.filter('producto=="PERFILES"')
        df_peje = maestra.filter('producto=="PGJ"')
        df_cheq = maestra.filter('producto=="CHEQUES"')
        df_otro = maestra.filter('producto=="OTRO"')
        liabilities_list = ["maes", "perf", "peje", "cheq", "otro"]
        liabilities_dfs = [df_maes, df_perf, df_peje, df_cheq, df_otro]
        filter_col = self.agg_dict['filter_cols']
        queries_dict = {}
        for df, liab in zip(liabilities_dfs, liabilities_list):
            df.createOrReplaceTempView("df_agg_temp_df_"+liab)
            queries_dict[liab] = self._create_groupby_query(
                self.agg_dict, liab, filter_col)
        df_pgom.createOrReplaceTempView("df_agg_temp_pgom")
        queries_dict["pgom"] = self._create_groupby_query_pagomatico(
            self.agg_dict, filter_col)
        liabilities_grouped = []
        for df in liabilities_list + ["pgom"]:
            liabilities_grouped.append(
                self.sqlContext.sql(queries_dict[df]))
        df_maes, df_perf, df_peje, df_cheq, df_otro, df_pgom = liabilities_grouped

        # Calculate ONUS
        onus_capt_snap = onus_capt_snap.select(
            *sel_colums, *selected_group)
        onus_capt_snap_tmp = onus_capt_snap.groupby(
            self.agg_dict["group_by_key"]).sum(*selected_columns)

        for variable in selected_columns:
            onus_capt_snap_tmp = onus_capt_snap_tmp.withColumnRenamed(
            "sum("+variable+")", variable)

        liabilities_dfs = [df_maes, df_perf,
                    df_peje, df_cheq, df_otro, df_pgom]

        map_maes = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["maes_new_cols"]))
        map_perf = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["perf_new_cols"]))
        map_peje = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["peje_new_cols"]))
        map_cheq = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["cheq_new_cols"]))
        map_otro = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["otro_new_cols"]))
        map_pgom = dict(
            zip(self.agg_dict["sel_cols"], self.agg_dict["pgom_new_cols"]))

        rename_maps = [map_maes, map_perf,
                    map_peje, map_cheq, map_otro, map_pgom]

        for idx, src in enumerate(liabilities_dfs):
            onus_capt_snap_tmp = onus_capt_snap_tmp.join(
                src.select([col(column) for column in self.agg_dict["group_by_key"]] +
                    [col(c).alias(rename_maps[idx][c]) for c in self.agg_dict["sel_cols"]]),
                on=self.agg_dict["group_by_key"],
                how='left'
            )

        onus_capt_snap_tmp = onus_capt_snap_tmp.fillna(0)
        self._save_parquet(onus_capt_snap_tmp, self._sibling_path("groupby"))
