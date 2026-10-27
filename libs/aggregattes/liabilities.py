"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql.functions import col, lit
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
        # El aggregate materializa auxiliar/outputs/df_<key> por cada
        # tabla de missing_treatment + df_pgom de pagomatico, los une y
        # agrega. ``table_names`` es el dict {key: tabla_historic} y
        # ``pagomatico`` la tabla del pagomatico (clave hermana).
        aux_path = self._sibling_path("auxiliar/outputs")
        for key, value in self.agg_dict["table_names"].items():
            liabilities_df = self.sqlContext.read.parquet(
                os.path.join(self.previous_path, value))
            filter_columns = liabilities_df.columns
            selected_columns = [colu for colu in self.agg_dict[
                "query_dict"]["variable"] if colu in filter_columns]
            liabilities_df = liabilities_df.filter(
                liabilities_df.numcliente.isNotNull())
            liabilities_df = liabilities_df.select(
                self.agg_dict["group_by_key"] + selected_columns)
            liabilities_df.createOrReplaceTempView("df_agg_temp_df_"+key)
            sql_query = self._create_groupby_query(
                self.agg_dict, key, filter_columns)
            liabilities_df = self.sqlContext.sql(sql_query)
            liabilities_df = liabilities_df.withColumn("src", lit(key))
            liabilities_df = liabilities_df.fillna(0)
            self._save_parquet(liabilities_df,
                               os.path.join(aux_path, "df_"+key))

        pagomatico_df = self.sqlContext.read.parquet(
            os.path.join(self.previous_path, self.agg_dict["pagomatico"]))
        pagomatico_df = pagomatico_df.withColumnRenamed(
            "num_cliente", "numcliente")
        filter_columns_pgom = pagomatico_df.columns
        selected_columns_pgom = [colu for colu in self.agg_dict[
            "PGMT_QUERY_DICT"]["variable"] if colu in filter_columns_pgom]
        pagomatico_df = pagomatico_df.filter(
            pagomatico_df.numcliente.isNotNull())
        pagomatico_df = pagomatico_df.select(
            self.agg_dict["group_by_key"] + selected_columns_pgom)
        pagomatico_df.createOrReplaceTempView("df_agg_temp_pgom")
        sql_query = self._create_groupby_query_pagomatico(
            self.agg_dict, selected_columns_pgom)
        pagomatico_df = self.sqlContext.sql(sql_query)
        pagomatico_df = pagomatico_df.withColumn("src", lit("pgom"))
        if "sdo_capital" in filter_columns_pgom:
            pagomatico_df = pagomatico_df.withColumnRenamed(
                "sdo_capital", "saldo_mensual")
        pagomatico_df = pagomatico_df.fillna(0)
        self._save_parquet(pagomatico_df,
                           os.path.join(aux_path, "df_pgom"))

        df_maes = self.sqlContext.read.parquet(aux_path+"/df_maes")
        df_perf = self.sqlContext.read.parquet(aux_path+"/df_perf")
        df_peje = self.sqlContext.read.parquet(aux_path+"/df_peje")
        df_cheq = self.sqlContext.read.parquet(aux_path+"/df_cheq")
        df_otro = self.sqlContext.read.parquet(aux_path+"/df_otro")
        df_pgom = self.sqlContext.read.parquet(aux_path+"/df_pgom")

        columns_to_add = [
            x for x in df_perf.columns if x not in df_pgom.columns]
        for column in columns_to_add:
            df_pgom = df_pgom.withColumn(column, lit(None))

        all_liabilities = df_maes.unionByName(df_perf)\
            .unionByName(df_peje)\
            .unionByName(df_cheq)\
            .unionByName(df_otro)\
            .unionByName(df_pgom)

        self._save_parquet(all_liabilities, os.path.join(
            self.previous_path, "appends"))
        all_liabilities = self.sqlContext.read.parquet(
            os.path.join(self.previous_path, "appends"))

        # ``selected_columns`` queda con las vars de la ULTIMA tabla del
        # loop (orden de insercion de ``table_names``) — comportamiento
        # fiel al original
        onus_capt_snap_tmp = all_liabilities.groupBy(
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
