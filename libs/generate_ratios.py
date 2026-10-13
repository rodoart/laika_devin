# -*- coding: utf-8 -*-
"""
Project: Data Pull Framework (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to calculate ratios (features transformations)
"""
from pyspark.sql.functions import array, col, explode, struct, lit, concat_ws
from pyspark.sql.types import IntegerType
import pandas as pd
import os
import re
import subprocess
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

class GenerateRatios:
    """
    Class to calculate ratios features

    Parameters
    ----------
    previous_path: string
        Path to read data from
    ratios_dict: Dictionary
        Parameters needed by the class(key, group_by, etc)
    source: string
        Data Source(e.g. "corporate")
    sqlContext: spark context
        Your sqlContext
    mode: string, optional
        Parquet write mode, defaults to 'overwrite'.

    """

    def __init__(self, previous_path, ratios_dict, source, sqlContext, mode="overwrite") -> None:
        self.previous_path = previous_path
        self.ratios_dict = ratios_dict
        self.source = source
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

    def run(self) -> None:
        """
        Calculate ratios based on given source specified on constructor.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        if self.source == "pnl":
            self.ratios_pnl()
        if self.source == "pnl_new":
            self.ratios_pnl_new()
        if self.source == "corporate":
            self.ratios_corporate()

    def ratios_pnl(self) -> None:
        """
        Calculate ratios features for PNL data source.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        df_in = self.sqlContext.read.parquet(self.previous_path)
        df_in.createOrReplaceTempView("temp")
        logging.debug("Ratios PNL Input Columns: %s",df_in.columns)
        filter_columns = set(["_".join([x for x in text1.split("_")[:-1] if x != ""]) for text1 in df_in.columns])
        logging.debug("Ratios PNL Clean Columns: %s",filter_columns)
        pnl_ratios = pd.DataFrame.from_dict(self.ratios_dict["ratio_dict"])
        cociente = ",".join([",".join(["case when {1}_{3}=0 then -1 else {0}_{3}/{1}_{3} end as {2}_{0}_{1}_{3}".format(
            item["numerador"], item["denominador"], self.ratios_dict["prefix"], t_aux) for t_aux in range(self.ratios_dict
            ["n_time"])]) for index, item in pnl_ratios.iterrows() if item["numerador"] in filter_columns and item["denominador"] in
            filter_columns])
        #print(cociente)
        query = "select *, {0} from temp ".format(cociente)
        df_final = self.sqlContext.sql(query)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_final, os.path.join(head, "ratios"))

    def ratios_pnl_new(self) -> None:
        """
        Calculate ratios features for PNL data source.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        df_in = self.sqlContext.read.parquet(self.previous_path)
        df_in.createOrReplaceTempView("temp")
        logging.debug("Ratios PNL Input Columns: %s",df_in.columns)
        filter_columns = set(["_".join([x for x in text1.split("_")[:-1] if x != ""]) for text1 in df_in.columns])
        logging.debug("Ratios PNL Clean Columns: %s",filter_columns)
        pnl_ratios = pd.DataFrame.from_dict(self.ratios_dict["ratio_dict"])
        cociente = ",".join(["case when {1}_{3}=0 then -1 else {0}_{3}/{1}_{3} end as {2}_{0}_{1}_{3}".format(
            item["numerador"], item["denominador"], self.ratios_dict["prefix"], t_aux) for index, item in pnl_ratios.iterrows() if
            item["numerador"] in filter_columns and item["denominador"] in filter_columns for t_aux in range(self.ratios_dict
            ["n_time"])])
        #print(cociente)
        query = "select *, {0} from temp ".format(cociente)
        df_final = self.sqlContext.sql(query)
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_final, os.path.join(head, "ratios"))

    def ratios_corporate(self) -> None:
        """
        Calculate ratios features for Corporate data source.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        path_write = self.previous_path.replace("/features", "")
        df_fe_all = self._join_features_ratios_corporate(
            path_features=self.previous_path, path_write=path_write)
        time_window = self.ratios_dict["time_window"]
        percentiles = self.ratios_dict["percentiles"]
        corp_dict = self._make_dicts_aux_corporate(time_window, percentiles)
        keep_var = self.ratios_dict["keep_var"]
        prefix = self.ratios_dict["prefix"]
        df_fe_all.createOrReplaceTempView("temp")
        cociente = ",".join(["case when {1} = 0 then -1 else {0}/{1} end as {2}_{0}_{1}".format(
            item["numerador"], item["denominador"], prefix) for index, item in corp_dict.iterrows()])
        query = "select {0}, {1} from temp ".format(keep_var, cociente)
        df_final = self.sqlContext.sql(query)
        ratios_path = os.path.join(self.previous_path, "ratios_features")
        self._save_parquet(df_final, ratios_path)

    def _make_dicts_aux_corporate(self, time_window, percentiles) -> DataFrame:
        """
        Creates the Corporate Ratios Dictionary.

        Parameters
        ----------
        time_window: list
            List of all months grouped in features
        percentiles: list
            List of percentiles used in corporate

        Returns
        ------
        bool
            True if path exist, False otherwise
        """
        lista_fe = self.ratios_dict["lista_fe"]
        num_antiguedad = ['sum_t_bajas_a_{}_{}m'.format(
            anti, month) for anti in percentiles for month in time_window]*len(lista_fe)
        num_antiguedad_bloq = ['sum_t_bajas_ab_{}m'.format(
            month) for month in time_window]*len(lista_fe)
        num_ingreso = ['sum_t_bajas_i_{}_{}m'.format(
            ingreso, month) for ingreso in percentiles for month in time_window]*len(lista_fe)
        num_ingreso_v2 = ['sum_tot_baja_i_{}_{}m'.format(
            ingreso, month) for ingreso in percentiles for month in time_window]*len(lista_fe)
        num_industria = ['sum_t_ind_bajas_i_{}_{}m'.format(
            ingreso, month) for ingreso in percentiles for month in time_window]*len(lista_fe)
        num_puntual = ['sum_t_bajas_i_{0}_{1}m'.format(anti, month) for anti in percentiles for month in time_window] + ['sum_t_bajas_a_{0}_{1}m'.format(
            anti, month) for anti in percentiles for month in time_window] + ['sum_t_bajas_ab_{0}m'.format(month) for month in
            time_window]
        num_puntual_v2 = ['sum_tot_baja_i_{0}_{1}m'.format(
            anti, month) for anti in percentiles for month in time_window]
        num_puntual_industria = ['sum_t_ind_bajas_i_{0}_{1}m'.format(
            anti, month) for anti in percentiles for month in time_window]
        numerador = num_antiguedad+num_antiguedad_bloq+num_ingreso+num_industria + \
            num_puntual+num_puntual_industria+num_ingreso_v2+num_puntual_v2
        den_antiguedad = ['{2}_t_act_a_{0}_{1}m'.format(
            anti, month, item_fe) for item_fe in lista_fe for anti in percentiles for month in time_window]
        den_antiguedad_bloq = ['{1}_t_act_ab_{0}m'.format(
            month, item_fe) for item_fe in lista_fe for month in time_window]
        den_ingreso = ['{2}_t_act_i_{0}_{1}m'.format(
            ingreso, month, item_fe) for item_fe in lista_fe for ingreso in percentiles for month in time_window]
        den_ingreso_v2 = ['{2}_tot_act_i_{0}_{1}m'.format(
            ingreso, month, item_fe) for item_fe in lista_fe for ingreso in percentiles for month in time_window]
        den_industria = ['{2}_t_ind_act_i_{0}_{1}m'.format(
            ingreso, month, item_fe) for item_fe in lista_fe for ingreso in percentiles for month in time_window]
        den_puntual = ['t_act_i_{0}_{1}'.format(anti, month) for anti in percentiles for month in [0]*len(time_window)]+['t_act_{0}_{1}'.format(
            anti, month) for anti in percentiles for month in [0]*len(time_window)]+['t_act_ab_{0}'.format(month) for month in [0]*len(time_window)]
        den_puntual_v2 = ['tot_act_i_{0}_{1}'.format(
            anti, month) for anti in percentiles for month in [0]*len(time_window)]
        den_puntual_industria = ['t_ind_act_i_{0}_{1}'.format(
            anti, month) for anti in percentiles for month in [0]*len(time_window)]
        denominador = den_antiguedad+den_antiguedad_bloq+den_ingreso+den_industria + \
            den_puntual+den_puntual_industria+den_ingreso_v2+den_puntual_v2
        dicti_corp = {'numerador': numerador,
                      'denominador': denominador}
        corp_ratios = pd.DataFrame.from_dict(dicti_corp)
        return corp_ratios

    def _join_features_ratios_corporate(self, path_features, path_write) -> Any:
        """
        Join Corporate Data source ratios features.

        Parameters
        ----------
        path_features: string
            path where features are located
        path_write: string
            path to write the auxiliar pivot

        Returns
        -------
        Pyspark dataframe
            Dataframe with all features needed
        """
        # list of all features
        raw_name = self.ratios_dict["raw_name"]
        pivot = self.sqlContext.read.parquet(
            os.path.join(path_features, raw_name))
        list_raw_n_features = self.ratios_dict["list_raw_n_features"]
        id_ = self.ratios_dict["keep_var"]
        for feature in list_raw_n_features:
            features_path = os.path.join(path_features, feature)
            if self._evaluate_path(features_path):
                fe_df = self.sqlContext.read.parquet(features_path)
                fe_df = fe_df.withColumnRenamed(id_, 'aux_id')
                condiciones = [pivot.id_msf_tools_v2_12315 == fe_df.aux_id]
                pivot = pivot.join(fe_df, condiciones,
                    how='left').drop('aux_id')

        path_write_pivot = os.path.join(path_write, "aux_ratios")
        pivot.write.mode(self.mode).parquet(path_write_pivot)
        pivot = self.sqlContext.read.parquet(path_write_pivot)
        return (pivot)

    def _evaluate_path(self, path) -> Any | bool:
        """
        Check if path exists in hadoop.

        Parameters
        ----------
        path: string
            Path to check if exists

        Returns
        -------
        bool
            True if path exist, False otherwise
        """
        args_list = ['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        proc.communicate()
        value = proc.returncode == 0 and True or False
        return value
