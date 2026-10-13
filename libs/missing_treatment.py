"""
Project: Missing Treatment (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gio Guerrero, Brayan Borquez, Gil Rodriguez, Jorge Galicia,
    Rodolfo Arturo Gonzalez
Update Date: 2025-02-20
Version: 1.1

Script containing the class and methods for the Missing Treatment in datapull
"""
import logging
from functools import reduce
import os
import re
import subprocess
from pathlib import PosixPath
from typing import Callable, List, Optional, Union

from numpy import source
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count as spark_count,
    count,
    expr,
    first,
    greatest,
    isnan,
    lit,
    max,
    max as spark_max,
    min,
    months_between,
    monotonically_increasing_id,
    sum as spark_sum,
    trunc,
    when,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

from .mobs.payroll_income import PreviousFixerPayrollIncome
from .mobs.payroll_streaks import PreviousFixerPayrollStreaks

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class MissingTreatment:
    """
    Class used for Missing Treatment in datapull

    Fills Values of a dataframe, this class is used to fill values
    after collect_history or after feature Engineering (MOBs).

    Parameters
    ----------
    year: string
        String with the year month to be processes with format YYYYMM
    mt_dict: dictionary
        Dictionary with the variables, methods and sources to be processed
    source: String
        Name of the source to specify paths
    n_mis: int
        Month's number of history 12,24,36,...,etc
    n_threads: int
        Number of threads to be used
    mode:
        How will the parquets be written
    """
    def __init__(self, year, mt_dict, source, n_mis, n_threads, mode="overwrite") -> None:
        """
        Constructor of missing_treatment class
        ----------
        """
        self.year = year
        self.mt_dict = mt_dict
        self.source = source
        self.n_mis = n_mis
        self.n_threads = n_threads
        self.mode = mode

    ######################################## metodo principal mobs (run_missing_treatment) ########################################
    def run_mt(self, path_mt, tables_mt, path_write, sql_context) -> None:
        """
        This method is the main method to run missing treatment, the objective is to fill missing values using statistics (median,ave...

        Parameters
        ----------
        path_mt: String
            String with the df to read before the last folder
        tables_mt: List
            List of strings to be read "n" subfolders
        path_write: string
            specifying the path where the parquet is going to be stored
        sql_context: SparkSession

        Returns
        ------
        Spark data frame
            Containing the missing treatment df after the fillings
        """
        intermedium=True
        list_dir=[]
        dir_temp1=os.path.join(path_write,self.year,self.source,"missing_treatment","temp1")
        dir_temp2=os.path.join(path_write,self.year,self.source,"missing_treatment","temp2")
        list_dir.append(dir_temp1)
        list_dir.append(dir_temp2)
        df_lst = self._get_dataframes(path_mt, tables_mt, sql_context)
        # Iterar sobre las tablas que existan en el directorio
        for item_df, df in enumerate(df_lst):
            # Iterar sobre los metodos de filling values
            if(self.source == "bureau_cohort"):
                df=self.__vars_select(df,self.mt_dict["missing_treatment"]["fill_variables"]["variable"])
            for item_method, method in enumerate(self.mt_dict["missing_treatment"]["fill_methods"]["metodo"]):
                grouped = self.mt_dict["missing_treatment"]["fill_methods"]["grouped"][item_method]
                if ("fill_stat" in method):
                    nmbr_stat=self.mt_dict["missing_treatment"]["fill_methods"]["valor"][item_method]
                    df = self.fill_stat(df, grouped, nmbr_stat,path_write,sql_context)
                    if(intermedium):
                        self.__write_parquet(df,os.path.join(path_write, self.year, self.source, "missing_treatment"), f"temp_{item_method}")
                        list_dir.append(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
                elif ("fill_value" in method):
                    df = self.fill_value(df, item_method)
                    if(intermedium):
                        self.__write_parquet(df,os.path.join(path_write, self.year, self.source, "missing_treatment"), f"temp_{item_method}")
                        list_dir.append(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
                elif ("fill_snap" in method):
                    df = self.fill_snap(df, grouped)
                    if(intermedium):
                        self.__write_parquet(df,os.path.join(path_write, self.year, self.source, "missing_treatment"), f"temp_{item_method}")
                        list_dir.append(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
                elif ("fill_historic" in method):
                    df = self.fill_historic(df, grouped, 3,path_write,sql_context)
                    if(intermedium):
                        self.__write_parquet(df,os.path.join(path_write, self.year, self.source, "missing_treatment"), f"temp_{item_method}")
                        list_dir.append(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
                elif ("fill_debito" in method):
                    df = self.fill_debito(df, grouped, sql_context)
                    if(intermedium):
                        self.__write_parquet(df,os.path.join(path_write, self.year, self.source, "missing_treatment"), f"temp_{item_method}")
                        list_dir.append(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
                df=sql_context.read.parquet(os.path.join(path_write, self.year, self.source, "missing_treatment",f"temp_{item_method}"))
            self.__write_parquet(df, os.path.join(
                path_write, self.year, self.source, "missing_treatment"), f"{tables_mt[item_df]}")

        for dir in list_dir:
            self._delete_intermediate(dir)
    ######################################## metodo principal mobs (run_mobs) ########################################
    def run_mobs(self, tables_path:str, pathwrite:str, features_path:str, feat_list:List[List[str]], periodo:int, sql_context: SparkSession) -> None:
        """
        This method is the main method to calculate the months on books of a client,
        the objective is to fill the features of new clients or the clients which have less than 12 months.

        Parameters
        ----------
        tables_path: List
            Path with collect_history for each source ("liabilities"...)
        pathwrite: String
            specifying the path where the parquet is going to be stored
        features_path: string
            String with the path of the original features
        feat_list: tuple
            tuple with the names of the features to imputate or copy
        periodo: int
            period of time for each feature (how the features applied the period (_3m,_4m,...,etc))
        sql_context: SparkSession
        """
        self.calculate_mobs(tables_path, pathwrite, sql_context)
        if self.source == "payroll_income":
            previous_fixer_object = PreviousFixerPayrollIncome(
                path_mobs_fe=pathwrite,
                features_path=features_path,
                write_mobs=pathwrite,
                feat_list=feat_list,
                periodo=periodo,
                n_mis=self.n_mis,
                n_threads=self.n_threads,
                sql_context=sql_context,
            )
            previous_fixer_object.multi_thread_imputate_features()
        elif self.source == "payroll_streaks":
            previous_fixer_object = PreviousFixerPayrollStreaks(
                path_mobs_fe=pathwrite,
                features_path=features_path,
                write_mobs=pathwrite,
                feat_list=feat_list,
                periodo=periodo,
                n_mis=self.n_mis,
                n_threads=self.n_threads,
                sql_context=sql_context,
            )
            previous_fixer_object.multi_thread_imputate_features()
        else:
            self.previous_fixer(pathwrite, features_path,
                pathwrite, feat_list, periodo, sql_context)

    ######################################## metodos para llenado mobs (Calculate) ########################################
    def calculate_mobs(self, historic_path, pathwrite, sql_context=None) -> None:
        """
        This method calls the corresponding calculate mobs depending on the sources
        that are received. Then it stores the obtained dataframe into the pathwrite.

        Parameters
        ----------
        historic_path: string
            specifiyng the path where liabilities collect tables are stored
        pathwrite: string
            specifiyng the path where the parquet is going to be stored
        sql_context: SparkSession
        """
        df_full = sql_context.createDataFrame([], StructType([StructField('numcliente', StringType(), True),
                                                            StructField('vintage_date', StringType(), True)]))
        tables_path = os.path.join(
            historic_path, self.year, "mobs_preparation")
        for source in self.mt_dict["missing_treatment"]["mobs"]["sources"]:
            list_tables = self.__list_tables(source)
            if (source == 'liabilities'):
                df = self.liabilities_mobs(
                    tables_path, list_tables, sql_context=sql_context)
            elif (source == 'credit_card'):
                df = self.credit_card_mobs(
                    tables_path, list_tables, sql_context=sql_context)
            elif (source == 'bureau'):
                df = self.bureau_mobs(tables_path, list_tables, sql_context)
            elif (source == 'payroll_income'):
                df = self.payroll_income_mobs(pathwrite, list_tables, sql_context)
            elif (source == 'payroll_streaks'):
                df = self.payroll_streaks_mobs(pathwrite, list_tables, sql_context)
            #
            df_full = df_full.join(
                df, on=["numcliente", "vintage_date"], how="fullouter")
        #
        if ("mobs_liabilities"in df_full.columns) \
            and ("mobs_credit_card" in df_full.columns):
            df_full = df_full.withColumn("max_liab_cc", greatest(col("mobs_liabilities"), col("mobs_credit_card")))
        #
        self.__write_parquet(df_full, pathwrite, "calculate_mobs")

    def liabilities_mobs(self, tables_path, table_names, sql_context=None) -> Any:
        """
        This method specifies the columns to be selected in the query for the
        6 liabilities tables and then calls _get_dataframes, _calculate_separated_mobs
        and mobs_tables_union.

        Parameters
        ----------
        tables_path: string
            specifiyng the path where liabilities collect tables are stored
        table_names: array
            contains the names of the tables to be used (for liabilities must be an array with 6 names)
        """
        def mobs_tables_union(dfs_liab) -> Any:
            """
            This method makes a union of all the data frames received.

            Parameters
            ----------
            dfs_liab: array
                contains the 6 data frames from liabilities historic tables
            Returns
            -------
            Spark data frame
                Containing one data frame with the union of all data frames
            """
            for item in range(len(dfs_liab)):
                if item == 0:
                    base = dfs_liab[item]
                else:
                    liab_aux = dfs_liab[item]
                    base = base.union(liab_aux)
            base = base.groupby(["numcliente", "vintage_date"]).\
                agg(max("mobs_liabilities").alias("mobs_liabilities"))
            return base

        query = ["bigint(numcliente) as numcliente",
                "to_date(cast(unix_timestamp(tfrom_date, 'yyyy-MM-dd') as timestamp)) as tfrom_date",
                "to_date(cast(unix_timestamp(fec_apertura, 'yyyy-MM-dd') as timestamp)) as fec_apertura",
                "tfrom", "to_date(cast(unix_timestamp(vintage_date,'yyyy-MM-dd') as timestamp)) vintage_date"]
        calculate_mobs = months_between(
            col("tfrom_date"), trunc(col("fec_apertura"), "month"))
        dfs = self._get_dataframes(
            tables_path, table_names, sql_context=sql_context)
        dfs_liab = self._calculate_separated_mobs(
            dfs, query, 'liabilities', calculate_mobs)
        base = mobs_tables_union(dfs_liab)
        return base

    def credit_card_mobs(self, tables_path, table_names, sql_context=None) -> Any:
        """
        This method specifies the columns to be selected in the query for the pnl table
        and then calls _get_dataframes, _calculate_separated_mobs and mobs_tables_union.

        Parameters
        ----------
        tables_path: string
            specifiyng the path where credit_card collect tables are stored
        table_names: array
            contains the names of the tables to be used (for credit_card must be an array with 1 name)

        Returns
        -------
        Spark data frame
            Containing the mobs for credit_card
        """
        query = ["bigint(numcliente) as numcliente",
                "to_date(cast(unix_timestamp(tfrom_date, 'yyyy-MM-dd') as timestamp)) as tfrom_date",
                "to_date(cast(unix_timestamp(fec_apertura, 'yyyy-MM-dd') as timestamp)) as fec_apertura",
                "tfrom", "to_date(cast(unix_timestamp(vintage_date,'yyyy-MM-dd') as timestamp)) vintage_date"]
        calculate_mobs = months_between(
            col("tfrom_date"), trunc(col("fec_apertura"), "month"))
        dfs_credit = self._get_dataframes(
            tables_path, table_names, sql_context=sql_context)
        dfs_credit = self._calculate_separated_mobs(
            dfs_credit, query, 'credit_card', calculate_mobs)
        return dfs_credit[0]

    def bureau_mobs(self, tables_path, list_tables, sql_context=None) -> DataFrame:
        """
        Calculate months in bureau of bureau customers.

        Parameters
        ----------
        tables_path: string
            specifying the path where bureau collect tables are stored
        list_tables: list of strings
            List with all the different tables needed (["adv_colocacion_i_t_m"])
        sql_context: SparkSession

        Returns
        -------
        Spark Dataframe
            With the bureau mobs
        """
        lst_pivot = self._get_dataframes(
            tables_path, list_tables, sql_context=sql_context)
        df_mobs = lst_pivot[0].select(
            "numcliente", "a5", "tfrom", "vintage_date")
        df_mobs = df_mobs.withColumn("mobs", col("a5").cast("int")+col("tfrom").cast("int")).\
            groupBy("numcliente", "vintage_date").agg(max("mobs").alias("mobs_bureau")).\
            fillna(0, subset=["mobs_bureau"])
        return df_mobs

    def payroll_income_mobs(self, tables_path:str, list_tables: List[str], sql_context:SparkSession) -> DataFrame:
        """
        Calcula los MOBS (Months on Books) no contiguos para diferentes
        columnas numéricas en un DataFrame de PySpark.
        los mobs finales estan separados por tef_1, tef_2 y tefs.

        Args:
            tables_path (str): La ruta a los archivos de tablas.
            list_tables (List[str]): Una lista de nombres de tablas a cargar.
            sql_context (SparkSession): El contexto de Spark utilizado para cargar los datos.

        Returns:
            DataFrame: Un DataFrame que contiene los MOBS calculados para las columnas numéricas especificadas.
        """
        # GET DATAFRAME
        datapull_path = str(PosixPath(tables_path).parent)
        source_data_frames = self._get_dataframes(datapull_path, list_tables, sql_context=sql_context)
        assert len(source_data_frames) == 1, "Only one mobs dataframe is expected"
        df_tagged = source_data_frames[0]
        assert "tfrom" in df_tagged.columns, "tfrom column is missing"
        assert "numcliente" in df_tagged.columns, "numcliente column is missing"
        #
        # GET NUMERIC COLUMNS
        numerical_columns = get_numeric_columns(df_tagged)
        # remove tfrom and numcliente
        try:
            numerical_columns.remove("tfrom")
        except:
            pass
        try:
            numerical_columns.remove("numcliente")
        except:
            pass
        # get tef columns
        tef_1_numerical_columns = [column for column in numerical_columns if "tef_1" in column]
        tef_2_numerical_columns = [column for column in numerical_columns if "tef_2" in column]
        tefs_numerical_columns = [column for column in numerical_columns if "tefs" in column]
        #
        # Get mobs from tefs
        #
        mobs_tef_1 = discontiguous_mobs(df_tagged, "mobs_tef_1", tef_1_numerical_columns, 0)
        mobs_tef_2 = discontiguous_mobs(df_tagged, "mobs_tef_2", tef_2_numerical_columns, 0)
        mobs_tefs = discontiguous_mobs(df_tagged, "mobs_tefs", tefs_numerical_columns, 0)
        #
        # join all
        #
        df_mobs = mobs_tef_1.join(mobs_tef_2, ["numcliente", "vintage_date"], "outer").join(mobs_tefs, ["numcliente", "vintage_date"], "outer")
        df_mobs = df_mobs.fillna(0.0, subset=["mobs_tef_1", "mobs_tef_2", "mobs_tefs"])
        return df_mobs

    def payroll_streaks_mobs(self, tables_path:str, list_tables: List[str], sql_context:SparkSession) -> DataFrame:
        """
        Genera un DataFrame con los mobs contiguos y no contiguos para las columnas 'tef_1', 'tef_2' y 'tefs'.
        """
        datapull_path = str(PosixPath(tables_path).parent)
        source_data_frames = self._get_dataframes(datapull_path, list_tables, sql_context=sql_context)
        assert len(source_data_frames) == 1, "Only one mobs dataframe is expected"
        df_tagged = source_data_frames[0]
        max_tfrom = df_tagged.coalesce(1).select(spark_max(col("tfrom"))).collect()[0][0]
        #
        df_tagged = df_tagged.withColumn("tefs", coalesce(col("tef_1"), col("tef_2")))
        #
        discontiguous_mobs_tef_1 = discontiguous_mobs(df_tagged, "discontiguous_mobs_tef_1", ["tef_1"], 0)
        discontiguous_mobs_tef_2 = discontiguous_mobs(df_tagged, "discontiguous_mobs_tef_2", ["tef_2"], 0)
        discontiguous_mobs_tefs = discontiguous_mobs(df_tagged, "discontiguous_mobs_tefs", ["tefs"], 0)
        #
        contiguous_mobs_tef_1 = contiguous_mobs(df_tagged, "contiguous_mobs_tef_1", ["tef_1"], 0)
        contiguous_mobs_tef_2 = contiguous_mobs(df_tagged, "contiguous_mobs_tef_2", ["tef_2"], 0)
        contiguous_mobs_tefs = contiguous_mobs(df_tagged, "contiguous_mobs_tefs", ["tefs"], 0)
        #
        # quitar un mes a todos los mobs, porque n_mis tiene un mes extra en el extract
        discontiguous_mobs_tef_1 = discontiguous_mobs_tef_1.withColumn("discontiguous_mobs_tef_1", when(col("discontiguous_mobs_tef_1")==max_tfrom+1, max_tfrom).otherwise(col("discontiguous_mobs_tef_1")))
        discontiguous_mobs_tef_2 = discontiguous_mobs_tef_2.withColumn("discontiguous_mobs_tef_2", when(col("discontiguous_mobs_tef_2")==max_tfrom+1, max_tfrom).otherwise(col("discontiguous_mobs_tef_2")))
        discontiguous_mobs_tefs = discontiguous_mobs_tefs.withColumn("discontiguous_mobs_tefs", when(col("discontiguous_mobs_tefs")==max_tfrom+1, max_tfrom).otherwise(col("discontiguous_mobs_tefs")))
        #
        contiguous_mobs_tef_1 = contiguous_mobs_tef_1.withColumn("contiguous_mobs_tef_1", when(col("contiguous_mobs_tef_1")==max_tfrom+1, max_tfrom).otherwise(col("contiguous_mobs_tef_1")))
        contiguous_mobs_tef_2 = contiguous_mobs_tef_2.withColumn("contiguous_mobs_tef_2", when(col("contiguous_mobs_tef_2")==max_tfrom+1, max_tfrom).otherwise(col("contiguous_mobs_tef_2")))
        contiguous_mobs_tefs = contiguous_mobs_tefs.withColumn("contiguous_mobs_tefs", when(col("contiguous_mobs_tefs")==max_tfrom+1, max_tfrom).otherwise(col("contiguous_mobs_tefs")))
        #
        # join all
        dfs_to_join = [discontiguous_mobs_tef_1, discontiguous_mobs_tef_2, discontiguous_mobs_tefs, contiguous_mobs_tef_1, contiguous_mobs_tef_2, contiguous_mobs_tefs]
        joined_mobs = reduce(lambda df1, df2: df1.join(df2, on=["vintage_date", "numcliente"], how="outer"), dfs_to_join)
        df_mobs = joined_mobs.fillna(0.0, subset=["discontiguous_mobs_tef_1", "discontiguous_mobs_tef_2", "discontiguous_mobs_tefs", "contiguous_mobs_tef_1", "contiguous_mobs_tef_2", "contiguous_mobs_tefs"])
        #
        return df_mobs
    ######################################## metodos para llenado mobs (Imputacion) ########################################
    def previous_fixer(self, path_mobs_fe, features_path, write_mobs, feat_list, periodo, sql_context) -> None:
        """
        Description:
        Fills the information for each feature, taking 3,6,9,12 months depending on how many months on books does the client have.

        Parameters
        ----------
        path_mobs: string
            Path were the mobs table is saved
        features_path: string
            Path of the original features calculated.
        pathwrite: string
            Path where the new features filled will be saved
        feat_list: list
            List of features to which mobs imputation will be applied
        sql_context: sql_context
            Your sql_context.
        """
        mb_ty = self.mt_dict["missing_treatment"]["mobs"]["mobs_tipo"][0]
        path_mobs = os.path.join(path_mobs_fe, "calculate_mobs")
        pathwrite = os.path.join(write_mobs, "imputate_mobs")
        join_mobs = sql_context.read.parquet(path_mobs).select(
            col("numcliente"), col("vintage_date"), col(mb_ty).alias('mobs'))
        raw_data = sql_context.read.parquet(os.path.join(features_path, "raw_features")).select(
            col("id_msf_tools_v2_12315"), col("numcliente"), col("vintage_date"))
        join_mobs = raw_data.join(join_mobs, ["numcliente", "vintage_date"], "left").drop(
            "numcliente").drop("vintage_date")
        join_mobs = join_mobs.withColumn("mobs", when(
            col("mobs").isNull(), 0).otherwise(col("mobs")))
        # genera la lista de meses de la periodicidad de los features [3,6,9,12],[2,4,6],...,etc
        lst_months = self.__get_months(periodo)
        df_paths=[]
        for item in range(len(lst_months)-1):
            df_paths.append(f"temp{item}")
        for feature in feat_list[0]:  # imputacion de features
            print(f"inicia imputacion {feature}")
            self.valor_previo4(features_path,feature,join_mobs,pathwrite,lst_months,sql_context)
        for feature in feat_list[1]:  # Copia de features
            print(f"inicia copia {feature}")
            self.__copy_feature(features_path, feature, pathwrite, sql_context)
        self._delete_intermediate_list(pathwrite, list(set(df_paths)))
    def valor_previo4(self,features_path, feature,mobs_info, pathwrite, lista_meses, sql_context) -> None:
        feats_path =  os.path.join(features_path,feature)
        for i in range(len(lista_meses)):
            num_temp = f"temp{str(i)}"
            if i == len (lista_meses)-1:
                path_out = pathwrite
            else:
                path_out = os.path.join(pathwrite,num_temp)
            self.valor_previo4_step(feats_path, feature,mobs_info, path_out, lista_meses[i], i, lista_meses, sql_context)
            feats_path = os.path.join(path_out,feature)
    def valor_previo4_step(self,features_path, feature,mobs_info, pathwrite, mes, num_iteracion, lista_meses, sql_context) -> None:
        data_feature =  sql_context.read.parquet(features_path)
        id_var = 'id_msf_tools_v2_12315'
        i = num_iteracion
        columnas = data_feature.columns
        vars_dicc = dict()
        for j in lista_meses:
            vars_dicc[str(j)]=[a for a in columnas if a.split('_')[-1] == str(j) + 'm' or a == id_var ]
        variables = vars_dicc[str(mes)]
        variables_no = [var for var in columnas if var not in variables or var ==id_var ]
        df_no = data_feature.select(variables_no)
        if i == 0:
            df_min = data_feature.select(variables)
            df_min = df_min.join(mobs_info, [id_var], "left")
            df_min = df_min.fillna(0, subset=["mobs"])
            df_ant = df_min.select([id_var, "mobs"])
            for columna in df_min.columns:
                if columna != id_var and columna != 'mobs':
                    df_ant = df_ant.withColumn(columna, lit(-99999))
        else:
            variables.append('mobs')
            df_min = data_feature.select(variables)
            mes_anterior = '_'+str(lista_meses[i-1])+ 'm'
            variable_anteriores = vars_dicc[str(lista_meses[i-1])]
            variable_anteriores.append('mobs')
            df_ant = data_feature.select(variable_anteriores)
            lista_columnas = df_ant.columns
            for columna in lista_columnas:
                columna_name = columna.replace( mes_anterior, '_'+str(mes)+'m')
                if columna != id_var and columna != 'mobs':
                    df_ant = df_ant.withColumn(columna_name, col(columna)).drop(columna)
        extremo_inferior = df_ant.filter(df_ant.mobs<mes)
        extremo_superior = df_min.filter(df_min.mobs>=mes)
        extremo_inferior = extremo_inferior.select(extremo_superior.columns)
        result = extremo_superior.union(extremo_inferior)
        i+=1
        df_no = df_no.drop('mobs')
        df_exit = result.join(df_no, [id_var], "left")
        if 'mobs' not in columnas:
            columnas.append('mobs')
        df_exit = df_exit.select(columnas)
        if num_iteracion == len(lista_meses)-1:
            df_exit = df_exit.drop('mobs')
        path_ = os.path.join(pathwrite,feature)
        df_exit.write.mode("overwrite").parquet(path_)
    ######################################## metodos para llenado de MT ########################################
    def fill_historic(self, df, grouped, kmiss,path_write,sql_context) -> Any:
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict
        """
        def snapshot(df,variable,grouped) -> Any:
            wp_client = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df = df.withColumn(variable,when((col("tfrom") == 0) & (col("null_count") <= 2) & (col(variable).isNull()), first(col(variable), ignorenulls=True).over(wp_client)).otherwise(col(variable)))
            return(df)

        def last_null(df,variable,grouped,n_mis) -> Any:
            wp_client_desc = Window.partitionBy(grouped).orderBy(col("tfrom").desc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df = df.withColumn(variable,
            when((col("tfrom") == (int(n_mis)-1)) & (col("null_count") <= 2) & (col(variable).isNull()), first(col(variable), ignorenulls=True).over(wp_client_desc)).otherwise(col(variable)))
            return(df)

        def interpolacion(df,variable,grouped,n_mis) -> Any:
            wp_client_mid = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(-1, 1)
            df = df.withColumn(variable,
            when(
                (col("tfrom") > 0) & (col("tfrom") < (int(n_mis)-1)) & (col("null_count") <= 2) & (col(variable).isNull()), avg(col(variable)).over(wp_client_mid)
            ).otherwise(col(variable)))
            return(df)

        def moving_average(df,variable,grouped,kmiss,n_mis) -> Any:
            wp_mov_avg_b = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(1, kmiss)
            wp_mov_avg_l = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween((kmiss)*-1, -1)
            df = df.withColumn(variable,
            when(
                ((col("null_count") == 3) & (col("tfrom") + kmiss) <= (int(n_mis)-1)) & (col(variable).isNull()), avg(col(variable)).over(wp_mov_avg_b)
            ).when(
                ((col("null_count") == 3) & (col("tfrom") + kmiss) > (int(n_mis)-1)) & (col(variable).isNull()), avg(col(variable)).over(wp_mov_avg_l)
            ).otherwise(col(variable)))
            return(df)

        dir_temp1=os.path.join(path_write,self.year,self.source,"missing_treatment","temp1")
        dir_temp2=os.path.join(path_write,self.year,self.source,"missing_treatment","temp2")
        wp_client = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            if (variable in df.columns):  # Si la columna existe en el df, hacer el llenado del missing
                null_col=df.filter(col(variable).isNull()).count()
                if(null_col != 0):
                    df = df.withColumn("null_count", count(when(df[variable].isNull(), 1)).over(wp_client))  # Conteo de nulos por cliente
                    unuse_df=df.filter(col("null_count") >= 4)
                    if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                        df=df.filter(col("null_count") < 4)
                        df=last_null(df,variable,grouped,self.n_mis)
                        df=snapshot(df,variable,grouped)
                        df=interpolacion(df,variable,grouped,self.n_mis)
                        df=moving_average(df,variable,grouped,kmiss,self.n_mis)
                        df=df.union(unuse_df)
                    df = df.drop("null_count")
                    df.write.mode("overwrite").parquet(dir_temp2)
                    tempdf=sql_context.read.parquet(dir_temp2)
                    tempdf.write.mode("overwrite").parquet(dir_temp1)
                    df=sql_context.read.parquet(dir_temp1)
            return (df)

    def fill_value(self, df, nmbr_id_variables) -> Any:
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict


        Parameters
        ----------
        df: Dataframe
            Df to be processed
        nmbr_id_variables: String
            String with the id, to identify wich variables will be filled in a dictionary

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        col_low=[]
        for item in df.columns:
            col_low.append(item.lower())
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            # validar que la variable del mt_dic exista en el df de origen
            if (variable.lower() in col_low):
                # Validar que se aplique metodo real number
                if ("real" in (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item].lower())):
                    nmbr_rplce = int(self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item].lower().replace(" ", "").replace("real", ""))
                    df = df.withColumn(variable, when(
                        col(variable).isNull(), nmbr_rplce).otherwise(col(variable)))
        return (df)

    def fill_stat(self, df, grouped, nmbr_id_stat,path_write,sql_context) -> Any:
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict


        Parameters
        ----------
        df: string
            Df to be processed
        grouped: List
            List of strings with the number of the variables how the dataframe will be grouped
        nmbr_id_stat: String
            ID of the statistic to be applied, "1" means will applied average, "0" means median

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        dir_temp1=os.path.join(path_write,self.year,self.source,"missing_treatment","temp1")
        dir_temp2=os.path.join(path_write,self.year,self.source,"missing_treatment","temp2")
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        window_spec = Window.partitionBy(grouped).orderBy("tfrom").rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            # Si se hara el metodo de llenado
            if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                # Si la variable del dic en el df, hacer el llenado del missing
                if (variable in df.columns):
                    null_col=df.filter(col(variable).isNull()).count()
                    if(null_col != 0):
                        if (nmbr_id_stat == "1"):  # Se aplica promedio (Average)
                            df = df.withColumn(variable, when(col(variable).isNull(), avg(
                                col(variable)).over(window_spec)).otherwise(col(variable))).sort(["tfrom"])
                        else:  # Se aplica mediana
                            percentile = expr(
                                f'percentile_approx({variable}, 0.5)')
                            df = df.withColumn(variable, when(col(variable).isNull(), percentile.over(
                                window_spec)).otherwise(col(variable))).sort(["tfrom"])
                    df.write.mode("overwrite").parquet(dir_temp2)
                    tempdf=sql_context.read.parquet(dir_temp2)
                    tempdf.write.mode("overwrite").parquet(dir_temp1)
                    df=sql_context.read.parquet(dir_temp1)
        return (df)

    def fill_snap(self, df, grouped) -> Any:
        """
        Description:
        Method to iterate the main dictionary and process the DF acording to the dict


        Parameters
        ----------
        df: string
            DF to be processed
        grouped: List
            List of strings with the number of the variables how the dataframe will be grouped

        Returns
        -------
        Dataframe
            Dataframe after fill the missing values using the snapshot
        """
        id_variables = self.mt_dict["missing_treatment"]["fill_methods"]["id_variables"][0]
        window_spec = Window.partitionBy(grouped).orderBy("tfrom").rangeBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        for item, variable in enumerate(self.mt_dict["missing_treatment"]["fill_variables"]["variable"]):
            if (self.mt_dict["missing_treatment"]["fill_variables"]["metodo"][item] == id_variables):
                if (variable in df.columns):
                    df = df.withColumn(variable, when((df.tfrom == 0) & df[variable].isNull(), first(
                        df[variable], ignorenulls=True).over(window_spec)).otherwise(df[variable]))
        return (df)

    def fill_debito(self, df, grouped, sql_context) -> Any:
        """
        Description:
        Method to fill_debito (liabilities)


        Parameters
        ----------
        df: Dataframe
            DF to be processed
        grouped: List
            List of strings with the number of the variables how the dataframe will be grouped
        sql_context: SparkSession

        Returns
        -------
        Dataframe
            Dataframe with the info filled
        """
        exclusiones = self.mt_dict["missing_treatment"]["fill_methods"]["valor"][0]
        # Definimos los limites
        expr_m = min("tfrom").alias("timeWindow")
        min_mob = int(df.groupBy().agg(expr_m).collect()[0][0])
        min_mob = min_mob-1
        expr = max("tfrom").alias("timeWindow")
        max_mob = int(df.groupBy().agg(expr).collect()[0][0])
        cuentas = Window.partitionBy(grouped)
        cancel = df.filter(df.indcancelada == 1)
        cancel = cancel.withColumn(
            "localizador_closed", max("tfrom").over(cuentas))
        cancel = cancel.select("numcliente", "num_contrato",
                               "vintage_date", "localizador_closed").distinct()
        new = df.filter(df.indapertura == 1)
        new = new.withColumn("localizador_new", max("tfrom").over(cuentas))
        new = new.select("numcliente", "num_contrato",
                         "vintage_date", "localizador_new").distinct()
        paso = df.join(cancel, grouped, how="left")
        paso = paso.join(new, grouped, how="left")
        paso1 = paso.withColumn("localizador_new", when(
            col("localizador_new").isNotNull(), col("localizador_new")).otherwise(max_mob))
        paso1 = paso1.withColumn("localizador_closed", when(col(
            "localizador_closed").isNotNull(), col("localizador_closed")).otherwise(min_mob))
        # sdo_capital
        paso1 = paso1.withColumn("MFlg_SDO", when(((paso1.tfrom <= paso1.localizador_new) & (paso1.tfrom > paso1.localizador_closed)) |
        (paso1.tfrom > paso1.localizador_new) & (paso1.tfrom <= paso1.localizador_closed) & (paso1.sdo_capital.isNull()),1).otherwise(0))
        #
        # Variables selection
        aux_exc = exclusiones[:]
        aux_exc = [el for el in aux_exc if el in df.columns]
        aux_exc.extend(grouped)
        aux_exc = list(set(aux_exc))
        lista_fin = list(set(df.columns)-set(aux_exc))
        paso1.createOrReplaceTempView("df_aux_flags")
        # Fill 0 para bandera=0
        text_fill = "select *,"+",".join(["case when MFlg_SDO=0 and "+item+" is null then 0 else "+item+" end as "+item for item in lista_fin])+\
            " from df_aux_flags"
        df_fill = sql_context.sql(text_fill)
        return (df_fill)
    ###############################################################
    # Helper methods
    ###############################################################
    def __vars_select(self,df,dict_stat) -> Any:
        """
        Description:
        Method to prepare the variables of the bureau_cohort data source
        The reason of this is because the order is different than the rest of the data sources (extract, group_by, pivot, missing_trea...

        Parameters
        ----------
        df: Dataframe
            Dataframe where the pivot variables are
        dict_stat: python dictionary
            Dictionary with the variables to be reduced
        """
        df = df.withColumn("id_msf_tools_v2_12315", monotonically_increasing_id())
        variables=[]
        patern_end="_i$"
        for item in dict_stat: #Se eliminan las columnas con terminaciones de _i y se quedan las unicas
            if(item.endswith("_i")):
                variables.append(re.sub(patern_end,"",item))
        lst_unq_var=[]
        patern_dgt="_[0-9]$|_[0-9][0-9]$" #Patron para terminaciones de variables "_0","_1","_2"...,"_99"
        for variable in df.columns:
            if(re.sub(patern_dgt,"",variable) not in lst_unq_var):
                lst_unq_var.append(re.sub(patern_dgt,"",variable))

        for item in lst_unq_var: #cols contiene las columnas sin las terminaciones
            if(item in variables):
                df=df.drop(f"{item}_1").drop(f"{item}_2")
                df=df.withColumnRenamed(f"{item}_0",item)
        return(df)

    def __copy_feature(self, features_path, feature, pathwrite, sql_context) -> None:
        """
        Description:
        Method to copy features that will not be processed

        Parameters
        ----------
        features_path: string
            String with the path where the original feature is
        feature: string
            string with the feature to be copied
        pathwrite: string
            String with the path where the feature will be saved
        sql_context: sql_context
            Your sql_context.
        """
        data_feature = sql_context.read.parquet(
            os.path.join(features_path, feature))
        self.__write_parquet(data_feature, pathwrite, feature)

    def __write_parquet(self, df: DataFrame, path:str, file_name:str) -> None:
        """
        Function to write a parquet

        Parameters
        ----------
        df: pyspark DataFrame
            Dataframe to write
        path: string
            Path where the parquet is going to be writed
        file_name: string
            Name of the parquet
        """
        df.write.mode(self.mode).parquet(os.path.join(path, file_name))

    def _get_dataframes(self, origin_path:str, table_names:List[str], sql_context:Optional[SparkSession] = None) -> List[DataFrame]:
        """
        This method joins the origin_path with each table name in the table_names
        array as a path, and reads the parquet in each of the paths.

        Parameters
        ----------
        origin_path: string
            specifying the collect history path
        table_names: array
            contains the names of the tables to be used (for liabilities must be an array with 6 names)
        sql_context: SparkSession
        """
        dfs = []
        for name in table_names:
            df = sql_context.read.parquet(os.path.join(origin_path, name))
            dfs.append(df)
        return dfs

    def _calculate_separated_mobs(self, dfs, query, source, calculate_mobs) -> list[Any]:
        """
        This method applies the query to each data frame in the dfs array,
        validates clients and then calculate mobs.

        Parameters
        ----------
        dfs: array
            contains the data frames where mobs are going to be calculated
        query: array
            contains the string of the columns to be selected
        source: sting
            specifies the mobs' source, is used for the mobs column rename
        calculate_mobs: operation
            contains the operation for calculating the relative mobs
        """
        dfs_liab = []
        validate_clients = col("numcliente") > 0
        calculate_real_mobs = col("mobs").cast('int') + col("tfrom").cast('int')
        for df in dfs:
            df = df.selectExpr(query).\
                filter(validate_clients).\
                withColumn("mobs", calculate_mobs).\
                withColumn("real_mobs", calculate_real_mobs).\
                groupBy("numcliente", "vintage_date").\
                agg(max("real_mobs").alias(f"mobs_{source}")).\
                fillna(0, subset=[f"mobs_{source}"])
            dfs_liab.append(df)
        return dfs_liab
    #
    def __list_tables(self, data_source:str) -> List[str]:
        """
        Description:
        Method to create a list with the tables to read depending the method to calculate
        """
        list_tables = []
        for item, source in enumerate(self.mt_dict["missing_treatment"]["mobs"]["source"]):
            if (source == data_source):
                list_tables.append(
                    self.mt_dict["missing_treatment"]["mobs"]["table_name"][item])
        return (list_tables)

    def __get_months(self, periodo:Union[str,int]) -> list[Any]:
        """
        Description:
        Method to create the months period ([3,6,9,12],[2,4,6,...,])
        """
        periodo = int(periodo)
        months_list = []
        for month in range(0, self.n_mis, periodo):
            if (month+periodo) < self.n_mis:
                months_list.append(month+periodo)
            else:
                months_list.append(self.n_mis)
        return (months_list)

    def __evaluate_path(self, path) -> bool:
        """
        Function to evaluate if a path exists
        """
        args_list = ['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        proc.communicate()
        value = True if proc.returncode == 0 else False
        return value

    def _delete_intermediate(self, path) -> None:
        """
        Function to delete paths using hadoop
        """
        if (self.__evaluate_path(path)):
            args_list = ['hdfs', 'dfs', '-rm', '-f', '-r', '-skipTrash', path]
            proc = subprocess.Popen(
                args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.communicate()

    def _delete_intermediate_list(self, path, id_list) -> None:
        """
        Function to delete paths using hadoop but using a list with multiple paths

        Parameters
        ----------
        path: string
            Path to delete
        id_list: List of strings
            concatenate the path and each item of the id_list
        """
        for item in id_list:
            self._delete_intermediate(os.path.join(path, item))


def get_numeric_columns(df: DataFrame) -> List[str]:
    """
    Obtiene una lista de nombres de columnas que son de tipo numérico en un DataFrame de PySpark.

    Args:
        df (DataFrame): El DataFrame de PySpark del cual se quieren obtener las columnas numéricas.

    Returns:
        List[str]: Una lista de nombres de columnas que son de tipo numérico.
    """
    # Filtrar las columnas que son de tipo numérico
    numeric_types = {"int", "bigint", "float", "double", "decimal"}
    numeric_columns = [col_name for col_name, col_type in df.dtypes if col_type in numeric_types]
    return numeric_columns


def count_non_null_values(df: DataFrame, vintage_column:str, id_column:str, column_to_check:str, output_column:Optional[str]="non_null_count") -> DataFrame:
    """
    Esta función cuenta los valores no nulos en una columna específica de un DataFrame, agrupados por dos columnas dadas.

    Ejemplo de uso:
    ----------------
    Supongamos que tenemos el siguiente DataFrame:

    +----------+--+--------------+-------------+
    |  vintage| id|column_to_check|sorting_column|
    +----------+--+--------------+-------------+
    |2023-01-01| 1|             1|             0|
    |2023-01-01| 1|             1|             1|
    |2023-01-01| 1|             1|             2|
    |2023-01-01| 2|          null|             0|
    |2023-01-01| 2|             2|             1|
    |2023-01-01| 2|          null|             2|
    |2023-01-01| 3|             1|             0|
    |2023-01-01| 3|          null|             1|
    |2023-01-01| 3|             2|             2|
    |2023-02-01| 1|          null|             0|
    |2023-02-01| 1|          null|             1|
    |2023-02-01| 1|          null|             2|
    |2023-02-01| 2|          null|             0|
    |2023-02-01| 2|             1|             1|
    |2023-02-01| 2|          null|             2|
    |2023-02-01| 3|             1|             0|
    |2023-02-01| 3|             1|             1|
    |2023-02-01| 3|             1|             2|
    +----------+--+--------------+-------------+

    Al aplicar la función count_non_null_values(df, 'vintage', 'id', 'column_to_check'), obtendremos:

    +----------+--+--------------+
    |  vintage| id|non_null_count|
    +----------+--+--------------+
    |2023-02-01| 3|             3|
    |2023-02-01| 1|             0|
    |2023-02-01| 2|             2|
    |2023-01-01| 2|             1|
    |2023-01-01| 3|             2|
    |2023-01-01| 1|             3|
    +----------+--+--------------+

    Args:
        df (DataFrame): El DataFrame de entrada.
        vintage_column (str): El nombre de la columna que representa la fecha.
        id_column (str): El nombre de la columna que representa el identificador.
        column_to_check (str): El nombre de la columna en la que se contarán los valores no nulos.
        output_column (Optional[str], optional): El nombre de la columna de salida que contendrá el conteo de valores no nulos.

    Returns:
        DataFrame: Un DataFrame con las columnas vintage, id y non_null_count, que representa el conteo de valores no nulos por grupo.
    """
    if output_column is None:
        output_column = "non_null_count"
    # Group by vintage and id, and count non-null values in column_to_check
    result = df.groupBy(vintage_column, id_column).agg(
        spark_count(col(column_to_check)).cast("double").alias(output_column)
    )
    return result


def get_last_contiguous_non_null_value(df, vintage_column, id_column, column_to_check, sorting_column, output_column) -> Any:
    """
    Obtiene el último valor contiguo de `sorting_column` en el que `column_to_check` no es nulo.

    El DataFrame tiene las siguientes columnas:
    - vintage: es una fecha que se emplea para separar la data, cada una debería ser independiente de otra.
    - id: es el ID de la tabla, único dentro de cada vintage date. Cada uno de los IDs se repite para todos los sorting values y vintage dates.
    - column_to_check: es la columna con valores nulos que se quiere revisar.
    - sorting_column: es la columna que se emplea para ordenar la data. Es único a nivel vintage date y cliente.

    La función devuelve el último valor contiguo de `sorting_column` en el que la columna `column_to_check` no es nula. Si todos los valores son nulos, devuelve -1.

    Args:
        df (DataFrame): DataFrame de entrada con las columnas especificadas.
        vintage_column (str): Nombre de la columna que representa la fecha de vintage.
        id_column (str): Nombre de la columna que representa el ID único dentro de cada vintage.
        column_to_check (str): Nombre de la columna que contiene valores nulos a revisar.
        sorting_column (str): Nombre de la columna que se usa para ordenar los datos.

    Returns:
        DataFrame: DataFrame con las columnas `vintage`, `id` y `last_non_null_sorting_column`,
            que indica el último valor contiguo de `sorting_column` donde `column_to_check` no es nulo.
            Si todos los valores son nulos desde el inicio, el valor será -1.

    Resultado esperado:

    +----------+--+------------------------+
    |  vintage| id|last_non_null_sorting_column|
    +----------+--+------------------------+
    |2023-02-01| 3|                       2|
    |2023-02-01| 1|                      -1|
    |2023-02-01| 2|                       0|
    |2023-01-01| 2|                      -1|
    |2023-01-01| 3|                       1|
    |2023-01-01| 1|                       2|
    +----------+--+------------------------+
    """
    # Define a window function to partition by vintage and id, and order by sorting_column
    window_spec = Window.partitionBy(vintage_column, id_column).orderBy(sorting_column)
    # Add a column to identify the first null value within the window
    df = df.withColumn('is_null', col(column_to_check).isNull())
    # Add a cumulative sum column to identify contiguous non-null blocks
    df = df.withColumn('cumsum', spark_sum(when(col('is_null'), 1).otherwise(0)).over(window_spec))
    # Filter out rows where cumsum is greater than 0 (i.e., after the first null)
    df_filtered = df.filter(col('cumsum') == 0)
    # Get the maximum sorting_column value for each vintage and id
    result = df_filtered.groupBy(vintage_column, id_column).agg(
        spark_max(col(sorting_column)).alias(output_column)
    )
    # Replace null values with -1
    result = result.withColumn(output_column, when(col(output_column).isNull(), -1).otherwise(col(output_column)))
    # Join with the original DataFrame to ensure all vintage and id combinations are included
    result = df.select(vintage_column, id_column).distinct().join(result, on=[vintage_column, id_column], how='left')
    # Replace null values with -1 for missing combinations
    result = result.withColumn(output_column, when(col(output_column).isNull(), -1).otherwise(col(output_column)))
    #
    return result


def contiguous_mobs(
    df:DataFrame,
    output_column_name:str,
    columns_to_evaluate:List[str],
    existing_columns_number_tolerance:Optional[int] = 0,
    missing_filter_condition:Optional[Callable[[str], Column]]= lambda column: (col(column) <= 0) | (col(column).isNull()) | (isnan(col(column)))
) -> DataFrame:
    """
    Calcula una columna de salida basada en la evaluación de una lista de columnas para valores no faltantes.

    Args:
        df (DataFrame): El DataFrame de PySpark que contiene los datos.
        output_column_name (str): El nombre de la columna de salida que se agregará al DataFrame resultante.
        columns_to_evaluate (List[str]): Una lista de nombres de columnas que se evaluarán para valores no faltantes.
        existing_columns_number_tolerance (Optional[int], optional): El número de columnas no faltantes requerido para considerar una
        missing_filter_condition (Optional[Callable[[str], Column]], optional): Una función que toma un nombre de columna y devuelve

    Returns:
        DataFrame: Un nuevo DataFrame con la columna de salida agregada, que contiene el conteo de filas que cumplen con la condición
    """
    # ADDRESS MISSING VALUES
    if missing_filter_condition is None:
        missing_filter_condition = lambda column: (col(column) <= 0) | (col(column).isNull()) | (isnan(col(column)))
    if existing_columns_number_tolerance is None:
        existing_columns_number_tolerance = 0
    #
    # ADD MISSING COUNTS
    count_expr = sum(when(~missing_filter_condition(column), 1).otherwise(0) for column in columns_to_evaluate)
    # ADD EXISTING COLUMNS COUNT
    df = df.withColumn("existing_columns_count", count_expr)
    df = df.withColumn("filtered_value", when(col("existing_columns_count") > existing_columns_number_tolerance, col("tfrom")).otherwise(None))
    #
    result = get_last_contiguous_non_null_value(df=df, vintage_column="vintage_date", id_column="numcliente", column_to_check="filtered_value", sorting_column="tfrom", output_column=output_column_name)
    result = result.withColumn(output_column_name, col(output_column_name)+1)
    result = result.withColumn(output_column_name, col(output_column_name).cast("double"))
    return result


def discontiguous_mobs(
    df:DataFrame,
    output_column_name:str,
    columns_to_evaluate:List[str],
    existing_columns_number_tolerance:Optional[int] = 0,
    missing_filter_condition:Optional[Callable[[str], Column]]= lambda column: (col(column) <= 0) | (col(column).isNull()) | (isnan(col(column)))
) -> DataFrame:
    """
    Calcula una columna de salida basada en la evaluación de una lista de columnas para valores no faltantes.

    Args:
        df (DataFrame): El DataFrame de PySpark que contiene los datos.
        output_column_name (str): El nombre de la columna de salida que se agregará al DataFrame resultante.
        columns_to_evaluate (List[str]): Una lista de nombres de columnas que se evaluarán para valores no faltantes.
        existing_columns_number_tolerance (Optional[int], optional): El número de columnas no faltantes requerido para considerar una
        missing_filter_condition (Optional[Callable[[str], Column]], optional): Una función que toma un nombre de columna y devuelve

    Returns:
        DataFrame: Un nuevo DataFrame con la columna de salida agregada, que contiene el conteo de filas que cumplen con la condición
    """
    # ADDRESS MISSING VALUES
    if missing_filter_condition is None:
        missing_filter_condition = lambda column: (col(column) <= 0) | (col(column).isNull()) | (isnan(col(column)))
    if existing_columns_number_tolerance is None:
        existing_columns_number_tolerance = 0
    #
    # ADD MISSING COUNTS
    count_expr = sum(when(~missing_filter_condition(column), 1).otherwise(0) for column in columns_to_evaluate)
    # ADD EXISTING COLUMNS COUNT
    df = df.withColumn("existing_columns_count", count_expr)
    df = df.withColumn("filtered_value", when(col("existing_columns_count") > existing_columns_number_tolerance, col("tfrom")).otherwise(None))
    #
    result = count_non_null_values(df=df, vintage_column="vintage_date", id_column="numcliente", column_to_check="filtered_value", output_column=output_column_name)
    result = result.withColumn(output_column_name, col(output_column_name).cast("double"))
    return result
