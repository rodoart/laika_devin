"""
Project: Extract (Time Collection Refactorization) (PEP 8 & Python3.6)
Developer: Brayan Borquez
Update Date: 2022-12-22
Version: 1.1

Script containing the class and functions for the extract in datapull
"""
from datetime import datetime
from pyspark.sql import DataFrame
import pandas as pd
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
import subprocess
from random import randint
from multiprocessing.pool import ThreadPool
from cytoolz.functoolz import partial
import os
import re

from .extract_special_treatments.default import ExtractSpecialTreatment
from .extract_special_treatments.payroll_streaks import PayrollStreaksExtractSpecialTreatment

class Extract():
    special_treatment_sources = [
        "payroll_streaks", "payroll_income"
    ]
    """
    Class used for extract in datapull

    Parameters
    ----------
    pivot_table: pyspark dataframe
        Dataframe of the pivot
    path: string
        path where we want to save the tables
    sources_cat: Dictionary
        All the constants and information of the sources we want to make the extraction
    source: string
        Name of the source
    date_id: string
        Name of the id to save folder
    sqlContext: pyspark sqlContext
        Your sqlContext
    number_threads: int
        Number of threads to use for paralelism, by default 32
    mode: string
        Mode for writes parquets, by default and recommended overwrite
    name: string
        Name for saving the step, by default extract


    """
    FORMAT_TIME = "%Y-%m-%d"

    def __init__(self,pivot_table,working_path,n_mis,sources_cat, source, sqlContext,number_threads=32,mode="overwrite",
    name="extract") -> None:
        self.pivot_table = pivot_table
        self.working_path = working_path
        self.sources_cat = sources_cat
        self.source = source
        self.sqlContext = sqlContext
        self.mode = mode
        self.n_mis = n_mis
        self.number_threads = number_threads
        self.tables_in = []
        self.controls_in = []
        self.controls_out = []
        self.controls_out_f = []
        self.name = name

    def _create_queries(self, list_dates, table_name, database, type_date, name_mis, select_variables) -> tuple[list[Any], list[Any]]:

        """
        Functions that creates the queries

        Parameters
        ----------
        list_dates: list
            list with all dates we want information
        table_name: string
            Name of the table source
        database: string
            Name of the database where table is located
        type_date: string
            The type of date if the information comes for month tables, if table is partitioned should be ""
        name_mis: string
            Name of the date column in partitioned table

        Returns
        -------
        queries: list
            All the queries needed for the extract
        tables: list
            Names of all the tables used for queries
        """
        n = len(list_dates)
        queries = []
        tables_list = []
        if type_date:
            meses={
            1:"ene",
            2:"feb",
            3:"mar",
            4:"abr",
            5:"may",
            6:"jun",
            7:"jul",
            8:"ago",
            9:"sep",
            10:"oct",
            11:"nov",
            12:"dic"
            }
            if type_date == "YYYYMM":
                tables = [table_name.replace("XXXX", list_dates[k][0:4] + list_dates[k][5:7]) for k in range(n)]
            elif type_date == "YYYYM":
                tables = [table_name.replace("XXXX", list_dates[k][0:4] + str(int(list_dates[k][5:7]))) for k in range(n)]
            elif type_date == "YYMM":
                tables = [table_name.replace("XXXX", list_dates[k][2:4] + list_dates[k][5:7]) for k in range(n)]
            elif type_date == "mmmYY":
                tables = [table_name.replace("XXXX", meses[int(list_dates[k][5:7])] + list_dates[k][0:4]) for k in range(n)]
            elif type_date == "MM_YYYY":
                tables = [table_name.replace("XXXX", list_dates[k][5:7] + "_" + list_dates[k][0:4]) for k in range(n)]
            for table in tables:
                query = f"SELECT {select_variables} FROM {database}.{table}"
                tables_list.append(table)
                queries.append(query)
        else:
            for k in range(len(list_dates)-1):
                query = f"SELECT {select_variables} FROM {database}.{table_name} WHERE {name_mis} >= '{list_dates[k]}' and {name_mis} < '{list_dates[k+1]}'"
                table = table_name + "_" + list_dates[k][0:4] + list_dates[k][5:7]
                tables_list.append(table)
                queries.append(query)
        return queries,tables_list

    def _special_treatment(self, input_df: DataFrame, table_name:str, date:str, query:str) -> "ExtractSpecialTreatment":
        """
        Despues de lo filtro de fecha y del select, hace un tratamiento especial a la tabla.

        Parameters
        ----------
        input_df: DataFrame
            DataFrame containing the input data.
        table_name: str
            Name of the source table.
        date: str
            Date for which the information is needed.
        query: str
            Query string to be used for extraction.

        Returns
        -------
        ExtractSpecialTreatment
            An instance of ExtractSpecialTreatment or its subclass based on the source.
        """
        if self.source in ["payroll_streaks", "payroll_income"]:
            special_treatment_object = PayrollStreaksExtractSpecialTreatment
        else:
            special_treatment_object = ExtractSpecialTreatment
        return special_treatment_object(parent=self, input_df=input_df, table_name=table_name, date=date, query=query,
        sqlContext=self.sqlContext)

    def _get_vintages_dates(self,pivot,name_vintage_date,table_key,sqlContext) -> list[Any]:
        """
        Get the distinct vintages dates from pivot

        Parameters
        ----------
        pivot: pyspark DataFrame
            Dataframe containing the customers we want the information and the vintage date of that customer
        name_vintage_date: string
            Name we are using as vintage in the pivot
        table_key: string
            key for temporal view, usually "master_data"
        sqlContext: pyspark sqlContext
            Your sqlContext

        Returns
        -------
        List
            All the vintages dates in pivot
        """
        df_pivot = pivot
        df_pivot.createOrReplaceTempView("pivot_temporal_"+table_key)
        vintages = sqlContext.sql(f"select distinct {name_vintage_date} from pivot_temporal_"+table_key).collect()
        vintages = [vintages[p][0] for p in range(len(vintages))]
        vintages.sort(reverse = True)
        return vintages

    def _range_dates(self,vintages,retraso,n_mis) -> list[Any]:
        """
        Function that make the range of dates of all data

        Parameters
        ----------
        vintages: list
            List with all the vintages in datetime
        retraso: int
            Number of months of lag
        n_mis: int
            Number of months to obtain information

        Returns
        -------
        List
            List with the complete range of dates
        """
        min_vintage = min(vintages)
        max_vintage = max(vintages)
        max_date = max_vintage + relativedelta(days = 1) - relativedelta(months = retraso - 1, days = 1)
        min_date = min_vintage + relativedelta(days = 1) - relativedelta(months = n_mis + retraso - 1, days = 1)
        aux = min_date
        list_dates = [aux.strftime(self.FORMAT_TIME)]
        while aux != max_date:
            aux = aux + relativedelta(days = 1) + relativedelta(months = 1,days = -1)
            list_dates.append(aux.strftime(self.FORMAT_TIME))
        return list_dates

    def _range_vintages(self,vintages,retraso,n_mis) -> list[Any]:
        """
        Function that make the range of dates of distinct vintages

        Parameters
        ----------
        vintages: list
            List with all the vintages in datetime
        retraso: int
            Number of months of lag
        n_mis: int
            Number of months to obtain information

        Returns
        -------
        List
            List with the range of dates for all te vintages
        """
        range_vin = []
        for vintage in vintages:
            max_date = vintage + relativedelta(days = 1) - relativedelta(months = retraso, days= 1)
            min_date = vintage + relativedelta(days = 1) - relativedelta(months = retraso + n_mis - 1, days = 1)
            range_vin.append((min_date.strftime(self.FORMAT_TIME), max_date.strftime(self.FORMAT_TIME)))
        return range_vin

    def _write_parquet(self, df, path, file_name) -> tuple[()]:
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
        df.write.mode(self.mode).parquet(os.path.join(path,file_name))
        return()

    def _evaluate_path(self, path) -> bool:
        """
        Function to evaluate if a path exists
        Parameters
        ----------
        path: string
            Path to check if exist or not

        Returns
        ------
        bool
            True if path exist, false otherwise

        """
        args_list=['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        proc.communicate()
        value = True if proc.returncode == 0 else False
        return value

    def _delete_intermediate(self,path) -> None:
        """
        Function to delete paths using hadoop

        Parameters
        ----------
        path: string
            Paths to delete

        """

        if self._evaluate_path(path):
            args_list=['hdfs', 'dfs', '-rm', '-f', '-r','-skipTrash', path]
            proc=subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            proc.communicate()
    def _create_cohorts(self, vintages_dates, vintage_ranges, key_pivote, save_path, name_vintage, table_name, pivot, sqlContext) -> list[Any]:
        """
        Create the cohorts for the distinct vintages

        Parameters
        ----------
        vintages_dates: list
            All the vintages dates in pivot
        vintage_ranges: list
            List of tuple (min_date,max_date) of the range of vintages
        key_pivote: string
            Primary key of pivot table
        save_path: string
            Path were the results are saved
        name_vintage:
            Name of vintage in the pivot
        table_name: strin
            Name of table of source
        pivot: pyspark dataframe
            Pivot table
        sqlContext:
            Your sqlContext


        Returns
        ------
        list:
            Paths with all the cohorts created

        """
        df_history = sqlContext.read.parquet(os.path.join(save_path, table_name + "_full"))
        path_aux_list=[]
        for k in range(len(vintages_dates)):
            vintage = vintages_dates[k]
            df_history = df_history.withColumn("AUX_VTG", F.to_date(F.lit(vintage)))
            df_final = df_history.filter(df_history.tfrom_date.between(vintage_ranges[k][0],vintage_ranges[k][1]))\
            .where(F.datediff(F.col(name_vintage), F.col("AUX_VTG")) == 0)
            df_final = df_final.withColumn("vintage_date", F.lit(vintage)).withColumn("tfrom_ini", F.lit(vintage_ranges[k][1]))\
            .withColumn("tfrom", F.months_between(F.col("tfrom_ini"), F.col("tfrom_date"))).drop("tfrom_ini","AUX_VTG")
            self._write_parquet(df_final, save_path, table_name+ "_" + vintage.strftime("%Y%m"))
            path_aux_list.append(os.path.join(save_path, table_name+ "_" + vintage.strftime("%Y%m")))
        return path_aux_list

    def _join_full_cohorts(self, save_path, path_aux_list, table_name, name_vintage_pivote, sqlContext) -> Any:
        """
        Create the full cohort parquet

        Parameters
        ----------
        save_path: str
            Path were the results are saved
        path_aux_list: list
            List of path of all cohorts in string format
        table_name: str
            Name of the table used
        name_vintage_pivote: str
            Name of vintage column in pivot
        sqlContext: pyspark sqlContext
            Your sqlContext

        Returns
        ------
        String
            Path were final cohort was writed

        """
        df_final = sqlContext.read.parquet(*path_aux_list)
        if name_vintage_pivote in df_final.columns:
            df_final = df_final.drop(name_vintage_pivote)
        self._write_parquet(df_final, save_path, table_name + "_historic")
        return(os.path.join(save_path, table_name + "_historic"))

    def _append_pandas(self,*args) -> Any | DataFrame:
        """
        Function to append pandas for controls

        Parameters
        ----------
        args: pandas dataframe
            Pandas we want to append

        Returns
        -------
        All dataframes appended
        """
        df = args[0]
        if len(args)>0:
            for k in range(1,len(args)):
                df = pd.concat([df,args[k]],ignore_index=True)
        return df

    def _controls_output(self,path_output,sqlContext) -> Any:
        """
        Create controls for the output of extract

        Parameters
        ----------
        path: string
            Path were the final output was saved

        Returns
        -------
        Pandas dataframe
            dataframe with the controls of output
        """
        df = sqlContext.read.parquet(path_output)
        controls = df.groupBy("vintage_date","tfrom_date","tfrom").count().orderBy(F.desc("vintage_date"),F.desc("tfrom_date")).toPandas()
        return controls

    def _controls_notnull_output(self,key_pivote,path_output,sqlContext) -> Any:
        """
        Create controls for the output filtering null values.
        It takes a random column of all columns (vintage_date, tfrom_date, tfrom dropped) and filter null values

        Parameters
        ----------
        key_pivote: string
            Key in pivot
        path_output: string
            Path were the final output was saved
        sqlContext: spark sqlContext
            Your sqlContext

        Returns
        -------
        Pandas dataframe
            dataframe with the controls of output with nonnull values
        """
        df = sqlContext.read.parquet(path_output)
        columns = df.drop(key_pivote,"vintage_date", "tfrom_date", "tfrom").columns
        k = randint(0,len(columns)-1)
        controls = df.where(df[columns[k]].isNotNull()).groupBy("vintage_date","tfrom_date","tfrom").count().orderBy(F.desc
        ("vintage_date"),F.desc("tfrom_date")).toPandas()
        return controls

    def create_html_controls(self, message_start,message_style,message_end, table_id = "default") -> Any:
        """
        Create html with the pandas for sending an email with the controls

        Parameters
        ----------
        message_start: string
            Innitial comment for title in the email
        message_syle: string
            Should contain the style you want to use for tables
        message_end: string
            Contains the final comment and the <\body>
        table_id: string
            Style in message_style you want to use, it has default value

        Returns
        -------
        String
            Contains the full html code for sending an email
        """
        mess_final = message_start + message_style
        for k in range(len(self.tables_in)):
            mess_final = mess_final + f"<br> Table: {self.tables_in[k]} <br>" + "<br> Input: <br>" + self.controls_in[k].to_html(index=False,table_id = table_id) +\
            "<br> Output: <br>" + self.controls_out[k].to_html(index=False,table_id = table_id) + "<br> Output (nulls filtered): <br>" +\
            self.controls_out_f[k].to_html(index=False,table_id = table_id)
        mess_final = mess_final + message_end
        return mess_final

    def _create_month_cohort(self, master_list, df_pivot, name_mis, key_pivote, key_source, save_path) -> tuple[Any, DataFrameLike | Any]:
        """
        Create the month cohorts for all the information

        Parameters
        ----------
        master_list: list
            This list has elements in format [query,table_name,date], all used for the final query and filters
        df_pivot: pyspark dataframe
            Dataframe with pivot
        name_mis: string
            Name of date column in table
        key_pivote: string
            Name of key in pivot
        key_source: string
            Name of key in the source
        save_path: string
            Path to save

        Returns
        -------
        Tuple
            A tuple of the form (path_parquet,controls), path_parquet contains the path were parquet was saved and
            controls are the input controls for that month.

        """
        query = master_list[0]
        table_name = master_list[1]
        date = master_list[2]
        df = self.sqlContext.sql(query)
        match = re.search(r'\((.*?)\)', name_mis)
        if match:
            name_mis = match.group(1)
        df = df.withColumn(name_mis, F.col(name_mis).cast("string"))
        if self.source in self.special_treatment_sources:
            df = self._special_treatment(df, table_name, date, query).run()
        #
        controls = df.groupBy(name_mis).count().toPandas()
        if key_pivote == "numcliente":
            df_temp = df_pivot.join(df, [key_source], "left")
        else:
            df_temp = df_pivot.join(df,(df_pivot[key_pivote] == df[key_source]),"left")
        if "numcliente" in df.columns and "numcliente" in df_pivot.columns:
            df_temp = df_temp.drop(df.numcliente)
        df_temp = df_temp.withColumn("tfrom_date", F.lit(date))
        self._write_parquet(df_temp, save_path, table_name)
        return (os.path.join(save_path, table_name), controls)

    def collect_history(self) -> tuple[()]:
        """
        Function that creates the collect history for a source (sources_cat) and for certain pivot in the specified path.

        """
        pivot_table = self.pivot_table
        sources_cat = self.sources_cat
        sqlContext = self.sqlContext
        n_mis = self.n_mis
        save_path = os.path.join(self.working_path, self.name)
        for k in range(len(sources_cat["table_name"])):
            paths_delete = []
            #Getting constants of dict:
            libname = sources_cat["libname"][k]
            table_name = sources_cat["table_name"][k]
            self.tables_in.append(table_name)
            select_variables = sources_cat["select_variables"][k]
            if "type_date" in sources_cat.keys():
                type_date = sources_cat["type_date"][k]
            else:
                type_date = ""
            retraso = sources_cat["retraso"][k]
            name_mis = sources_cat["name_mis"][k]
            table_pivote = sources_cat["table_pivote"][k]
            key_pivote = sources_cat["key_pivote"][k]
            name_vintage_pivote = sources_cat["name_vintage_pivote"][k]
            key_source = sources_cat["key_source"][k]
            #List of dates and queries
            vintages_dates = self._get_vintages_dates(pivot = pivot_table, name_vintage_date = name_vintage_pivote,\
                        table_key = table_pivote, sqlContext = sqlContext)
            list_dates = self._range_dates(vintages = vintages_dates, retraso = retraso, n_mis = n_mis)
            vintage_ranges = self._range_vintages(vintages = vintages_dates, retraso = retraso, n_mis = n_mis)
            queries, tables = self._create_queries(list_dates = list_dates, table_name = table_name,\
                        database = libname, type_date = type_date,\
                        name_mis = name_mis, select_variables = select_variables)
            if key_pivote == "numcliente":
                df_pivot = pivot_table.select(key_pivote, name_vintage_pivote).distinct()
            else:
                df_pivot = pivot_table.select(key_pivote, name_vintage_pivote,"numcliente").distinct()
            df_pivot.cache()
            paths = []
            controls_input = []
            thread_pool = ThreadPool(self.number_threads)
            lista_aux = []
            master_lists = [[queries[k], tables[k], list_dates[k]] for k in range(len(queries))]
            fun_month_cohorts = partial(self._create_month_cohort, df_pivot = df_pivot, name_mis = name_mis, key_pivote = key_pivote,\
                        key_source = key_source, save_path = save_path)
            lista_aux.append(thread_pool.map(fun_month_cohorts, master_lists))
            self.controls_in.append(self._append_pandas(*[tuple_aux[1] for tuple_aux in lista_aux[0]]))
            df_full = sqlContext.read.parquet(*[tuple_aux[0] for tuple_aux in lista_aux[0]])
            self._write_parquet(df_full,save_path, table_name + "_full")
            paths_delete += [tuple_aux[0] for tuple_aux in lista_aux[0]]
            path_aux_list = self._create_cohorts(vintages_dates = vintages_dates, vintage_ranges = vintage_ranges, key_pivote =
            key_pivote,\
                        save_path = save_path, name_vintage = name_vintage_pivote, table_name = table_name,\
                        pivot = pivot_table, sqlContext = sqlContext)
            paths_delete.append(os.path.join(save_path, table_name + "_full"))
            path_output = self._join_full_cohorts(save_path = save_path, path_aux_list = path_aux_list, table_name = table_name,
            name_vintage_pivote = name_vintage_pivote, sqlContext = sqlContext)
            paths_delete += path_aux_list
            fun_delete = partial(self._delete_intermediate)
            thread_pool.map(fun_delete, paths_delete)
            self.controls_out.append(self._controls_output(path_output = path_output, sqlContext = sqlContext))
            self.controls_out_f.append(self._controls_notnull_output(key_pivote = key_pivote, path_output = path_output,sqlContext =
            sqlContext))
        thread_pool.close()
        df_pivot.unpersist()
        return()
