"""
Project: Extract (Time Collection Refactorization) (PEP 8 & Python3.6)
Developer: Brayan Borquez
Update Date: 2022-12-22
Version: 1.1

Script con la clase y funciones del extract del datapull: lee las tablas
fuente de Hive por mes, las cruza con el pivot de clientes y genera los
parquets ``<table>_historic`` (cohortes por vintage) junto a los controles
de entrada/salida que se envían por correo.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
try:
    from pyspark.sql.pandas.types import DataFrameLike
except ImportError:
    DataFrameLike = DataFrame
import pandas as pd
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from random import randint
from multiprocessing.pool import ThreadPool
from cytoolz.functoolz import partial
import os
import re

from .extract_special_treatments.default import ExtractSpecialTreatment
from .extract_special_treatments.payroll_streaks import PayrollStreaksExtractSpecialTreatment
from .steps import path_exists, path_delete

class Extract():
    #: Fuentes a las que se aplica tratamiento especial en la extracción.
    special_treatment_sources = [
        "payroll_streaks", "payroll_income"
    ]
    """Clase usada para el extract en datapull.

    Parameters
    ----------
    pivot_table : DataFrame
        Dataframe del pivot (cliente x vintage).
    working_path : str
        Path donde guardar las tablas.
    sources_cat : Dict[str, Any]
        Todas las constantes e info de las fuentes a extraer (tablas,
        libname, select_variables, retraso, name_mis, keys...).
    source : str
        Nombre de la fuente.
    n_mis : int
        Meses de información a extraer.
    sqlContext : SparkSession
        Tu sqlContext.
    number_threads : int
        Número de hilos para el paralelismo; por defecto 32.
    mode : str
        Modo de escritura de parquets; por defecto y recomendado
        ``overwrite``.
    name : str
        Nombre con el que guardar el paso; por defecto ``extract``.
    """
    FORMAT_TIME = "%Y-%m-%d"

    def __init__(self, pivot_table: DataFrame, working_path: str, n_mis: int,
                 sources_cat: Dict[str, Any], source: str,
                 sqlContext: SparkSession, number_threads: int = 32,
                 mode: str = "overwrite", name: str = "extract") -> None:
        self.pivot_table: DataFrame = pivot_table
        self.working_path: str = working_path
        self.sources_cat: Dict[str, Any] = sources_cat
        self.source: str = source
        self.sqlContext: SparkSession = sqlContext
        self.mode: str = mode
        self.n_mis: int = n_mis
        self.number_threads: int = number_threads
        self.tables_in: List[str] = []
        self.controls_in: List[pd.DataFrame] = []
        self.controls_out: List[pd.DataFrame] = []
        self.controls_out_f: List[pd.DataFrame] = []
        self.name: str = name

    def _create_queries(self, list_dates: List[str], table_name: str,
                        database: str, type_date: str, name_mis: str,
                        select_variables: str) -> Tuple[List[str], List[str]]:
        """Construye las queries del extract.

        Parameters
        ----------
        list_dates : List[str]
            Lista con todas las fechas de las que se quiere información.
        table_name : str
            Nombre de la tabla fuente.
        database : str
            Nombre de la base de datos donde está la tabla.
        type_date : str
            Tipo de fecha si la info viene en tablas mensuales
            (``YYYYMM``, ``YYYYM``, ``YYMM``, ``mmmYY``, ``MM_YYYY``); si
            la tabla está particionada debe ser ``""``.
        name_mis : str
            Nombre de la columna de fecha en la tabla particionada.
        select_variables : str
            Columnas a seleccionar en el SELECT.

        Returns
        -------
        Tuple[List[str], List[str]]
            ``(queries, tables)``: todas las queries del extract y los
            nombres de las tablas usadas en ellas.
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
            # type_date -> formato del sufijo de tabla para una fecha
            # 'YYYY-MM-DD'
            date_formats = {
                "YYYYMM": lambda d: d[0:4] + d[5:7],
                "YYYYM": lambda d: d[0:4] + str(int(d[5:7])),
                "YYMM": lambda d: d[2:4] + d[5:7],
                "mmmYY": lambda d: meses[int(d[5:7])] + d[0:4],
                "MM_YYYY": lambda d: d[5:7] + "_" + d[0:4],
            }
            tables = [table_name.replace("XXXX", date_formats[type_date](list_dates[k]))
                      for k in range(n)]
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

    def _special_treatment(self, input_df: DataFrame, table_name: str,
                           date: str, query: str) -> "ExtractSpecialTreatment":
        """Aplica un tratamiento especial a la tabla tras el filtro de fecha
        y el select.

        Parameters
        ----------
        input_df : DataFrame
            DataFrame con los datos de entrada.
        table_name : str
            Nombre de la tabla fuente.
        date : str
            Fecha de la que se quiere información.
        query : str
            Query usada en la extracción.

        Returns
        -------
        ExtractSpecialTreatment
            Instancia de ``ExtractSpecialTreatment`` o de su subclase
            según la fuente.
        """
        if self.source in ["payroll_streaks", "payroll_income"]:
            special_treatment_object = PayrollStreaksExtractSpecialTreatment
        else:
            special_treatment_object = ExtractSpecialTreatment
        return special_treatment_object(parent=self, input_df=input_df, table_name=table_name, date=date, query=query,
        sqlContext=self.sqlContext)

    def _get_vintages_dates(self, pivot: DataFrame, name_vintage_date: str,
                            table_key: str,
                            sqlContext: SparkSession) -> List[Any]:
        """Obtiene las fechas de vintage distintas del pivot.

        Parameters
        ----------
        pivot : DataFrame
            Dataframe con los clientes y su fecha de vintage.
        name_vintage_date : str
            Nombre de la columna que usamos como vintage en el pivot.
        table_key : str
            Clave de la vista temporal, normalmente ``"master_data"``.
        sqlContext : SparkSession
            Tu sqlContext.

        Returns
        -------
        List[Any]
            Todas las fechas de vintage del pivot (orden descendente).
        """
        df_pivot = pivot
        df_pivot.createOrReplaceTempView("pivot_temporal_"+table_key)
        vintages = sqlContext.sql(f"select distinct {name_vintage_date} from pivot_temporal_"+table_key).collect()
        vintages = [vintages[p][0] for p in range(len(vintages))]
        vintages.sort(reverse = True)
        return vintages

    def _range_dates(self, vintages: List[Any], retraso: int,
                     n_mis: int) -> List[str]:
        """Construye el rango de fechas de todos los datos.

        Parameters
        ----------
        vintages : list
            Lista con todos los vintages en datetime.
        retraso : int
            Número de meses de lag.
        n_mis : int
            Número de meses de información a obtener.

        Returns
        -------
        List[str]
            Lista con el rango completo de fechas (cada fin de mes).
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

    def _range_vintages(self, vintages: List[Any], retraso: int,
                        n_mis: int) -> List[Tuple[str, str]]:
        """Construye el rango de fechas por vintage.

        Parameters
        ----------
        vintages : list
            Lista con todos los vintages en datetime.
        retraso : int
            Número de meses de lag.
        n_mis : int
            Número de meses de información a obtener.

        Returns
        -------
        List[Tuple[str, str]]
            Lista de tuplas ``(min_date, max_date)`` por vintage.
        """
        range_vin = []
        for vintage in vintages:
            max_date = vintage + relativedelta(days = 1) - relativedelta(months = retraso, days= 1)
            min_date = vintage + relativedelta(days = 1) - relativedelta(months = retraso + n_mis - 1, days = 1)
            range_vin.append((min_date.strftime(self.FORMAT_TIME), max_date.strftime(self.FORMAT_TIME)))
        return range_vin

    def _write_parquet(self, df: DataFrame, path: str,
                       file_name: str) -> Tuple[()]:
        """Escribe un parquet.

        Parameters
        ----------
        df : DataFrame
            Dataframe a escribir.
        path : str
            Path donde se va a escribir el parquet.
        file_name : str
            Nombre del parquet.
        """
        df.write.mode(self.mode).parquet(os.path.join(path,file_name))
        return()

    def _evaluate_path(self, path: str) -> bool:
        """Evalúa si ``path`` existe (ver :func:`libs.steps.path_exists`)."""
        return path_exists(path)

    def _delete_intermediate(self, path: str) -> None:
        """Borra ``path`` si existe (ver :func:`libs.steps.path_delete`)."""
        path_delete(path)

    def _create_cohorts(self, vintages_dates: List[Any],
                        vintage_ranges: List[Tuple[str, str]],
                        key_pivote: str, save_path: str, name_vintage: str,
                        table_name: str, pivot: DataFrame,
                        sqlContext: SparkSession) -> List[str]:
        """Crea las cohortes por cada vintage.

        Parameters
        ----------
        vintages_dates : list
            Todas las fechas de vintage del pivot.
        vintage_ranges : list
            Lista de tuplas ``(min_date, max_date)`` del rango de cada
            vintage.
        key_pivote : str
            Clave primaria de la tabla pivot.
        save_path : str
            Path donde se guardan los resultados.
        name_vintage : str
            Nombre del vintage en el pivot.
        table_name : str
            Nombre de la tabla de la fuente.
        pivot : DataFrame
            Tabla pivot.
        sqlContext : SparkSession
            Tu sqlContext.

        Returns
        -------
        List[str]
            Paths con todas las cohortes creadas
            (``<table>_<YYYYMM>`` por vintage).
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

    def _join_full_cohorts(self, save_path: str, path_aux_list: List[str],
                           table_name: str, name_vintage_pivote: str,
                           sqlContext: SparkSession) -> str:
        """Crea el parquet ``<table>_historic`` uniendo todas las cohortes.

        Parameters
        ----------
        save_path : str
            Path donde se guardan los resultados.
        path_aux_list : List[str]
            Lista de paths de todas las cohortes en formato string.
        table_name : str
            Nombre de la tabla usada.
        name_vintage_pivote : str
            Nombre de la columna de vintage en el pivot.
        sqlContext : SparkSession
            Tu sqlContext.

        Returns
        -------
        str
            Path donde se escribió la cohorte final.
        """
        df_final = sqlContext.read.parquet(*path_aux_list)
        if name_vintage_pivote in df_final.columns:
            df_final = df_final.drop(name_vintage_pivote)
        self._write_parquet(df_final, save_path, table_name + "_historic")
        return(os.path.join(save_path, table_name + "_historic"))

    def _append_pandas(self, *args: pd.DataFrame) -> pd.DataFrame:
        """Concatena dataframes pandas para los controles.

        Parameters
        ----------
        *args : pd.DataFrame
            Dataframes a concatenar.

        Returns
        -------
        pd.DataFrame
            Todos los dataframes concatenados.
        """
        df = args[0]
        if len(args)>0:
            for k in range(1,len(args)):
                df = pd.concat([df,args[k]],ignore_index=True)
        return df

    def _controls_output(self, path_output: str,
                         sqlContext: SparkSession) -> pd.DataFrame:
        """Crea los controles de salida del extract.

        Parameters
        ----------
        path_output : str
            Path donde se guardó la salida final.
        sqlContext : SparkSession
            Tu sqlContext.

        Returns
        -------
        pd.DataFrame
            Dataframe con los controles de salida (conteo por
            ``vintage_date``/``tfrom_date``/``tfrom``).
        """
        df = sqlContext.read.parquet(path_output)
        controls = df.groupBy("vintage_date","tfrom_date","tfrom").count().orderBy(F.desc("vintage_date"),F.desc("tfrom_date")).toPandas()
        return controls

    def _controls_notnull_output(self, key_pivote: str, path_output: str,
                                 sqlContext: SparkSession) -> pd.DataFrame:
        """Controles de salida filtrando valores nulos.

        Toma una columna aleatoria de todas las columnas (quitando
        ``vintage_date``, ``tfrom_date``, ``tfrom``) y filtra los nulos.

        Parameters
        ----------
        key_pivote : str
            Clave del pivot.
        path_output : str
            Path donde se guardó la salida final.
        sqlContext : SparkSession
            Tu sqlContext.

        Returns
        -------
        pd.DataFrame
            Dataframe con los controles de salida sin nulos.
        """
        df = sqlContext.read.parquet(path_output)
        columns = df.drop(key_pivote,"vintage_date", "tfrom_date", "tfrom").columns
        k = randint(0,len(columns)-1)
        controls = df.where(df[columns[k]].isNotNull()).groupBy("vintage_date","tfrom_date","tfrom").count().orderBy(F.desc
        ("vintage_date"),F.desc("tfrom_date")).toPandas()
        return controls

    def create_html_controls(self, message_start: str, message_style: str,
                             message_end: str,
                             table_id: str = "default") -> str:
        """Crea el HTML con los pandas para enviar un correo con los
        controles.

        Parameters
        ----------
        message_start : str
            Comentario inicial para el título del correo.
        message_style : str
            Debe contener el estilo a usar para las tablas.
        message_end : str
            Contiene el comentario final y el ``</body>``.
        table_id : str
            Estilo de ``message_style`` a usar; tiene valor por defecto.

        Returns
        -------
        str
            El código HTML completo para enviar un correo.
        """
        mess_final = message_start + message_style
        for k in range(len(self.tables_in)):
            mess_final = mess_final + f"<br> Table: {self.tables_in[k]} <br>" + "<br> Input: <br>" + self.controls_in[k].to_html(index=False,table_id = table_id) +\
            "<br> Output: <br>" + self.controls_out[k].to_html(index=False,table_id = table_id) + "<br> Output (nulls filtered): <br>" +\
            self.controls_out_f[k].to_html(index=False,table_id = table_id)
        mess_final = mess_final + message_end
        return mess_final

    def _create_month_cohort(self, master_list: List[Any], df_pivot: DataFrame,
                             name_mis: str, key_pivote: str, key_source: str,
                             save_path: str) -> Tuple[str, pd.DataFrame]:
        """Crea las cohortes mensuales de toda la información.

        Parameters
        ----------
        master_list : list
            Lista con elementos ``[query, table_name, date]``, usados en la
            query final y los filtros.
        df_pivot : DataFrame
            Dataframe con el pivot.
        name_mis : str
            Nombre de la columna de fecha en la tabla.
        key_pivote : str
            Nombre de la clave en el pivot.
        key_source : str
            Nombre de la clave en la fuente.
        save_path : str
            Path donde guardar.

        Returns
        -------
        Tuple[str, pd.DataFrame]
            Tupla ``(path_parquet, controls)``: el path donde se guardó el
            parquet del mes y sus controles de entrada.
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

    def collect_history(self) -> Tuple[()]:
        """Crea el collect history de una fuente (``sources_cat``) para el
        pivot en el path especificado.

        Para cada tabla de la fuente: genera las queries mensuales, corre
        :meth:`_create_month_cohort` en un ``ThreadPool``, une todo en
        ``<table>_full``, crea las cohortes por vintage
        (``<table>_historic``), borra los intermedios y calcula los
        controles de entrada/salida.
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
