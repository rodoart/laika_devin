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
import logging
import os
import re

from .extract_special_treatments.default import ExtractSpecialTreatment
from .extract_special_treatments.payroll_streaks import PayrollStreaksExtractSpecialTreatment
from .extract_special_treatments.adobe import AdobeExtractSpecialTreatment
from .steps import path_delete, path_exists, save_parquet
from .hive_path import list_files, mkdir, touch
from .weeks import retag_tfrom_columns, tfrom_window_bounds

class Extract():
    #: Fuentes a las que se aplica tratamiento especial en la extracción.
    special_treatment_sources = [
        "payroll_streaks", "payroll_income",
        "adobe_bancanet", "adobe_mobile"
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
    is_dynamic : bool
        ``True`` -> extract reanudable: cada parquet intermedio ya
        escrito (mes ``<table>_YYYYMM``, ``<table>_full``, cohorte de
        vintage ``<table>_vintage_YYYYMM`` y ``<table>_historic``) se
        recarga en vez de recomputarse. ``False`` -> recomputa todo
        (comportamiento del pipeline legado).
    """
    FORMAT_TIME = "%Y-%m-%d"

    def __init__(self, pivot_table: DataFrame, working_path: str, n_mis: int,
                 sources_cat: Dict[str, Any], source: str,
                 sqlContext: SparkSession, number_threads: int = 32,
                 mode: str = "overwrite", name: str = "extract",
                 is_dynamic: bool = False,
                 weeks_dict: Optional[Dict[str, Any]] = None) -> None:
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
        self.is_dynamic: bool = is_dynamic
        # Config del antiguo paso ``weeks`` integrada en el extract: si
        # está presente, el ``<table>_historic`` sale ya con el tfrom
        # semanal/diario (particiones ``vintage_date/tfrom`` finales)
        self.weeks_dict: Dict[str, Any] = weeks_dict or {}

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
        elif self.source in ["adobe_bancanet", "adobe_mobile"]:
            special_treatment_object = AdobeExtractSpecialTreatment
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

    def _weeks_table_config(self, k: int) -> Optional[Dict[str, Any]]:
        """Config de reetiquetado semanal/diario para la tabla ``k``.

        Devuelve ``None`` si la fuente no tiene bloque ``weeks`` o la
        tabla no tiene ``date_column_for_weekly_tfrom`` (esa tabla
        mantiene el tfrom mensual).
        """
        try:
            date_col = self.weeks_dict["date_column_for_weekly_tfrom"][k]
            type_of_date = self.weeks_dict["type_of_date_column"][k]
        except (KeyError, IndexError, TypeError):
            return None
        method = self.weeks_dict.get("tfrom_method", "standard")
        if isinstance(method, list):
            method = method[k]
        formats = self.weeks_dict.get("sql_date_format") or []
        return {
            "date_col": date_col,
            "type_of_date": type_of_date,
            "method": method,
            "date_format": formats[k]
            if k < len(formats) and formats[k] else "yyyy-MM-dd",
            # ``drop_columns_at_the_end`` existe en la config pero el
            # viejo WeeklyTFrom nunca se lo pasaba al retagger (default
            # []) — se ignora para conservar los outputs
            "drop_cols": [],
        }

    def _join_month_pivot(self, df: DataFrame, df_pivot: DataFrame,
                          key_pivote: str, key_source: str) -> DataFrame:
        """Join ``df_pivot`` (left) con los datos del mes.

        Misma lógica que tenía el cohorte mensual: join por lista de
        columnas cuando la clave es ``numcliente`` y por expresión en
        otro caso (eliminando la clave duplicada cuando
        ``key_source == key_pivote``).
        """
        if key_pivote == "numcliente":
            df_temp = df_pivot.join(df, [key_source], "left")
        else:
            df_temp = df_pivot.join(df,(df_pivot[key_pivote] == df[key_source]),"left")
            # join por expresión conserva la clave de AMBOS lados; con
            # key_source == key_pivote (p.ej. crd_acct_nbr en
            # rev_balance) la columna queda duplicada y el write falla
            if key_source == key_pivote:
                df_temp = df_temp.drop(df[key_source])
        if "numcliente" in df.columns and "numcliente" in df_pivot.columns:
            df_temp = df_temp.drop(df.numcliente)
        return df_temp

    def _max_tfrom_date(self, df: DataFrame, df_pivot: DataFrame,
                        key_pivote: str, key_source: str,
                        name_vintage_pivote: str, retraso: int,
                        date: str, wk: Dict[str, Any]) -> Any:
        """Max de ``wk["date_col"]`` del mes — bounds de la ventana semanal.

        Equivalente a ``_month_base(_join_month_pivot(df, df_pivot,
        ...)).agg(max(to_date(date_col)))`` pero mucho más barato:

        - Solo se proyectan ``key_source`` + la fecha del lado datos:
          la poda de columnas de Catalyst recorta el scan del mes a 2
          columnas (la tabla adobe de hits es muy ancha).
        - ``inner`` en vez de ``left``: para un ``max`` las filas del
          pivot sin match (fecha ``null``) no ganan nunca y las filas de
          datos fuera del pivot se descartan igual en ambos joins.
        - El filtro de ventana por vintage replica exactamente el
          ``between`` de ``_month_base`` (``lit(date)`` es la constante
          del bucket mensual comparada contra los bounds por fila).
        """
        dat = df.select(
            F.col(key_source).alias("__k"),
            F.to_date(F.col(wk["date_col"]), wk["date_format"])
                .alias("__d"))
        piv = df_pivot.select(
            F.col(key_pivote).alias("__k"),
            F.col(name_vintage_pivote).alias("__v")).distinct()
        vintage = F.to_date(F.col("__v"))
        tfrom_ini = F.date_sub(
            F.add_months(F.date_add(vintage, 1), -retraso), 1)
        tfrom_min = F.date_sub(
            F.add_months(F.date_add(vintage, 1),
                         -(retraso + self.n_mis - 1)), 1)
        return (dat.join(piv, "__k", "inner")
                   .filter(F.to_date(F.lit(date))
                           .between(tfrom_min, tfrom_ini))
                   .agg(F.max("__d"))
                   .collect()[0][0])

    def _month_base(self, df_temp: DataFrame, date: str,
                    name_vintage_pivote: str, retraso: int) -> DataFrame:
        """Filas del mes ya acotadas al rango temporal de su vintage.

        ``df_temp`` lleva ya el join con el pivot. ``tfrom_date`` es el
        bucket mensual (fin de mes) y el filtro ``between`` equivale al
        antiguo cohorte por vintage, pero evaluado por fila (cada fila
        usa su propio vintage). Además asigna ``vintage_date``.
        """
        vintage = F.to_date(F.col(name_vintage_pivote))
        tfrom_ini = F.date_sub(
            F.add_months(F.date_add(vintage, 1), -retraso), 1)
        tfrom_min = F.date_sub(
            F.add_months(F.date_add(vintage, 1),
                         -(retraso + self.n_mis - 1)), 1)
        return (df_temp
                .withColumn("tfrom_date", F.to_date(F.lit(date)))
                .filter(F.col("tfrom_date").between(tfrom_min, tfrom_ini))
                .withColumn("vintage_date", F.col(name_vintage_pivote)))

    def _apply_tfrom(self, base: DataFrame, name_vintage_pivote: str,
                     retraso: int, wk: Optional[Dict[str, Any]],
                     bounds: Optional[Tuple[Any, Any]]) -> DataFrame:
        """Calcula ``tfrom``/``tfrom_date`` finales sobre ``base``.

        Sin ``wk``: tfrom mensual (``months_between`` al fin del rango
        del vintage). Con ``wk``: el reetiquetado semanal/diario del
        antiguo paso ``weeks``, incluido el filtro global de ventana
        (``bounds``), anclado a ``last_day(vintage_date)``.
        """
        if wk is None:
            vintage = F.to_date(F.col(name_vintage_pivote))
            tfrom_ini = F.date_sub(
                F.add_months(F.date_add(vintage, 1), -retraso), 1)
            result = base.withColumn(
                "tfrom", F.months_between(tfrom_ini, F.col("tfrom_date")))
        else:
            # el retagger calculaba el tfrom sobre la fecha del fichero
            # (tfrom_date = to_date(date_col)), no sobre el bucket
            result = base.withColumn(
                "tfrom_date",
                F.to_date(F.col(wk["date_col"]), wk["date_format"]))
            if bounds is not None:
                result = result.filter(
                    (F.col("tfrom_date") >= F.lit(str(bounds[0])))
                    & (F.col("tfrom_date") < F.lit(str(bounds[1]))))
            result = retag_tfrom_columns(
                result, wk["date_col"], wk["method"], wk["type_of_date"],
                wk["date_format"], wk["drop_cols"])
        if name_vintage_pivote in result.columns \
                and name_vintage_pivote != "vintage_date":
            result = result.drop(name_vintage_pivote)
        return result

    def _write_month_partitions(self, df: DataFrame,
                                historic_path: str) -> None:
        """Escribe un mes en ``<table>_historic`` con overwrite dinámico.

        ``partitionOverwriteMode=dynamic`` hace el write idempotente:
        solo se reemplazan las particiones ``vintage_date/tfrom``
        presentes en el df, así un kill a mitad de mes deja particiones
        parciales que la reanudación limpia al reescribir el mismo
        conjunto. Los meses vacíos no escriben nada.
        """
        if not df.head(1):
            return
        # un kill previo puede dejar staging ``_temporary`` del commit
        # abortado — se ignora al leer pero ocupa espacio
        staging = os.path.join(historic_path, "_temporary")
        if path_exists(staging):
            path_delete(staging)
        conf = "spark.sql.sources.partitionOverwriteMode"
        self.sqlContext.conf.set(conf, "dynamic")
        try:
            df.write.mode("overwrite") \
                .partitionBy("vintage_date", "tfrom").parquet(historic_path)
        finally:
            self.sqlContext.conf.set(conf, "static")

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

    def _run_month_query(self, query: str, name_mis: str, table_name: str,
                         date: str) -> Tuple[DataFrame, DataFrame]:
        """Ejecuta la query de un mes + tratamiento especial + controles.

        Returns
        -------
        Tuple[DataFrame, DataFrame]
            ``(df_mes, controls_spark)``: el df mensual (con
            ``name_mis`` como string y el ``ExtractSpecialTreatment``
            aplicado) y el ``groupBy(name_mis).count()`` de controles
            de entrada (Spark df minúsculo, listo para persistir).
        """
        match = re.search(r'\((.*?)\)', name_mis)
        if match:
            name_mis = match.group(1)
        df = self.sqlContext.sql(query)
        df = df.withColumn(name_mis, F.col(name_mis).cast("string"))
        if self.source in self.special_treatment_sources:
            df = self._special_treatment(df, table_name, date, query).run()
        return df, df.groupBy(name_mis).count()

    def _read_controls_sidecar(self, ctrl_path: str,
                               sqlContext: SparkSession) -> pd.DataFrame:
        """Recarga el parquet de controles de un mes ya procesado."""
        try:
            return sqlContext.read.parquet(ctrl_path).toPandas()
        except Exception:
            return pd.DataFrame()

    def collect_history(self) -> Tuple[()]:
        """Crea el collect history de una fuente (``sources_cat``) para el
        pivot en el path especificado.

        Por cada tabla corre las queries mensuales y escribe **directo**
        en las particiones ``vintage_date/tfrom`` de
        ``<table>_historic`` (overwrite dinámico) — sin dirs
        ``<table>_YYYYMM``, ``<table>_vintage_*`` ni ``_full``. Si la
        fuente tiene bloque ``weeks``, el tfrom semanal/diario y la
        ventana global ``filtered`` se calculan aquí mismo (la etapa
        ``weeks`` queda como no-op).

        Reanudación: marcadores ``_meta/<table>/<YYYYMM>.done`` por mes
        (con ``<YYYYMM>.ctrl`` = controles de entrada persistidos) y
        ``_meta/<table>/_DONE`` cuando la tabla está completa.
        """
        pivot_table = self.pivot_table
        sources_cat = self.sources_cat
        sqlContext = self.sqlContext
        n_mis = self.n_mis
        save_path = os.path.join(self.working_path, self.name)
        for k in range(len(sources_cat["table_name"])):
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
            historic_path = os.path.join(save_path, table_name + "_historic")
            meta_dir = os.path.join(save_path, "_meta", table_name)
            done_flag = os.path.join(meta_dir, "_DONE")
            wk = self._weeks_table_config(k)
            # Reanudación a nivel tabla: ``_DONE`` -> se recarga entera;
            # solo se recalculan los controles para el correo
            if self.is_dynamic and path_exists(done_flag):
                logging.info(f"Table {table_name} historic already written, reloading")
                try:
                    ctrl_dirs = [os.path.join(meta_dir, f)
                                 for f in sorted(list_files(meta_dir))
                                 if f.endswith(".ctrl")]
                    self.controls_in.append(
                        sqlContext.read.parquet(*ctrl_dirs).toPandas()
                        if ctrl_dirs else pd.DataFrame())
                except Exception:
                    self.controls_in.append(pd.DataFrame())
                self.controls_out.append(self._controls_output(path_output = historic_path, sqlContext = sqlContext))
                self.controls_out_f.append(self._controls_notnull_output(key_pivote = key_pivote, path_output = historic_path,sqlContext =
                sqlContext))
                continue
            #List of dates and queries
            vintages_dates = self._get_vintages_dates(pivot = pivot_table, name_vintage_date = name_vintage_pivote,\
                        table_key = table_pivote, sqlContext = sqlContext)
            list_dates = self._range_dates(vintages = vintages_dates, retraso = retraso, n_mis = n_mis)
            queries, tables = self._create_queries(list_dates = list_dates, table_name = table_name,\
                        database = libname, type_date = type_date,\
                        name_mis = name_mis, select_variables = select_variables)
            if key_pivote == "numcliente":
                df_pivot = pivot_table.select(key_pivote, name_vintage_pivote).distinct()
            else:
                df_pivot = pivot_table.select(key_pivote, name_vintage_pivote,"numcliente").distinct()
            df_pivot.cache()
            mkdir(meta_dir, "-p")
            controls_input = []
            master_lists = [[queries[j], tables[j], list_dates[j]]
                            for j in range(len(queries))]
            month_markers = [os.path.join(meta_dir, t + ".done")
                             for t in tables]
            done_flags = [self.is_dynamic and path_exists(m)
                          for m in month_markers]
            # Paridad con el overwrite total antiguo: una corrida no
            # dinámica (o una sin marcadores mensuales — restos de runs
            # previos) arranca con el _historic limpio para que no
            # queden particiones de datos que ya no se computan
            if path_exists(historic_path) and (
                    not self.is_dynamic or not any(done_flags)):
                path_delete(historic_path)
            # bounds de la ventana semanal: el máximo ``tfrom_date``
            # global de la tabla vive en el mes elegible más reciente con
            # datos -> escaneo descendente hasta el primer mes no vacío.
            # Solo hace falta si queda algo por escribir: con todos los
            # marcadores presentes no se reejecuta ninguna query.
            bounds = None
            if wk is not None and not all(done_flags):
                for master_list in reversed(master_lists):
                    df_b, _ = self._run_month_query(
                        master_list[0], name_mis, master_list[1],
                        master_list[2])
                    max_tfrom_date = self._max_tfrom_date(
                        df_b, df_pivot, key_pivote, key_source,
                        name_vintage_pivote, retraso, master_list[2],
                        wk)
                    if max_tfrom_date is not None:
                        # la ventana usa el n_mis AMPLIADO (+1 weeks):
                        # el viejo WeeksStep también subía n_mis antes
                        # de llamar al retagger
                        bounds = tfrom_window_bounds(
                            max_tfrom_date, wk["method"],
                            wk["type_of_date"], n_mis)
                        break
            # Spill de particiones semanales: una semana a caballo de dos
            # meses agrupa ficheros de DOS buckets distintos en la misma
            # partición (vintage, tfrom) — p.ej. la semana lun 29-sep a
            # dom 5-oct contiene ficheros del 29-30 sep (bucket sep) y
            # del 1-5 oct (bucket oct). Si cada bucket escribiera "sus"
            # particiones, el segundo write clobberaría al primero.
            # Regla: el bucket con fecha lit en el mes M escribe TODAS
            # las particiones cuyo DOMINGO (lunes+6) cae en M — ese
            # bucket tiene garantizados los ficheros del tramo final de
            # la semana; el tramo inicial llega como spill del bucket
            # anterior (sus ficheros cuyo domingo cae en M). El método
            # 'daily' y el tfrom mensual no tienen spill: cada fichero
            # cae en un único bucket.
            needs_spill = wk is not None and wk["method"] != "daily"
            spill_prev = None
            df_template = None
            for j, master_list in enumerate(master_lists):
                query, month_table, month_date = master_list
                marker = month_markers[j]
                ctrl_path = os.path.join(meta_dir, month_table + ".ctrl")
                month_lit = month_date[0:7] + "-01"
                next_lit = master_lists[j + 1][2][0:7] + "-01" \
                    if j + 1 < len(master_lists) else None
                month_done = done_flags[j]
                # Un mes con marker solo se recomputa si el siguiente
                # bucket necesita su spill (sus ficheros de lunes en el
                # mes siguiente)
                spill_needed = needs_spill and next_lit is not None \
                    and not done_flags[j + 1]
                if month_done and not spill_needed:
                    logging.info(
                        f"Month {month_table} already written, reloading")
                    controls_input.append(
                        self._read_controls_sidecar(ctrl_path, sqlContext))
                    spill_prev = None
                    continue
                df, controls = self._run_month_query(
                    query, name_mis, month_table, month_date)
                df_temp = self._join_month_pivot(df, df_pivot, key_pivote,
                                                 key_source)
                base = self._month_base(df_temp, month_date,
                                        name_vintage_pivote, retraso)
                if wk is not None and bounds is None:
                    # ningún mes tuvo fechas válidas -> la ventana global
                    # vacía toda la tabla
                    df_out = self._apply_tfrom(
                        base.limit(0), name_vintage_pivote, retraso, wk,
                        bounds)
                else:
                    df_out = self._apply_tfrom(base, name_vintage_pivote,
                                               retraso, wk, bounds)
                df_template = df_out
                if needs_spill:
                    # mes del domingo de la semana (tfrom_date = lunes)
                    sunday_month = F.trunc(
                        F.date_add(F.col("tfrom_date"), 6), "month")
                    # El último bucket no tiene un "dueño" posterior para
                    # su spill (p.ej. ficheros de oct con domingo en oct
                    # dentro del bucket lit-oct): sus particiones las
                    # escribe él mismo por completo
                    own = df_out if next_lit is None else df_out.filter(
                        sunday_month == F.lit(month_lit))
                    spill = df_out.filter(
                        sunday_month == F.lit(next_lit)) \
                        if next_lit is not None else base.limit(0)
                    df_out = own if spill_prev is None \
                        else own.unionByName(spill_prev)
                    spill_prev = spill
                if month_done:
                    # solo se recomputó para aportar su spill al bucket
                    # siguiente; su escritura ya está hecha
                    controls_input.append(
                        self._read_controls_sidecar(ctrl_path, sqlContext))
                    continue
                controls.write.mode("overwrite").parquet(ctrl_path)
                controls_input.append(controls.toPandas())
                self._write_month_partitions(df_out, historic_path)
                touch(marker)
            # Si ningún mes escribió nada (tabla vacía) el histórico no
            # existe: materializarlo vacío para los controles y los
            # lectores posteriores
            if not path_exists(historic_path):
                if df_template is None:
                    raise RuntimeError(
                        f"extract: marcadores _meta de {table_name} sin "
                        f"historic — estado inconsistente, borra {meta_dir}")
                save_parquet(df_template.limit(0), historic_path, self.mode)
            # los writes con partitionOverwriteMode=dynamic no dejan
            # ``_SUCCESS`` en la raíz (Spark 3.3) — se toca al cerrar la
            # tabla para que las comprobaciones ``is_successful`` sigan
            # funcionando sobre el _historic completo
            touch(os.path.join(historic_path, "_SUCCESS"))
            touch(done_flag)
            self.controls_in.append(self._append_pandas(*controls_input))
            self.controls_out.append(self._controls_output(path_output = historic_path, sqlContext = sqlContext))
            self.controls_out_f.append(self._controls_notnull_output(key_pivote = key_pivote, path_output = historic_path,sqlContext =
            sqlContext))
            df_pivot.unpersist()
        return()
