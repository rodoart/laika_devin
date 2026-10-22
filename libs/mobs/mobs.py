"""
Project: MOBs (Months on Books) (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gio Guerrero, Brayan Borquez, Gil Rodriguez, Jorge Galicia,
    Rodolfo Arturo Gonzalez
Update Date: 2025-02-20
Version: 1.1

Script containing the class and methods for the MOBs calculation and
imputation in datapull.

Historically MOBs lived inside ``libs.missing_treatment.MissingTreatment``;
they are now a separate class/module because they are a different pipeline
concern: the missing treatment fills nulls in the historic tables
(``mt`` stage) while MOBs compute client seniority and impute the
feature-engineering outputs for clients with little history (``mobs``
stage).
"""
import logging
import os
from functools import reduce
from pathlib import PurePosixPath as PosixPath
from typing import Any, Callable, Dict, List, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    count as spark_count,
    greatest,
    isnan,
    lit,
    max,
    max as spark_max,
    months_between,
    sum as spark_sum,
    trunc,
    when,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

from ..hive_path import is_successful
from ..steps import path_delete
from .adobe import PreviousFixerAdobe
from .payroll_income import PreviousFixerPayrollIncome, get_months
from .payroll_streaks import PreviousFixerPayrollStreaks

logger = logging.getLogger(__name__)

#: Imputador de features por MOBs de cada fuente payroll; el resto usa el
#: ``previous_fixer`` genérico de :class:`Mobs`.
_FIXER_BY_SOURCE = {
    "payroll_income": PreviousFixerPayrollIncome,
    "payroll_streaks": PreviousFixerPayrollStreaks,
    # las features adobe son _<N>w (semanas) y mobs_adobe está en meses —
    # el fixer genérico (sufijo _<N>m) no las tocaría
    "adobe_bancanet": PreviousFixerAdobe,
    "adobe_mobile": PreviousFixerAdobe,
}

#: SELECT común de los cálculos de seniority (liabilities / credit_card).
_SENIORITY_QUERY = [
    "bigint(numcliente) as numcliente",
    "to_date(cast(unix_timestamp(tfrom_date, 'yyyy-MM-dd') as timestamp)) as tfrom_date",
    "to_date(cast(unix_timestamp(fec_apertura, 'yyyy-MM-dd') as timestamp)) as fec_apertura",
    "tfrom",
    "to_date(cast(unix_timestamp(vintage_date,'yyyy-MM-dd') as timestamp)) vintage_date",
]


def _seniority_calc() -> Column:
    """Expresión común de seniority: meses entre la apertura y el corte
    (función porque crear ``Column``s requiere un SparkContext activo)."""
    return months_between(
        col("tfrom_date"), trunc(col("fec_apertura"), "month"))


class Mobs:
    """
    Class used for MOBs (Months on Books) calculation and imputation in
    datapull.

    It has two responsibilities:

    1. ``calculate_mobs`` — computes per-source seniority tables
       (``mobs_<source>``) from the historic data and stores a single
       ``calculate_mobs`` parquet keyed by ``(numcliente, vintage_date)``.
    2. Imputation — fills the feature-engineering outputs for clients
       whose seniority is below each feature period, either with the
       generic ``previous_fixer`` (``valor_previo4``) or with the
       source-specific ``PreviousFixer*`` classes of this package.

    Parameters
    ----------
    year: string
        String with the year month to be processed with format YYYYMM
    mt_dict: dictionary
        Dictionary with the variables, methods and sources to be processed
        (same ``config_rigid[source]`` dict used by ``MissingTreatment``;
        MOBs read the ``missing_treatment.mobs`` section)
    source: String
        Name of the source to specify paths
    n_mis: int
        Month's number of history 12,24,36,...,etc
    n_threads: int
        Number of threads to be used
    mode:
        How will the parquets be written
    is_dynamic:
        Si es True, los intermedios ya escritos (``calculate_mobs`` y cada
        feature imputada/copiada con ``_SUCCESS``) se recargan/saltan en
        vez de recomputarse — equivale al decorador ``dynamic`` de
        ``libs.steps`` para los guardados que no devuelven DataFrame.
    """
    def __init__(self, year: str, mt_dict: Dict[str, Any], source: str,
                 n_mis: int, n_threads: int,
                 mode: str = "overwrite",
                 is_dynamic: bool = True) -> None:
        """Constructor de la clase Mobs."""
        self.year: str = year
        self.mt_dict: Dict[str, Any] = mt_dict
        self.source: str = source
        self.n_mis: int = n_mis
        self.n_threads: int = n_threads
        self.mode: str = mode
        self.is_dynamic: bool = is_dynamic

    ######################################## metodo principal mobs (run_mobs) ########################################
    def run_mobs(self, tables_path: str, pathwrite: str, features_path: str,
                 feat_list: List[List[str]], periodo: int,
                 sql_context: SparkSession) -> None:
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
        fixer_cls = _FIXER_BY_SOURCE.get(self.source)
        if fixer_cls is not None:
            fixer_cls(
                path_mobs_fe=pathwrite,
                features_path=features_path,
                write_mobs=pathwrite,
                feat_list=feat_list,
                periodo=periodo,
                n_mis=self.n_mis,
                n_threads=self.n_threads,
                sql_context=sql_context,
                # is_dynamic: las features ya imputadas se recargan/saltan
                # (reanudación a mitad de mobs)
                is_dynamic=True,
            ).multi_thread_imputate_features()
        else:
            self.previous_fixer(pathwrite, features_path,
                pathwrite, feat_list, periodo, sql_context)

    ######################################## metodos para llenado mobs (Calculate) ########################################
    def calculate_mobs(self, historic_path: str, pathwrite: str,
                       sql_context: Optional[SparkSession] = None) -> None:
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
        # reload intermedio: calculate_mobs ya escrito (dir con _SUCCESS)
        # y corrida dinámica -> se salta el join pesado por fuente
        if self.is_dynamic and is_successful(
                os.path.join(pathwrite, "calculate_mobs")):
            logging.info("calculate_mobs reloaded from %s", pathwrite)
            return
        df_full = sql_context.createDataFrame([], StructType([StructField('numcliente', StringType(), True),
                                                            StructField('vintage_date', StringType(), True)]))
        tables_path = os.path.join(
            historic_path, self.year, "mobs_preparation")
        # fuente -> (método, path base). Las payroll leen del directorio
        # de salida (su input es el tagged del propio datapull).
        dispatch = {
            "liabilities": (self.liabilities_mobs, tables_path),
            "credit_card": (self.credit_card_mobs, tables_path),
            "bureau": (self.bureau_mobs, tables_path),
            "payroll_income": (self.payroll_income_mobs, pathwrite),
            "payroll_streaks": (self.payroll_streaks_mobs, pathwrite),
            "adobe": (self.adobe_mobs, pathwrite),
        }
        for source in self.mt_dict["missing_treatment"]["mobs"]["sources"]:
            method, base_path = dispatch[source]
            df = method(base_path, self._list_tables(source), sql_context)
            df_full = df_full.join(
                df, on=["numcliente", "vintage_date"], how="fullouter")
        #
        if ("mobs_liabilities"in df_full.columns) \
            and ("mobs_credit_card" in df_full.columns):
            df_full = df_full.withColumn("max_liab_cc", greatest(col("mobs_liabilities"), col("mobs_credit_card")))
        #
        self._write_parquet(df_full, pathwrite, "calculate_mobs")

    def _seniority_mobs(self, tables_path: str, table_names: List[str],
                        source: str,
                        sql_context: Optional[SparkSession] = None) -> List[DataFrame]:
        """Lectura + ``_calculate_separated_mobs`` con la query/expresión
        de seniority compartida (``_SENIORITY_QUERY`` / ``_seniority_calc``)."""
        dfs = self._get_dataframes(
            tables_path, table_names, sql_context=sql_context)
        return self._calculate_separated_mobs(
            dfs, _SENIORITY_QUERY, source, _seniority_calc())

    def liabilities_mobs(self, tables_path: str, table_names: List[str],
                         sql_context: Optional[SparkSession] = None) -> DataFrame:
        """
        This method specifies the columns to be selected in the query for the
        6 liabilities tables, calculates the separated mobs and unions them
        taking the maximum seniority per client.

        Parameters
        ----------
        tables_path: string
            specifiyng the path where liabilities collect tables are stored
        table_names: array
            contains the names of the tables to be used (for liabilities must be an array with 6 names)
        """
        dfs_liab = self._seniority_mobs(
            tables_path, table_names, 'liabilities', sql_context)
        return reduce(lambda a, b: a.union(b), dfs_liab).\
            groupby(["numcliente", "vintage_date"]).\
            agg(max("mobs_liabilities").alias("mobs_liabilities"))

    def credit_card_mobs(self, tables_path: str, table_names: List[str],
                         sql_context: Optional[SparkSession] = None) -> DataFrame:
        """
        This method specifies the columns to be selected in the query for the pnl table
        and calculates its mobs.

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
        return self._seniority_mobs(
            tables_path, table_names, 'credit_card', sql_context)[0]

    def bureau_mobs(self, tables_path: str, list_tables: List[str],
                    sql_context: Optional[SparkSession] = None) -> DataFrame:
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

    def _single_tagged_df(self, tables_path: str, list_tables: List[str],
                          sql_context: SparkSession) -> DataFrame:
        """Lee el único dataframe tagged de una fuente payroll.

        ``tables_path`` es el directorio de salida de mobs, así que el
        input real está un nivel arriba (``datapull``)."""
        # os.path.dirname: en Windows el separador es '\' y PosixPath
        # lo trataría como carácter normal (parent equivocado)
        datapull_path = os.path.dirname(tables_path)
        source_data_frames = self._get_dataframes(
            datapull_path, list_tables, sql_context=sql_context)
        assert len(source_data_frames) == 1, "Only one mobs dataframe is expected"
        return source_data_frames[0]

    @staticmethod
    def _join_mobs(dfs: List[DataFrame], mobs_columns: List[str]) -> DataFrame:
        """Full-outer join de los dataframes de mobs por
        ``(numcliente, vintage_date)`` y ``fillna(0.0)`` en sus columnas."""
        joined = reduce(
            lambda df1, df2: df1.join(
                df2, on=["vintage_date", "numcliente"], how="outer"),
            dfs)
        return joined.fillna(0.0, subset=mobs_columns)

    def payroll_income_mobs(self, tables_path: str, list_tables: List[str],
                            sql_context: SparkSession) -> DataFrame:
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
        df_tagged = self._single_tagged_df(tables_path, list_tables, sql_context)
        assert "tfrom" in df_tagged.columns, "tfrom column is missing"
        assert "numcliente" in df_tagged.columns, "numcliente column is missing"
        #
        # GET NUMERIC COLUMNS (sin las claves)
        numerical_columns = [c for c in get_numeric_columns(df_tagged)
                             if c not in ("tfrom", "numcliente")]
        # columnas de cada familia tef
        tef_columns = {
            key: [c for c in numerical_columns if key in c]
            for key in ("tef_1", "tef_2", "tefs")
        }
        #
        # Get mobs from tefs
        #
        mobs_columns = [f"mobs_{key}" for key in tef_columns]
        dfs = [discontiguous_mobs(df_tagged, f"mobs_{key}", cols, 0)
               for key, cols in tef_columns.items()]
        return self._join_mobs(dfs, mobs_columns)

    def payroll_streaks_mobs(self, tables_path: str, list_tables: List[str],
                             sql_context: SparkSession) -> DataFrame:
        """
        Genera un DataFrame con los mobs contiguos y no contiguos para las columnas 'tef_1', 'tef_2' y 'tefs'.
        """
        df_tagged = self._single_tagged_df(tables_path, list_tables, sql_context)
        max_tfrom = df_tagged.coalesce(1).select(spark_max(col("tfrom"))).collect()[0][0]
        #
        df_tagged = df_tagged.withColumn("tefs", coalesce(col("tef_1"), col("tef_2")))
        #
        keys = ["tef_1", "tef_2", "tefs"]
        mobs_columns = [f"discontiguous_mobs_{k}" for k in keys] + \
                       [f"contiguous_mobs_{k}" for k in keys]
        dfs = [discontiguous_mobs(df_tagged, f"discontiguous_mobs_{k}", [k], 0) for k in keys]
        dfs += [contiguous_mobs(df_tagged, f"contiguous_mobs_{k}", [k], 0) for k in keys]
        #
        # quitar un mes a todos los mobs, porque n_mis tiene un mes extra en el extract
        dfs = [df.withColumn(name, when(col(name) == max_tfrom + 1, max_tfrom).otherwise(col(name)))
               for df, name in zip(dfs, mobs_columns)]
        #
        return self._join_mobs(dfs, mobs_columns)

    def adobe_mobs(self, tables_path: str, list_tables: List[str],
                   sql_context: SparkSession) -> DataFrame:
        """
        Antigüedad digital (mobs_adobe) para fuentes Adobe Clickstream.

        Lee el ``extract/<table>_historic`` (tfrom mensual, pre-``weeks``)
        filtrando los meses vacíos de la malla pivot y define
        ``mobs_adobe = max(tfrom)``: meses desde la primera actividad
        digital observada (tfrom inclusivo: 1 = mes del vintage), capeado
        al borde de la ventana. Un cliente sin hits no aparece en el
        dataframe — ``previous_fixer`` lo lleva a ``mobs=0`` y sus
        features quedan en -99999.
        """
        df_tagged = self._single_tagged_df(tables_path, list_tables, sql_context)
        assert "tfrom" in df_tagged.columns, "tfrom column is missing"
        # la malla del extract trae meses vacíos (columnas de hit a null)
        # por cada cliente del pivot: sin filtrar, max(tfrom) siempre
        # sería el tope de ventana y nadie tendría antigüedad corta
        date_col = self.mt_dict.get("extract", {}).get(
            "name_mis", "file_date")
        if isinstance(date_col, list):
            date_col = date_col[0]
        df_tagged = df_tagged.filter(col(date_col).isNotNull())
        # tfrom del extract es inclusivo: 1 = mes del vintage, max = mes
        # más viejo extraído -> max(tfrom) ya es la antigüedad en meses,
        # naturalmente capeada al borde de la ventana (conservador: actividad
        # en el mes más viejo no distingue 6m de historia mayor)
        return df_tagged.groupBy("numcliente", "vintage_date")\
            .agg(spark_max(col("tfrom")).alias("mobs_adobe"))\
            .fillna(0, subset=["mobs_adobe"])
    ######################################## metodos para llenado mobs (Imputacion) ########################################
    def previous_fixer(self, path_mobs_fe: str, features_path: str,
                       write_mobs: str, feat_list: List[List[str]],
                       periodo: int, sql_context: SparkSession) -> None:
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
        lst_months = self._get_months(periodo)
        df_paths=[]
        for item in range(len(lst_months)-1):
            df_paths.append(f"temp{item}")
        for feature in feat_list[0]:  # imputacion de features
            # reload intermedio: feature ya imputada (dir con _SUCCESS)
            # -> se salta; reanudación a mitad de mobs
            if self.is_dynamic and is_successful(
                    os.path.join(pathwrite, feature)):
                logging.info("Feature %s reloaded from %s", feature, pathwrite)
                continue
            print(f"inicia imputacion {feature}")
            self.valor_previo4(features_path,feature,join_mobs,pathwrite,lst_months,sql_context)
        for feature in feat_list[1]:  # Copia de features
            print(f"inicia copia {feature}")
            self._copy_feature(features_path, feature, pathwrite, sql_context)
        self._delete_intermediate_list(pathwrite, list(set(df_paths)))
    def valor_previo4(self, features_path: str, feature: str,
                      mobs_info: DataFrame, pathwrite: str,
                      lista_meses: List[int],
                      sql_context: SparkSession) -> None:
        feats_path =  os.path.join(features_path,feature)
        for i in range(len(lista_meses)):
            num_temp = f"temp{str(i)}"
            if i == len (lista_meses)-1:
                path_out = pathwrite
            else:
                path_out = os.path.join(pathwrite,num_temp)
            self.valor_previo4_step(feats_path, feature,mobs_info, path_out, lista_meses[i], i, lista_meses, sql_context)
            feats_path = os.path.join(path_out,feature)
    def valor_previo4_step(self, features_path: str, feature: str,
                           mobs_info: DataFrame, pathwrite: str, mes: int,
                           num_iteracion: int, lista_meses: List[int],
                           sql_context: SparkSession) -> None:
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
    ###############################################################
    # Helper methods
    ###############################################################
    def _copy_feature(self, features_path: str, feature: str,
                      pathwrite: str, sql_context: SparkSession) -> None:
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
        # reload intermedio: feature ya copiada (dir con _SUCCESS)
        if self.is_dynamic and is_successful(
                os.path.join(pathwrite, feature)):
            logging.info("Feature %s reloaded from %s", feature, pathwrite)
            return
        data_feature = sql_context.read.parquet(
            os.path.join(features_path, feature))
        self._write_parquet(data_feature, pathwrite, feature)

    def _write_parquet(self, df: DataFrame, path: str, file_name: str) -> None:
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

    def _get_dataframes(self, origin_path: str, table_names: List[str],
                        sql_context: Optional[SparkSession] = None) -> List[DataFrame]:
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
        return [sql_context.read.parquet(os.path.join(origin_path, name))
                for name in table_names]

    def _calculate_separated_mobs(self, dfs: List[DataFrame],
                                  query: List[str], source: str,
                                  calculate_mobs: Column) -> List[DataFrame]:
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
    def _list_tables(self, data_source: str) -> List[str]:
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

    def _get_months(self, periodo: Union[str, int]) -> List[int]:
        """
        Description:
        Method to create the months period ([3,6,9,12],[2,4,6,...,])
        (delega en :func:`libs.mobs.payroll_income.get_months`).
        """
        return get_months(periodo, self.n_mis)

    def _delete_intermediate_list(self, path: str, id_list: List[str]) -> None:
        """
        Deletes a list of paths below ``path`` (dual backend via
        :func:`libs.steps.path_delete`: ``hdfs dfs`` in the cluster, local
        filesystem otherwise; it is a no-op when a path does not exist).

        Parameters
        ----------
        path: string
            Parent path
        id_list: List of strings
            each item is joined to ``path`` and deleted
        """
        for item in id_list:
            path_delete(os.path.join(path, item))


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


def count_non_null_values(df: DataFrame, vintage_column: str, id_column: str,
                          column_to_check: str,
                          output_column: Optional[str] = "non_null_count") -> DataFrame:
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
    |2023-01-01| 3|             1|             1|
    |2023-01-01| 3|             2|             2|
    |2023-02-01| 1|          null|             0|
    |2023-02-01| 1|          null|             1|
    |2023-02-01| 1|          null|             2|
    |2023-02-01| 2|          null|             0|
    |2023-02-01| 2|          null|             1|
    |2023-02-01| 2|          null|             2|
    |2023-02-01| 3|             1|             0|
    |2023-02-01| 3|             1|             1|
    |2023-02-01| 3|             2|             2|
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


def get_last_contiguous_non_null_value(df: DataFrame, vintage_column: str,
                                       id_column: str, column_to_check: str,
                                       sorting_column: str,
                                       output_column: str) -> DataFrame:
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
        output_column (str): Nombre de la columna de salida.

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


def _default_missing_condition(column: str) -> Column:
    """Condición por defecto de "missing": ``<= 0``, nulo o NaN."""
    return (col(column) <= 0) | (col(column).isNull()) | (isnan(col(column)))


def _mobs_filtered_df(
    df: DataFrame,
    columns_to_evaluate: List[str],
    existing_columns_number_tolerance: Optional[int],
    missing_filter_condition: Optional[Callable[[str], Column]],
) -> DataFrame:
    """Preludio común de :func:`contiguous_mobs`/:func:`discontiguous_mobs`:
    añade ``filtered_value`` = ``tfrom`` cuando la fila supera el número
    mínimo de columnas con dato; ``None`` en caso contrario."""
    if missing_filter_condition is None:
        missing_filter_condition = _default_missing_condition
    if existing_columns_number_tolerance is None:
        existing_columns_number_tolerance = 0
    #
    # ADD MISSING COUNTS
    count_expr = sum(when(~missing_filter_condition(column), 1).otherwise(0) for column in columns_to_evaluate)
    # ADD EXISTING COLUMNS COUNT
    df = df.withColumn("existing_columns_count", count_expr)
    df = df.withColumn("filtered_value", when(col("existing_columns_count") > existing_columns_number_tolerance, col("tfrom")).otherwise(None))
    return df


def contiguous_mobs(
    df: DataFrame,
    output_column_name: str,
    columns_to_evaluate: List[str],
    existing_columns_number_tolerance: Optional[int] = 0,
    missing_filter_condition: Optional[Callable[[str], Column]] = None
) -> DataFrame:
    """
    Meses contiguos con dato desde ``tfrom`` 0 hasta el primer missing.

    Args:
        df (DataFrame): El DataFrame de PySpark que contiene los datos.
        output_column_name (str): El nombre de la columna de salida que se agregará al DataFrame resultante.
        columns_to_evaluate (List[str]): Una lista de nombres de columnas que se evaluarán para valores no faltantes.
        existing_columns_number_tolerance (Optional[int], optional): El número de columnas no faltantes requerido para considerar una
        missing_filter_condition (Optional[Callable[[str], Column]], optional): Una función que toma un nombre de columna y devuelve
            la condición de "missing" (por defecto ``<=0 | null | NaN``).

    Returns:
        DataFrame: Un nuevo DataFrame con la columna de salida agregada, que contiene el conteo de filas que cumplen con la condición
    """
    df = _mobs_filtered_df(df, columns_to_evaluate,
                           existing_columns_number_tolerance,
                           missing_filter_condition)
    #
    result = get_last_contiguous_non_null_value(df=df, vintage_column="vintage_date", id_column="numcliente", column_to_check="filtered_value", sorting_column="tfrom", output_column=output_column_name)
    result = result.withColumn(output_column_name, col(output_column_name)+1)
    result = result.withColumn(output_column_name, col(output_column_name).cast("double"))
    return result


def discontiguous_mobs(
    df: DataFrame,
    output_column_name: str,
    columns_to_evaluate: List[str],
    existing_columns_number_tolerance: Optional[int] = 0,
    missing_filter_condition: Optional[Callable[[str], Column]] = None
) -> DataFrame:
    """
    Meses totales con dato (contiguos o no).

    Args:
        df (DataFrame): El DataFrame de PySpark que contiene los datos.
        output_column_name (str): El nombre de la columna de salida que se agregará al DataFrame resultante.
        columns_to_evaluate (List[str]): Una lista de nombres de columnas que se evaluarán para valores no faltantes.
        existing_columns_number_tolerance (Optional[int], optional): El número de columnas no faltantes requerido para considerar una
        missing_filter_condition (Optional[Callable[[str], Column]], optional): Una función que toma un nombre de columna y devuelve
            la condición de "missing" (por defecto ``<=0 | null | NaN``).

    Returns:
        DataFrame: Un nuevo DataFrame con la columna de salida agregada, que contiene el conteo de filas que cumplen con la condición
    """
    df = _mobs_filtered_df(df, columns_to_evaluate,
                           existing_columns_number_tolerance,
                           missing_filter_condition)
    #
    result = count_non_null_values(df=df, vintage_column="vintage_date", id_column="numcliente", column_to_check="filtered_value", output_column=output_column_name)
    result = result.withColumn(output_column_name, col(output_column_name).cast("double"))
    return result
