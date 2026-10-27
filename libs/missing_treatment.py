"""
Project: Missing Treatment (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gio Guerrero, Brayan Borquez, Gil Rodriguez, Jorge Galicia,
    Rodolfo Arturo Gonzalez
Update Date: 2025-02-20
Version: 1.1

Script containing the class and methods for the Missing Treatment in datapull
(``mt`` stage: fills nulls of the historic tables using the configured
``fill_methods``).

The MOBs (Months on Books) calculation/imputation that used to live in this
class now lives in :class:`libs.mobs.mobs.Mobs` (``mobs`` stage).
"""
import logging
import os
import re
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    first,
    lit,
    max,
    min,
    monotonically_increasing_id,
    when,
)
from pyspark.sql.window import Window

from .steps import path_delete, save_parquet

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class MissingTreatment:
    """
    Class used for Missing Treatment in datapull

    Fills Values of a dataframe, this class is used to fill values
    after collect_history (``mt`` stage).

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
    def __init__(self, year: str, mt_dict: Dict[str, Any], source: str,
                 n_mis: int, n_threads: int,
                 mode: str = "overwrite") -> None:
        """Constructor de la clase MissingTreatment."""
        self.year: str = year
        self.mt_dict: Dict[str, Any] = mt_dict
        self.source: str = source
        self.n_mis: int = n_mis
        self.n_threads: int = n_threads
        self.mode: str = mode

    ######################################## metodo principal missing treatment (run_mt) ########################################
    def run_mt(self, path_mt: str, tables_mt: List[str], path_write: str,
               sql_context: SparkSession) -> None:
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
        fill_methods = self.mt_dict["missing_treatment"]["fill_methods"]
        mt_dir = os.path.join(path_write, self.year, self.source, "missing_treatment")
        # Iterar sobre las tablas que existan en el directorio
        for item_df, df in enumerate(df_lst):
            # Iterar sobre los metodos de filling values
            if(self.source == "bureau_cohort"):
                df=self.__vars_select(df,self.mt_dict["missing_treatment"]["fill_variables"]["variable"])
            for item_method, method in enumerate(fill_methods["metodo"]):
                grouped = fill_methods["grouped"][item_method]
                # nombre en el config -> función de relleno
                dispatch = {
                    "fill_stat": lambda d: self.fill_stat(
                        d, grouped, fill_methods["valor"][item_method],
                        path_write, sql_context),
                    "fill_value": lambda d: self.fill_value(d, item_method),
                    "fill_snap": lambda d: self.fill_snap(d, grouped),
                    "fill_historic": lambda d: self.fill_historic(
                        d, grouped, 3, path_write, sql_context),
                    "fill_debito": lambda d: self.fill_debito(
                        d, grouped, sql_context),
                }
                for key, fill in dispatch.items():
                    if key in method:
                        df = fill(df)
                        break
                # temp por tabla+método: un checkpoint compartido entre
                # tablas puede mezclar schemas distintos si un write
                # dinámico deja particiones del writer anterior
                temp_name = f"temp_{item_df}_{item_method}"
                if(intermedium):
                    self.__write_parquet(df, mt_dir, temp_name)
                    list_dir.append(os.path.join(mt_dir, temp_name))
                df=sql_context.read.parquet(os.path.join(mt_dir, temp_name))
            self.__write_parquet(df, mt_dir, f"{tables_mt[item_df]}")

        for dir in list_dir:
            path_delete(dir)

    ######################################## metodos para llenado de MT ########################################
    def fill_historic(self, df: DataFrame, grouped: List[str], kmiss: int,
                      path_write: str,
                      sql_context: SparkSession) -> DataFrame:
        """Rellena los missings usando valores históricos del propio cliente.

        Cadena: ``last_null`` (extremo final) -> ``snapshot`` (tfrom 0) ->
        ``interpolacion`` (media de vecinos) -> ``moving_average``
        (media móvil de ``kmiss`` meses). Solo se aplica a clientes con
        menos de ``null_count`` nulos según el config.

        Parameters
        ----------
        df : DataFrame
            Dataframe a procesar.
        grouped : List[str]
            Columnas de agrupación de la ventana.
        kmiss : int
            Tamaño de la ventana de la media móvil.
        path_write : str
            Working path (para los parquets temporales).
        sql_context : SparkSession
            Tu sqlContext.

        Returns
        -------
        DataFrame
            Dataframe con los valores rellenados.
        """
        def snapshot(df: DataFrame, variable: str, grouped: List[str]) -> DataFrame:
            wp_client = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df = df.withColumn(variable,when((col("tfrom") == 0) & (col("null_count") <= 2) & (col(variable).isNull()), first(col(variable), ignorenulls=True).over(wp_client)).otherwise(col(variable)))
            return(df)

        def last_null(df: DataFrame, variable: str, grouped: List[str],
                      n_mis: int) -> DataFrame:
            wp_client_desc = Window.partitionBy(grouped).orderBy(col("tfrom").desc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            df = df.withColumn(variable,
            when((col("tfrom") == (int(n_mis)-1)) & (col("null_count") <= 2) & (col(variable).isNull()), first(col(variable), ignorenulls=True).over(wp_client_desc)).otherwise(col(variable)))
            return(df)

        def interpolacion(df: DataFrame, variable: str, grouped: List[str],
                          n_mis: int) -> DataFrame:
            wp_client_mid = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(-1, 1)
            df = df.withColumn(variable,
            when(
                (col("tfrom") > 0) & (col("tfrom") < (int(n_mis)-1)) & (col("null_count") <= 2) & (col(variable).isNull()), avg(col(variable)).over(wp_client_mid)
            ).otherwise(col(variable)))
            return(df)

        def moving_average(df: DataFrame, variable: str, grouped: List[str],
                           kmiss: int, n_mis: int) -> DataFrame:
            wp_mov_avg_b = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween(1, kmiss)
            wp_mov_avg_l = Window.partitionBy(grouped).orderBy(col("tfrom")).rangeBetween((kmiss)*-1, -1)
            df = df.withColumn(variable,
            when(
                ((col("null_count") == 3) & ((col("tfrom") + kmiss) <= (int(n_mis)-1))) & (col(variable).isNull()), avg(col(variable)).over(wp_mov_avg_b)
            ).when(
                ((col("null_count") == 3) & ((col("tfrom") + kmiss) > (int(n_mis)-1))) & (col(variable).isNull()), avg(col(variable)).over(wp_mov_avg_l)
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
                    # checkpoint por variable (trunca el lineage); antes
                    # se copiaba verbatim temp2->temp1: un write extra
                    # completo del dataset por variable
                    save_parquet(df, dir_temp1)
                    df=sql_context.read.parquet(dir_temp1)
            return (df)

    def fill_value(self, df: DataFrame, nmbr_id_variables: int) -> DataFrame:
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

    def fill_stat(self, df: DataFrame, grouped: List[str],
                  nmbr_id_stat: str, path_write: str,
                  sql_context: SparkSession) -> DataFrame:
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
                        # checkpoint solo cuando el fill se aplicó; antes
                        # escribia temp2+temp1 aun sin nulos (df intacto)
                        save_parquet(df, dir_temp1)
                        df=sql_context.read.parquet(dir_temp1)
        return (df)

    def fill_snap(self, df: DataFrame, grouped: List[str]) -> DataFrame:
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

    def fill_debito(self, df: DataFrame, grouped: List[str],
                    sql_context: SparkSession) -> DataFrame:
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
        # orden determinista (el esquema de df) — set() daría orden aleatorio
        aux_exc = set(aux_exc)
        lista_fin = [c for c in df.columns if c not in aux_exc]
        paso1.createOrReplaceTempView("df_aux_flags")
        # Fill 0 para bandera=0. Cada columna se proyecta una sola vez
        # (las de lista_fin van con su CASE): "select *, ... as item"
        # dejaba nombres duplicados y el writer de parquet tronaba con
        # "Found duplicate column(s)". Se conserva el orden del schema.
        fill_exprs = {item: "case when MFlg_SDO=0 and {0} is null then 0 else {0} end as {0}".format(
            item) for item in lista_fin}
        text_fill = "select "+",".join(fill_exprs.get(column, column)
            for column in paso1.columns)+" from df_aux_flags"
        df_fill = sql_context.sql(text_fill)
        return (df_fill)
    ###############################################################
    # Helper methods
    ###############################################################
    def __vars_select(self, df: DataFrame, dict_stat: List[str]) -> DataFrame:
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

    def __write_parquet(self, df: DataFrame, path: str, file_name: str) -> None:
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
        save_parquet(df, os.path.join(path, file_name), self.mode)

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
