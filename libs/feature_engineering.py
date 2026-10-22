"""
Project: Feature Engineering (Refactorization) (PEP 8, Python3.6 and OOP)
Developer: Gil Rodriguez & Brayan Borquez & Rodolfo Arturo Gonzalez
Update Date: 2025-03-02
Version: 1.1

Script containing the class and functions for the feature engineering in datapull
"""

import os
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, Sequence, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
import zlib

from numpy.typing import NDArray
from pyspark.sql.types import IntegerType,DoubleType,ArrayType,StringType
from multiprocessing.pool import ThreadPool
from cytoolz.functoolz import partial
import logging
import types
from itertools import groupby as it_groupby

from .steps import dynamic, path_exists
from .hive_path import is_successful

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)



FloatArray1D = NDArray[np.float64]



class FeatureEngineering:
    default_non_found_value = 0.0
    """
    Class used for feature engineering in datapull

    Parameters
    ----------
    df: pyspark dataframe
        Dataframe with the output of missing treatment
    path: string
        Working path of datapull
    period: int
        Number of months for agrupations
    sqlContext: pyspark sqlContext
        Your sqlContext
    number_threads: int
        Number of threads to use, by default 32
    overwrite: string
        mode of write of parquets, by default overwrite
    """
    CLOSE_AS = ")) as "
    FROM_DF = " from df"
    SELECT_ID = "select id_msf_tools_v2_12315, "

    def __init__(self, df: "DataFrame", working_path: str, period: int,
                 sqlContext: "SparkSession",
                 fe_dict: Optional[Dict[str, Any]] = None,
                 number_threads: int = 32,
                 mode: str = "overwrite",
                 is_dynamic: bool = True) -> None:
        self.path = os.path.join(working_path, "features")
        self.sqlContext = sqlContext
        self.period = period
        self.number_threads = number_threads
        self.mode = mode
        self.is_dynamic = is_dynamic
        self.hdfs = {"raw_features": os.path.join(self.path, "raw_features")}
        self.df = self._raw_features(df)
        self.list_mvars = self._get_mvars(period)
        self.lista_metodo = self._get_lista_metodo()

        try:
            self.ignore_this_value_when_calculating_mt_features = fe_dict["ignore_this_value_when_calculating_mt_features"]
        except:
            self.ignore_this_value_when_calculating_mt_features = -99999

        try:
            self.frequency_sufix = fe_dict["frequency_sufix"]
        except:
            self.frequency_sufix = "m"

        self.weights_dict = {last_weight:self.assign_weights(last_weight) for last_weight in [0.0, 0.25, 0.5, 0.75]}

    @dynamic("raw_features")
    def _raw_features(self, df: DataFrame) -> DataFrame:
        """Parquet de raw variables con reload-or-recompute (ver
        :func:`libs.steps.dynamic`): si ``raw_features`` ya está escrito y
        ``is_dynamic``, se recarga de disco en vez de recomputarse."""
        return self._create_dataframe(df)

    def _create_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Creates the dataframe for the transformations and the raw variables parquet

        Parameters
        ----------
        df: Pyspark dataframe
            Dataframe with the output of missing treatment

        Returns
        -------
        Pyspark dataframe
            Dataframe of all raw variables with null filled
        """
        df = self._create_raw_variables(df, self.path)
        df = self._fill_na_zero(df)
        return df

    def _get_lista_metodo(self) -> List[List[str]]:
        """
        Creates the list of lists of variables for certain methods, this list has all the months of variables, for example, for 6
        months of variable a5 returns:
        [[a5_0, a5_1, a5_2, a5_3, a5_4, a5_5]]

        Returns
        -------
        list
            List with all months of information of the variable
        """
        cols_df = self._obtain_columns(self.df)
        variables = self._obtain_variables(cols_df)
        lista_metodo = list()
        for feature in variables:
            lista_aux = self._filter_columns_feature(cols_df, feature)
            # ordenar por tfrom numérico; las fuentes con dominios de
            # tfrom dispersos (adobe: ld/dseq) no son densos desde 0
            lista_aux_2 = [lista_aux[k]
                           for k in sorted(lista_aux, key=int)]
            lista_metodo.append(lista_aux_2)
        return lista_metodo

    def _get_mvars(self, period: int) -> List[List[str]]:
        """
        Makes the list with the variables grouped by months

        Returns
        -------
        list
            Variables grouped by months
        """
        cols_df = self._obtain_columns(self.df)
        variables = self._obtain_variables(cols_df)
        list_mvars = self._list_mvars(variables,cols_df,period)
        return list_mvars

    def _create_raw_variables(self, df: DataFrame, path: str) -> DataFrame:
        """
        Creates a parquet with the raw variables.

        Parameters
        ----------
        df: Pyspark Dataframe
            Dataframe with the output of missing treatment
        path: string
            Path to write the features (raw variables for this function)

        Returns
        -------
        Pyspark Dataframe
            Dataframe of all raw variables with null filled
        """
        # print("Running raw features")
        logging.info("Performing Raw Features")
        df = df.withColumn("id_msf_tools_v2_12315", F.monotonically_increasing_id())
        logging.info("Ending Raw Features")
        #print("Ending raw features")
        return(df)

    def _obtain_columns(self, df: DataFrame) -> List[str]:
        """
        Make the array with only numerical columns

        Parameters
        ----------
        df: Pyspark Dataframe
            Dataframe of raw variables

        Returns
        -------
        None
        """
        cols = df.columns
        col_string = [item[0] for item in df.dtypes if item[1].startswith('double')]
        cols_df = [x for x in cols if x in col_string]
        return cols_df

    def _filter_columns_feature(self, cols_df: List[str],
                                feature: str) -> Dict[str, List[str]]:
        """
        Obtaing a list of features in cols of dataframe

        Parameters
        ----------
        cols_df: list
            List with the columns of dataframe
        feature: string
            feature to obtain the list

        Returns
        -------
        list
            All the features in the columns of dataframe
            {'0': 'hitbureau_0', '1': 'hitbureau_1', '10': 'hitbureau_10', '11': 'hitbureau_11', '2': 'hitbureau_2', '3':
            'hitbureau_3', '4': 'hitbureau_4', '5': 'hitbureau_5', '6': 'hitbureau_6', '7': 'hitbureau_7', '8': 'hitbureau_8', '9':
            'hitbureau_9'}
        """
        aux = [str for str in cols_df if feature in str]
        dict_feat = {}
        n = len(feature)
        aux_2 = [variable for variable in aux if variable.rindex("_") == n]
        for feature_aux in aux_2:
            dict_feat[feature_aux[n+1:]] = feature_aux
        return dict_feat

    def _obtain_variables(self, cols_df: List[str]) -> List[str]:
        """
        Obtain a list with the set of all raw features in dataframe

        Parameters
        ----------
        cols_df: list
            All numerical columns in the dataframe

        Returns
        -------
        list
            List of all the disctinct variables in dataframe
        """
        variables = []
        for variable in cols_df:
            if "_" in variable:
                if variable[variable.rindex("_")+1:].isdigit() and len(variable[variable.rindex("_")+1:])<=2:
                    n = variable.rindex("_")
                    variables.append(variable[:n])
                else:
                    cols_df.remove(variable)
            else:
                cols_df.remove(variable)
        # orden determinista (set() no lo es) — el orden decide el orden de
        # columnas del parquet de features
        variables = sorted(set(variables))
        return variables

    def _list_mvars(self, variables: List[str], cols_df: List[str],
                    periodo: int) -> List[List[str]]:
        """
        Obtain the list of variables by month, for example, using 6 months of variable a5 and a period of 2, returns
        [[a5_0, a5_1],
         [a5_0, a5_1, a5_2, a5_3],
         [a5_0, a5_1, a5_2, a5_3, a5_4, a5_5]]

        Parameters
        ----------
        variables: list
            List of all unique variables
        cols_df: list
            List with the columns of dataframe
        periodo: int
            Period to make feature engineering, for example, if periodo = 3 for 12 months of information for a feature returns
            feature_3m, feature_6m, feature_9m, feature_12m

        Returns
        -------
        list
            grouped months by variable
        """
        return_features = list()
        for feature in variables:
            features = self._filter_columns_feature(cols_df, feature)
            cont = 0
            n_periodo = 0
            while cont < len(features):
                cont += 1
                if cont % periodo == 0:
                    n_periodo += 1
                    # solo tfroms presentes: dominios dispersos (adobe)
                    # no tienen todas las columnas 0..k
                    list_aux = [features[str(k)]
                                for k in range(n_periodo * periodo)
                                if str(k) in features]
                    # saltar ventanas que no añaden columnas nuevas
                    # (mismo último tfrom -> mismo stat y nombre duplicado)
                    if list_aux and (not return_features
                                     or list_aux[-1] != return_features[-1][-1]):
                        return_features.append(list_aux)
        return return_features

    def _fill_na_zero(self, df: DataFrame) -> DataFrame:
        """
        Fill the null values of dataframe with zeros

        Parameters
        ----------
        df: pyspark dataframe
            Dataframe to fill nulls with zeros

        Returns
        -------
        pyspark dataframe
            Dataframe with nulls filled with zeros
        """
        df=df.na.fill(0)
        return(df)

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """Guarda ``df_save`` como parquet con ``self.mode`` (protocolo de
        :func:`libs.steps.dynamic`)."""
        df_save.write.mode(self.mode).parquet(path)

    def _write_parquet(self, df: DataFrame, path: str,
                       file_name: str) -> Tuple[()]:
        """
        Write the parquet of a certain df

        Parameters
        ----------
        df: pyspark dataframe
            Dataframe to write
        path: string
            path to write dataframe
        file_name: string
            name of the parquet file

        Returns
        -------
        None
        """
        df.write.mode(self.mode).parquet(os.path.join(path, file_name))
        return()

    def mt_transformations(self) -> 'StandardMethodsMT':
        """
        Return the class with the transformations for the mt features

        Returns
        -------
        StandardMethodsMT
            Class with the transformations for the mt features
        """
        return StandardMethodsMT(self)

    def onz_transformations(self) -> 'OnesAndZerosMethods':
        """
        Return the class with the transformations for the onz features

        Returns
        -------
        OnesAndZerosMethods
            Class with the transformations for the onz features
        """
        return OnesAndZerosMethods(self)

    def onz_transformations_mt(self) -> 'OnesAndZerosMethodsMT':
        """
        Return the class with the transformations for the onz features

        Returns
        -------
        OnesAndZerosMethods
            Class with the transformations for the onz features
        """
        return OnesAndZerosMethodsMT(self)

    def _run_transformation(self, transformation: str) -> None:
        """
        Run the transformations required

        Parameters
        ----------
        transformations: list
            List with all the transformations
        """
        # reload intermedio: si <trans>_features ya está escrita
        # (directorio con _SUCCESS), se recarga/salta en vez de recomputar
        out_path = os.path.join(self.path, transformation + "_features")
        if self.is_dynamic and is_successful(out_path):
            logging.info("%s_features reloaded from %s",
                         transformation, out_path)
            return
        # El orden importa: "onz_mt" termina en "_mt"; "onz_mt" termina
        # en "mt" pero no en "onz" al revés. Sufijo -> fábrica del objeto
        # que define el método `_<trans>Transformation`.
        owner = self
        for suffix, factory in (("onz_mt", self.onz_transformations_mt),
                                ("_mt", self.mt_transformations),
                                ("onz", self.onz_transformations)):
            if transformation.endswith(suffix):
                owner = factory()
                break
        getattr(owner, f"_{transformation}Transformation")()

    def run(self, transformations: List[str]) -> None:
        """
        Run the transformations required

        Parameters
        ----------
        :transformations: list
            List with all the transformations
        """
        # copia: `transformations` suele ser config_custom["lista_trans"] —
        # quitar ratiosmod de la lista original corrompería el config para
        # fuentes posteriores y para _fe_performed
        transformations = list(transformations)
        self.transformations = transformations
        self.df.createOrReplaceTempView("df")
        thread_pool = ThreadPool(self.number_threads)
        fun = partial(self._run_transformation)
        trans_dependeces = []
        if "ratiosmod" in transformations:
            transformations.remove("ratiosmod")
            trans_dependeces.append("ratiosmod")
        if "ratiosmod_mt" in transformations:
            transformations.remove("ratiosmod_mt")
            trans_dependeces.append("ratiosmod_mt")
        #
        thread_pool.map(fun, transformations)
        if len(trans_dependeces) > 0:
            thread_pool.map(fun, trans_dependeces)
        thread_pool.close()

    def _app_transformation(self, list_vars: str, key: List[str]) -> str:
        """
        Make the query to do the transformation using the register raw variables dataframe

        Parameters
        ----------
        key: list
            List with the name of the functions defined previously
        list_vars: list
            List of variables to make the transformation, can be lista_metodo or lista_mvars
        Returns
        -------
        app_transformation
            query used to make the transformation
        """
        if list_vars == "list_mvars":
            var_lists = self.list_mvars
            def name_of(k: int, inner: List[str]) -> str:
                base = key[k].replace("_fe", "")
                stem = inner[0][:inner[0].rindex("_")]
                return f"{base}_{stem}_{len(inner)}{self.frequency_sufix}"
        elif list_vars == "lista_metodo":
            var_lists = self.lista_metodo
            def name_of(k: int, inner: List[str]) -> str:
                stem = inner[0][:inner[0].rindex("_")]
                return f"{key[k]}_{stem}"
        else:
            raise ValueError(f"Invalid list_vars: {list_vars}")
        per_key = [
            ','.join(key[k] + "(array(" + ','.join(inner) + self.CLOSE_AS
                     + name_of(k, inner) for inner in var_lists)
            for k in range(len(key))
        ]
        aux = ', '.join(per_key)
        return self.SELECT_ID + aux + self.FROM_DF

    def _defineUdfTransformation(self, key: List[str],
                                 transformation: List[Any], list_vars: str,
                                 tipo: Any = DoubleType()) -> DataFrame:
        """
        Define the udf transformation and apply for the list of variables

        Parameters
        ----------
        key: list
            List with the name of functions nedeed to apply
        transformation: list
            List with the transformations to apply
        list_vars: int
            List with the vars to make transformations, it can be list_mvars or lista_metodo
        tipo: pyspark type
            Pyspark type used to return in the udf
        Returns
        -------
        pyspark dataframe
            Dataframe with the transformation defined maked
        """
        for k in range(len(key)):
            self.sqlContext.udf.register(key[k], transformation[k], tipo)
        app_transformation = self._app_transformation(list_vars,key)
        return_transformation = self.sqlContext.sql(app_transformation)
        return return_transformation

    def _runUdfTransformation(self, name: str, key: List[str],
                              transformation: List[Any], list_vars: str,
                              tipo: Any = DoubleType()) -> None:
        """
        Run the transformation using one or many udf's
        Parameters
        ----------
        name: string
            Name of the transformation to perform
        key: list
            List for all the names to define udf
        transformation: list
            List of all transformations to define udf
        list_vars: string
            List of vars to use, can be lista_metodo or list_mvars
        tipo: pyspark type
            Type to return in udf
        """
        # print(f"Running {name} transformations")
        logging.info("Running %s transformations", name)
        df_tmp = self._defineUdfTransformation(key, transformation, list_vars, tipo)
        self._write_parquet(df_tmp, self.path, name + '_features')
        df_tmp.unpersist()
        # print(f"Ending {name} transformations")
        logging.info("Ending %s transformations", name)

    # Transformaciones elementales
    def _meanTransformation(self) -> None:
        """
        Calculates the means

        :math:`\\frac{1}{n} \\sum_{i=0}^{n} x_{i}`

        using group of <period> months defined in class
        """
        self._runUdfTransformation("mean", ['mean_fe'], [lambda x: float(np.mean(x))], "list_mvars", DoubleType())
    def _stdTransformation(self) -> None:
        """
        Calculates the standard deviation using the formula

        :math:`\\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} (x_{i}-\\mu)^2}`

        where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        self._runUdfTransformation("std", ['std_fe'], [lambda x: float(np.std(x, ddof = 1))], "list_mvars", DoubleType())
    def _sumTransformation(self) -> None:
        """
        Calculates the minimum

        :math:`\\sum_{i=1}^{n} x_{i}`

        using group of <period> months defined in class.
        """
        self._runUdfTransformation("sum", ['sum_fe'], [lambda x: float(np.sum(x))], "list_mvars", DoubleType())
    def _minTransformation(self) -> None:
        """
        Calculates the minimum using group of <period> months defined in class.
        """
        self._runUdfTransformation("min", ['min_fe'], [lambda x: float(np.min(x))], "list_mvars", DoubleType())
    def _maxTransformation(self) -> None:
        """
        Calculates the maximum using group of <period> months defined in class.
        """
        self._runUdfTransformation("max", ['max_fe'], [lambda x: float(np.max(x))], "list_mvars", DoubleType())
    def _logmeanTransformation(self) -> None:
        """
        Calculates the logarithm of the mean :math:`log(\\mu)` if the mean is higher than 1
        else returns 0, using group of <period> months defined in class
        """
        def logmean(x: Sequence[float]) -> float:
            mean_value = float(np.mean(x))
            if mean_value>0:
                return float(np.log(mean_value))
            return (-1e3)
        self._runUdfTransformation("logmean", ['logmean'], [logmean], "list_mvars", DoubleType())
    def _squareTransformation(self) -> None:
        """
        Calculates the square of the mean :math:`\\mu^{2}` using group of <period> months defined in class.
        """
        def square(x: Sequence[float]) -> float:
            mean_value = float(np.mean(x))
            return float(mean_value)**2
        self._runUdfTransformation("square", ['square'], [square], "list_mvars", DoubleType())
    def _inverseTransformation(self) -> None:
        """
        Calculates the inverse of the mean :math:`\\frac{1}{\\mu}` using group of <period> months defined in class.
        If mean equals to zero the returns a high value (1e5)
        """
        def inverse(x: Sequence[float]) -> float:
            mean_value = float(np.mean(x))
            if mean_value != 0.0:
                return 1/mean_value
            else:
                return 1e5
        self._runUdfTransformation("inverse" ,['inverse'], [inverse], "list_mvars", DoubleType())
    def _trendTransformation(self) -> None:
        """
        Calculates the coefficient :math:`b` of the linear regresion

        :math:`y = a + bx`

        using group of <period> months defined in class.

        """
        def trend(x: Sequence[float]) -> float:
            n = len(x)
            y = len(x)
            sum_xy = 0.0
            sum_x_sq = 0.0
            sum_x = 0.0
            sum_y = 0.0
            for z in x:
                xy = float(y*z)
                x_sq=float(y*y)
                x = float(y)
                sum_xy += xy
                sum_x_sq += x_sq
                sum_x += x
                sum_y += float(z)
                y-=1
            num = float(((n*sum_xy)-(sum_x*sum_y)))
            den = float(((n*sum_x_sq)-(sum_x*sum_x)))
            if den != 0.0:
                trend = float(num/den)
            else:
                trend = float(num*1e5)
            return(trend)
        self._runUdfTransformation("trend",['trend'], [trend], "list_mvars", DoubleType())
    def _rmeansTransformation(self) -> None:
        """
        Calculates the ratios of means using actual vs previous, for example, for period of 3 months
        and 12 months of information calculates the ratio of the mean of months 4, 5 and 6 and the mean
        of months 1,2 and 3, also calculates the ratio of mean of months 7, 8, 9, 10, 11 and 12 and the mean
        of months 1,2,3,4,5 and 6.
        """
        def rmeans(x: Sequence[float], y: Sequence[float]) -> float:
            m_1 = np.mean(x)
            m_2 = np.mean(y)
            if m_2 != 0.0:
                r_mean = float(m_1 / m_2)
            else:
                r_mean = float(m_1 * 1e5)
            return(r_mean)
        #print("Running rmeans transformation")
        logging.info("Running Rmeans transformations")
        n = len(self.lista_metodo[0]) // self.period
        m = 1
        lista_rmeans = []
        while 2*m <= n:
            lista_rmeans += [[self.lista_metodo[k][:m * self.period],self.lista_metodo[k][m * self.period:2 * m *self.period],self.
            lista_metodo[k][0][:self.lista_metodo[k][0].rindex("_")]] for k in range(len(self.lista_metodo))]
            m += 1
        self.sqlContext.udf.register("rmeans", rmeans, DoubleType())
        query = "select id_msf_tools_v2_12315, " + ",".join(["rmeans(array(" + ",".join(inner_list[0]) + "),array(" + ",".join
        (inner_list[1]) + ")) as rmeans_" +\
            inner_list[2] + "_" + str(len(inner_list[0])) +  self.frequency_sufix for inner_list in lista_rmeans])+ self.FROM_DF
        df_r_means = self.sqlContext.sql(query)
        self._write_parquet(df_r_means, self.path, "rmeans_features")
        df_r_means.unpersist()
        #print("Ending rmeans transformation")
        logging.info("Ending Rmeans transformations")

    def _incrementalTransformation(self) -> None:
        """
        Calculates how incremental is the variable among the group of months using <period> according to the formula:

        :math:`\\sum_{i=1}^{n-1} {(1 \\quad \\text{if} \\quad x_{i+1} > x_{i} \\quad \\text{else} \\quad 0)}`

        where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        def incremental(x: Sequence[float]) -> float:
            x.reverse()
            const_inc=0.0
            for i in range(len(x)-1):
                if x[i+1]<x[i]:
                    const_inc += 1
            return(const_inc)
        #print("Running incremental transformations")
        logging.info("Running Incremental transformations")
        n = len(self.lista_metodo[0])
        m = 1
        lista_inc = []
        while self.period * m < n-1:
            lista_inc += [self.lista_metodo[k][:self.period * m + 1] for k in range(len(self.lista_metodo))]
            m += 1
        self.sqlContext.udf.register("incremental", incremental, DoubleType())
        query = self.SELECT_ID + ','.join(["incremental(array(" + ','.join(inner_hist) + self.CLOSE_AS + "i" + str(len(inner_hist)
        -1) + "_" +\
            inner_hist[0][:inner_hist[0].rindex("_")] for inner_hist in lista_inc])  + self.FROM_DF
        df_incremental = self.sqlContext.sql(query)
        self._write_parquet(df_incremental, self.path, "incremental_features")
        df_incremental.unpersist()
        #print("Ending incremental transformations")
        logging.info("Ending Incremental transformations")

    def _vpTransformation(self) -> None:
        """
        Calculates the mean and the standard deviation of the percental variations of all months defined by the next formula

        :math:`\\frac{x_{i+1}-x_{i}}{x_{i}}`

        If the denominator equals to 0 then the denominator is replaced by a small value (1e-5)
        """
        def means_vp(x: Sequence[float]) -> float:
            list_vp=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp = float((x[t+1]-x[t])/x[t])
                else:
                    vp = float((x[t+1]-x[t])*1e5)
                list_vp.append(vp)
                t+=1
            m = float(np.mean(list_vp))
            return m
        def std_vp(x: Sequence[float]) -> float:
            list_vp=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp = float((x[t+1]-x[t])/x[t])
                else:
                    vp = float((x[t+1]-x[t])*1e5)
                list_vp.append(vp)
                t+=1
            sd=float(np.std(list_vp, ddof = 1))
            return sd
        self._runUdfTransformation("vp", ['means_vp','std_vp'], [means_vp, std_vp], "lista_metodo", DoubleType())

    def _rtTransformation(self) -> None:
        """
        Calculates the mean and the standard deviation of the ratios of all months defined by the next formula

        :math:`\\frac{x_{i+1}}{x_{i}}`

        If the denominator equals to 0 then the denominator is replaced by a small value (1e-5)
        """
        def means_rt(x: Sequence[float]) -> float:
            list_ratios=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t+=1
            m=float(np.mean(list_ratios))
            return m
        def std_rt(x: Sequence[float]) -> float:
            list_ratios=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t+=1
            sd=float(np.std(list_ratios, ddof = 1))
            return sd
        self._runUdfTransformation("rt", ['means_rt','std_rt'], [means_rt, std_rt], "lista_metodo", DoubleType())
    def _soTransformation(self) -> None:
        """
        Calculates stochastic oscillator using groups of months defined by <period> according the formula:

        :math:`\\frac{\\mu-x_{min}}{x_{max}-x_{min}}`

        If the denominator equals to 0 then the return value is 1.
        """
        def stochastic_oscillator(x: Sequence[float]) -> Any | float:
            media=sum(x)/len(x)
            minimo=min(x)
            maximo=max(x)
            num=(media - minimo)
            den=(maximo - minimo)
            if den != 0.0:
                so = num/den
            else:
                so = 1.0
            return(so)
        self._runUdfTransformation("so", ['so'], [stochastic_oscillator], "list_mvars", DoubleType())
    def _avgmovTransformation(self) -> None:
        """
        Calculates the movil averages across the group of months
        """
        def avg_aux(x: Sequence[float]) -> float:
            cont=0.0
            num1=0.0
            den1=0.0
            for var in x:
                num1+=float(var)/float(cont+1)
                den1+=1/float(cont+1)
                cont=cont+1
            return(float(num1)/float(den1))
        self._runUdfTransformation("avgmov", ['avgmov'], [avg_aux], "list_mvars", DoubleType())
    def _run_ratios(self, tran_rt: Tuple[str, str]) -> None:
        """
        Perform the ratios transformation of ratiosmod

        Parameters
        ----------
        tran_rt: tuple
            Tuple with pair numerator/denominator
        """
        name_ratio = tran_rt[0] + tran_rt[1]
        df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt[0]+"_features"))
        df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt[1]+"_features"))
        list_num = df_num.drop("id_msf_tools_v2_12315").columns
        list_den = df_den.drop("id_msf_tools_v2_12315").columns
        list_rnumden = list()
        for var_num in list_num:
            var_name = var_num[var_num.index("_"):]
            var_den = tran_rt[1] + var_name
            if var_den in list_den:
                list_rnumden.append([var_num, var_den])
        if not list_rnumden:
            logging.info("ratiosmod %s: sin columnas comunes, se omite",
                         name_ratio)
            return
        df = df_num.join(df_den, df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315).drop(df_den.id_msf_tools_v2_12315)
        app_rt_mod_rnumden = self.SELECT_ID + ', '.join(["rt_mod(array(" + ','.join(inner_rnumden) + self.CLOSE_AS + f"r{name_ratio}_" + inner_rnumden[0][inner_rnumden[0].index("_")+1:] for inner_rnumden in list_rnumden]) + " from df_aux_r" + name_ratio
        df.createOrReplaceTempView("df_aux_r" + name_ratio)
        df_aux = self.sqlContext.sql(app_rt_mod_rnumden)
        self._write_parquet(df_aux, self.path, f"r{name_ratio}_features")
        df_aux.unpersist()

    def _run_deltas(self, tran_d: Tuple[str, str]) -> None:
        """
        Perform the ratios transformation of ratiosmod

        Parameters
        ----------
        tran_d: tuple
            Tuple with pair (a,b) to make a-b
        """
        name_d = tran_d[0] + tran_d[1]
        df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[0]+"_features"))
        df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[1]+"_features"))
        list_p = df_num.drop("id_msf_tools_v2_12315").columns
        list_m = df_den.drop("id_msf_tools_v2_12315").columns
        list_dpm = list()
        for var_p in list_p:
            var_name = var_p[var_p.index("_"):]
            var_m = tran_d[1] + var_name
            if var_m in list_m:
                list_dpm.append([var_p, var_m])
        if not list_dpm:
            logging.info("ratiosmod deltas %s: sin columnas comunes, se omite",
                         name_d)
            return
        df = df_num.join(df_den, df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315).drop(df_den.id_msf_tools_v2_12315)
        app_d_mod_dpm = self.SELECT_ID + ', '.join(["df_mod(array(" + ','.join(inner_dpm) + self.CLOSE_AS + f"d{name_d}_" +\
            inner_dpm[0][inner_dpm[0].index("_")+1:] for inner_dpm in list_dpm]) + " from df_aux_d" + name_d
        df.createOrReplaceTempView("df_aux_d" + name_d)
        df_aux = self.sqlContext.sql(app_d_mod_dpm)
        self._write_parquet(df_aux, self.path, f"d{name_d}_features")
        df_aux.unpersist()

    def _ratiosmodTransformation(self) -> None :
        """
        Calculates ratios and deltas between the combinations of min, max and mean of all variables.
        This transformation need the mean, min and max transformations.
        """
        def rt_mod(x: Sequence[float]) -> float:
            if x[1] != 0.0:
                rt=float((x[0] / x[1]))
            else:
                rt = 0.0
            return rt
        def df_mod(x: Sequence[float]) -> float:
            dif = float((x[0] - x[1]))
            return dif
        #print("Running ratiosmod transformation")
        logging.info("Running RatiosMod transformations")
        self.sqlContext.udf.register("rt_mod", rt_mod, DoubleType())
        self.sqlContext.udf.register("df_mod", df_mod, DoubleType())
        elemental_trans = ["min", "max", "mean", "std", "sum"]
        trans_rt = [("min","max"),("min","mean"),("max","mean"),("std","sum"),("std","mean")]
        trans_d = [("max","min"),("mean","min"),("max","mean")]
        not_performed_trans = []
        for trans in elemental_trans:
            if not is_successful(os.path.join(self.path, trans + "_features")):
                for element in trans_rt.copy():
                    if trans in element:
                        trans_rt.remove(element)
                for element in trans_d.copy():
                    if trans in element:
                        trans_d.remove(element)
        thread_pool = ThreadPool(self.number_threads)
        fun_1 = partial(self._run_ratios)
        fun_2 = partial(self._run_deltas)
        thread_pool.map(fun_1, trans_rt)
        thread_pool.map(fun_2, trans_d)
        thread_pool.close()
        #print("Ending ratiosmod transformation")
        logging.info("Ending RatiosMod transformations")

    def _rmsTransformation(self) -> None:
        """
        Calculates root mean square using groups of months defined by <period> according the formula:

        :math:`\\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} x_{i}^2}`

        """
        def rms(x: Sequence[float]) -> float:
            return float(np.sqrt(np.mean(np.square(x))))
        self._runUdfTransformation("rms", ['rms'], [rms], "list_mvars", DoubleType())
    def _cmpxTransformation(self) -> None:
        """
        Calculates complexity of all months using the formula:

        :math:`\\sqrt{\\sum_{i=1}^{n} (x_{i+1}-x_{i})^2}`

        """
        def complexity(x: Sequence[float]) -> float:
            x = np.diff(x)
            return float(np.sqrt(np.dot(x, x)))
        self._runUdfTransformation("cmpx", ['cmpx'], [complexity], "list_mvars", DoubleType())
    def _skewTransformation(self) -> None:
        """
        Returns the sample skewness across all the months(calculated with the adjusted Fisher-Pearson
        standardized moment coefficient G1)
        """
        def skewness(x: Sequence[float]) -> float:
            if len(x) < 4:
                value = 0.0
            else:
                x = pd.Series(x)
                value = float(pd.Series.skew(x))
            return value
        self._runUdfTransformation("skew", ['skew'], [skewness], "list_mvars", DoubleType())
    def _curtTransformation(self) -> None:
        """
        Returns the kurtosis of all months (calculated with the adjusted Fisher-Pearson standardized
        moment coefficient G2)
        """
        def curtosis(x: Sequence[float]) -> float:
            if len(x) < 4:
                value = 0.0
            else:
                x = pd.Series(x)
                value = float(pd.Series.kurtosis(x))
            return value
        self._runUdfTransformation("curt", ['curt'], [curtosis], "list_mvars", DoubleType())
    #
    def _weighted_meanTransformation(self) -> None:
        """
        Calculates the weighted mean using the weights defined in the class
        """
        def create_weighted_mean_functions(weights: Sequence[float]) -> Callable[..., float]:
            def weighted_mean(x: Sequence[float]) -> float:
                truncated_weights = weights[:len(x)]
                return float(np.average(x, weights=truncated_weights))
            return weighted_mean
        name = "weighted_mean"
        keys = ["weighted_mean_" + convert_to_three_digit_string(last_weight) for last_weight, weights in self.weights_dict.items()]
        transformations = [create_weighted_mean_functions(weights) for last_weight, weights in self.weights_dict.items()]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())
    #
    def assign_weights(self, last_weight: float) -> FloatArray1D:
        max_leng_mvars = max(len(x) for x in self.list_mvars)
        weights = np.linspace(1, last_weight, max_leng_mvars)
        return weights
    #
    def _evaluate_path(self, path: str) -> bool:
        """Evalúa si ``path`` existe (ver :func:`libs.steps.path_exists`)."""
        return path_exists(path)
    #

class ExternalTransformations:
    """Clase base de las transformaciones "externas" (MT y ONZ).

    Recibe el ``FeatureEngineering`` padre y copia sus atributos
    compartidos (``sqlContext``, ``lista_metodo``, ``list_mvars``,
    constantes de query, ``number_threads``).
    """
    def __init__(
        self,
        parent:"FeatureEngineering"
    ) -> None:
        self.parent = parent
        self.sqlContext = self.parent.sqlContext
        self.lista_metodo = self.parent.lista_metodo
        self.frequency_sufix = self.parent.frequency_sufix
        self.period = self.parent.period
        self.path = self.parent.path
        self.list_mvars = self.parent.list_mvars
        self.FROM_DF = self.parent.FROM_DF
        self.CLOSE_AS = self.parent.CLOSE_AS
        self.SELECT_ID = self.parent.SELECT_ID
        self.number_threads = self.parent.number_threads
    #
    def _write_parquet(self, df: DataFrame, path: str,
                       file_name: str) -> Tuple[()]:
        """
        Write the parquet of a certain df

        Parameters
        ----------
        df: pyspark dataframe
            Dataframe to write
        path: string
            path to write dataframe
        file_name: string
            name of the parquet file
        Returns
        -------
        None
        """
        return self.parent._write_parquet(df, path, file_name)
    #
    def _runUdfTransformation(self, name: str, key: List[str],
                              transformation: List[Any], list_vars: str,
                              tipo: Any = DoubleType()) -> None:
        """
        Run the transformation using one or many udf's
        Parameters
        ----------
        name: string
            Name of the transformation to perform
        key: list
            List for all the names to define udf
        transformation: list
            List of all transformations to define udf
        list_vars: string
            List of vars to use, can be lista_metodo or list_mvars
        tipo: pyspark type
            Type to return in udf
        """
        return self.parent._runUdfTransformation(name, key, transformation, list_vars, tipo)
    def _evaluate_path(self, path: str) -> bool:
        """
        Evaluate if certain path exists in hadoop
        Parameters
        ----------
        :path: string
            path to check if exists
        :Returns:
        --------
        Boolean
            True if path exist false if not
        """
        return self.parent._evaluate_path(path)


class StandardMethodsMT(ExternalTransformations):
    def _mean_mtTransformation(self) -> None:
        """
        Calculates the mean_mts

        :math:`\\frac{1}{n} \\sum_{i=0}^{n} x_{i}`

        using group of <period> months defined in class
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def mean_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            return float(np.mean(x))
        self._runUdfTransformation("mean_mt", ['mean_mt'], [mean_mt], "list_mvars", DoubleType())
    def _std_mtTransformation(self) -> None:
        """
        Calculates the standard deviation using the formula

        :math:`\\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} (x_{i}-\\mu)^2}`

        where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def std_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) <= 2:
                return 0.0
            return float(np.std(x, ddof = 1))
        self._runUdfTransformation("std_mt", ['std_mt'], [std_mt], "list_mvars", DoubleType())
    def _sum_mtTransformation(self) -> None:
        """
        Calculates the minimum

        :math:`\\sum_{i=1}^{n} x_{i}`

        using group of <period> months defined in class.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def sum_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            return float(np.sum(x))
        self._runUdfTransformation("sum_mt", ['sum_mt'], [sum_mt], "list_mvars", DoubleType())
    def _min_mtTransformation(self) -> None:
        """
        Calculates the minimum using group of <period> months defined in class.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def min_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            return float(np.min(x))
        self._runUdfTransformation("min_mt", ['min_mt'], [min_mt], "list_mvars", DoubleType())
    def _max_mtTransformation(self) -> None:
        """
        Calculates the maximum using group of <period> months defined in class.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def max_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            return float(np.max(x))
        self._runUdfTransformation("max_mt", ['max_mt'], [max_mt], "list_mvars", DoubleType())
    def _logmean_mtTransformation(self) -> None:
        """
        Calculates the logarithm of the mean :math:`log(\\mu)` if the mean is higher than 1
        else returns 0, using group of <period> months defined in class
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def logmean_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            mean_value = float(np.mean(x))
            if mean_value>0:
                return float(np.log(mean_value))
            return (-1e3)
        self._runUdfTransformation("logmean_mt", ['logmean_mt'], [logmean_mt], "list_mvars", DoubleType())
    def _square_mtTransformation(self) -> None:
        """
        Calculates the square_mt of the mean :math:`\\mu^{2}` using group of <period> months defined in class.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def square_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            mean_value = float(np.mean(x))
            return float(mean_value)**2
        self._runUdfTransformation("square_mt", ['square_mt'], [square_mt], "list_mvars", DoubleType())
    def _inverse_mtTransformation(self) -> None:
        """
        Calculates the inverse_mt of the mean :math:`\\frac{1}{\\mu}` using group of <period> months defined in class.
        If mean equals to zero the returns a high value (1e5)
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def inverse_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            mean_value = float(np.mean(x))
            if mean_value != 0.0:
                return 1/mean_value
            else:
                return 1e5
        self._runUdfTransformation("inverse_mt" ,['inverse_mt'], [inverse_mt], "list_mvars", DoubleType())
    def _trend_mtTransformation(self) -> None:
        """
        Calculates the coefficient :math:`b` of the linear regresion

        :math:`y = a + bx`

        using group of <period> months defined in class.

        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def trend_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            n = len(x)
            y = len(x)
            sum_xy = 0.0
            sum_x_sq = 0.0
            sum_x = 0.0
            sum_y = 0.0
            for z in x:
                xy = float(y*z)
                x_sq=float(y*y)
                x = float(y)
                sum_xy += xy
                sum_x_sq += x_sq
                sum_x += x
                sum_y += float(z)
                y-=1
            num = float(((n*sum_xy)-(sum_x*sum_y)))
            den = float(((n*sum_x_sq)-(sum_x*sum_x)))
            if den != 0.0:
                trend_mt = float(num/den)
            else:
                trend_mt = float(num*1e5)
            return(trend_mt)
        self._runUdfTransformation("trend_mt",['trend_mt'], [trend_mt], "list_mvars", DoubleType())
    def _rmeans_mtTransformation(self) -> None:
        """
        Calculates the ratios of means using actual vs previous, for example, for period of 3 months
        and 12 months of information calculates the ratio of the mean of months 4, 5 and 6 and the mean
        of months 1,2 and 3, also calculates the ratio of mean of months 7, 8, 9, 10, 11 and 12 and the mean
        of months 1,2,3,4,5 and 6.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def rmeans_mt(x: Sequence[float], y: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            y = [val for val in y if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(y) == 0:
                return -99999.0
            m_1 = np.mean(x)
            m_2 = np.mean(y)
            if m_2 != 0.0:
                r_mean = float(m_1 / m_2)
            else:
                r_mean = float(m_1 * 1e5)
            return(r_mean)
        #print("Running rmeans_mt transformation")
        logging.info("Running Rmeans_mt transformations")
        n = len(self.lista_metodo[0]) // self.period
        m = 1
        lista_rmeans_mt = []
        while 2*m <= n:
            lista_rmeans_mt += [[self.lista_metodo[k][:m * self.period],self.lista_metodo[k][m * self.period:2 * m *self.period],
            self.lista_metodo[k][0][:self.lista_metodo[k][0].rindex("_")]] for k in range(len(self.lista_metodo))]
            m += 1
        self.sqlContext.udf.register("rmeans_mt", rmeans_mt, DoubleType())
        query = "select id_msf_tools_v2_12315, " + ",".join(["rmeans_mt(array(" + ",".join(inner_list[0]) + "),array(" + ",".join(inner_list[1]) + ")) as rmeans_mt_" + inner_list[2] + "_" + str(len(inner_list[0])) +  self.frequency_sufix for inner_list in lista_rmeans_mt])+ self.FROM_DF
        df_r_means = self.sqlContext.sql(query)
        self._write_parquet(df_r_means, self.path, "rmeans_mt_features")
        df_r_means.unpersist()
        #print("Ending rmeans_mt transformation")
        logging.info("Ending Rmeans_mt transformations")
    def _incremental_mtTransformation(self) -> None:
        """
        Calculates how incremental_mt is the variable among the group of months using <period> according to the formula:

        :math:`\\sum_{i=1}^{n-1} {(1 \\quad \\text{if} \\quad x_{i+1} > x_{i} \\quad \\text{else} \\quad 0)}`

        where :math:`\\mu` is the mean, using group of <period> months defined in class
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def incremental_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            x.reverse()
            const_inc=0.0
            for i in range(len(x)-1):
                if x[i+1]<x[i]:
                    const_inc += 1
            return(const_inc)
        #print("Running incremental_mt transformations")
        logging.info("Running Incremental_mt transformations")
        n = len(self.lista_metodo[0])
        m = 1
        lista_inc = []
        while self.period * m < n-1:
            lista_inc += [self.lista_metodo[k][:self.period * m + 1] for k in range(len(self.lista_metodo))]
            m += 1
        self.sqlContext.udf.register("incremental_mt", incremental_mt, DoubleType())
        query = self.SELECT_ID + ','.join(["incremental_mt(array(" + ','.join(inner_hist) + self.CLOSE_AS + "i" + str(len
        (inner_hist)-1) + "_" +\
            inner_hist[0][:inner_hist[0].rindex("_")] for inner_hist in lista_inc])  + self.FROM_DF
        df_incremental_mt = self.sqlContext.sql(query)
        self._write_parquet(df_incremental_mt, self.path, "incremental_mt_features")
        df_incremental_mt.unpersist()
        #print("Ending incremental_mt transformations")
        logging.info("Ending Incremental_mt transformations")

    def _vp_mtTransformation(self) -> None:
        """
        Calculates the mean and the standard deviation of the percental variations of all months defined by the next formula

        :math:`\\frac{x_{i+1}-x_{i}}{x_{i}}`

        If the denominator equals to 0 then the denominator is replaced by a small value (1e-5)
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def means_vp_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            list_vp_mt=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp_mt = float((x[t+1]-x[t])/x[t])
                else:
                    vp_mt = float((x[t+1]-x[t])*1e5)
                list_vp_mt.append(vp_mt)
                t+=1
            m = float(np.mean(list_vp_mt))
            return m
        def std_vp_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) <= 3:
                return 0.0
            #
            list_vp_mt=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    vp_mt = float((x[t+1]-x[t])/x[t])
                else:
                    vp_mt = float((x[t+1]-x[t])*1e5)
                list_vp_mt.append(vp_mt)
                t+=1
            sd=float(np.std(list_vp_mt, ddof = 1))
            return sd
        self._runUdfTransformation("vp_mt", ['means_vp_mt','std_vp_mt'], [means_vp_mt, std_vp_mt], "lista_metodo", DoubleType())

    def _rt_mtTransformation(self) -> None:
        """
        Calculates the mean and the standard deviation of the ratios of all months defined by the next formula

        :math:`\\frac{x_{i+1}}{x_{i}}`

        If the denominator equals to 0 then the denominator is replaced by a small value (1e-5)
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def means_rt_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            list_ratios=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t+=1
            m=float(np.mean(list_ratios))
            return m

        def std_rt_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) <= 3:
                return 0.0
            #
            list_ratios=list()
            t=0
            while t + 1 < len(x):
                if x[t] > 0.0:
                    ratio = float(x[t+1]/x[t])
                else:
                    ratio = float(x[t+1]*1e5)
                list_ratios.append(ratio)
                t+=1
            sd=float(np.std(list_ratios, ddof = 1))
            return sd
        self._runUdfTransformation("rt_mt", ['means_rt_mt','std_rt_mt'], [means_rt_mt, std_rt_mt], "lista_metodo", DoubleType())
    def _so_mtTransformation(self) -> None:
        """
        Calculates stochastic oscillator using groups of months defined by <period> according the formula:

        :math:`\\frac{\\mu-x_{min}}{x_{max}-x_{min}}`

        If the denominator equals to 0 then the return value is 1.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def stochastic_oscillator(x: Sequence[float]) -> float | Any:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            media=sum(x)/len(x)
            minimo=min(x)
            maximo=max(x)
            num=(media - minimo)
            den=(maximo - minimo)
            if den != 0.0:
                so_mt = num/den
            else:
                so_mt = 1.0
            return(so_mt)
        self._runUdfTransformation("so_mt", ['so_mt'], [stochastic_oscillator], "list_mvars", DoubleType())
    def _avgmov_mtTransformation(self) -> None:
        """
        Calculates the movil averages across the group of months
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def avg_aux(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            cont=0.0
            num1=0.0
            den1=0.0
            for var in x:
                num1+=float(var)/float(cont+1)
                den1+=1/float(cont+1)
                cont=cont+1
            return(float(num1)/float(den1))
        self._runUdfTransformation("avgmov_mt", ['avgmov_mt'], [avg_aux], "list_mvars", DoubleType())
    def _run_ratios_mt(self, tran_rt_mt: Tuple[str, str]) -> None:
        """
        Perform the ratios transformation of ratiosmod_mt
        Parameters
        ----------
        :tran_rt_mt: tuple
            Tuple with pair numerator/denominator
        """
        name_ratio = tran_rt_mt[0] + tran_rt_mt[1]
        df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt_mt[0]+"_features"))
        df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_rt_mt[1]+"_features"))
        list_num = df_num.drop("id_msf_tools_v2_12315").columns
        list_den = df_den.drop("id_msf_tools_v2_12315").columns
        list_rnumden = list()
        # el prefijo es `max_mt`/`min_mt`...: hay que cortar por su longitud
        # (index("_") cortaría tras "max" dejando "_mt_...")
        prefix_len = len(tran_rt_mt[0])
        for var_num in list_num:
            var_name = var_num[prefix_len:]
            var_den = tran_rt_mt[1] + var_name
            if var_den in list_den:
                list_rnumden.append([var_num, var_den])
        if not list_rnumden:
            logging.info("ratiosmod_mt %s: sin columnas comunes, se omite",
                         name_ratio)
            return
        df = df_num.join(df_den, df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315).drop(df_den.id_msf_tools_v2_12315)
        app_rt_mt_mod_rnumden = self.parent.SELECT_ID + ', '.join(["rt_mt_mod(array(" + ','.join(inner_rnumden) + self.parent.CLOSE_AS + f"r{name_ratio}_" + inner_rnumden[0][prefix_len+1:] for inner_rnumden in list_rnumden]) + " from df_aux_r" + name_ratio
        #self.sqlContext.registerDataFrameAsTable(df, "df_aux_r" + name_ratio)
        df.createOrReplaceTempView("df_aux_r" + name_ratio)
        df_aux = self.sqlContext.sql(app_rt_mt_mod_rnumden)
        self._write_parquet(df_aux, self.path, f"r{name_ratio}_features")
        df_aux.unpersist()
    def _run_deltas_mt(self, tran_d: Tuple[str, str]) -> None:
        """
        Perform the ratios transformation of ratiosmod_mt
        Parameters
        ----------
        :tran_d: tuple
            Tuple with pair (a,b) to make a-b
        """
        name_d = tran_d[0] + tran_d[1]
        df_num = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[0]+"_features"))
        df_den = self.sqlContext.read.parquet(os.path.join(self.path,tran_d[1]+"_features"))
        list_p = df_num.drop("id_msf_tools_v2_12315").columns
        list_m = df_den.drop("id_msf_tools_v2_12315").columns
        list_dpm = list()
        # corte por longitud de prefijo (`max_mt`, no `max`)
        prefix_len = len(tran_d[0])
        for var_p in list_p:
            var_name = var_p[prefix_len:]
            var_m = tran_d[1] + var_name
            if var_m in list_m:
                list_dpm.append([var_p, var_m])
        if not list_dpm:
            logging.info("ratiosmod_mt deltas %s: sin columnas comunes, se omite",
                         name_d)
            return
        df = df_num.join(df_den, df_num.id_msf_tools_v2_12315 == df_den.id_msf_tools_v2_12315).drop(df_den.id_msf_tools_v2_12315)
        app_d_mod_dpm = self.SELECT_ID + ', '.join(["df_mt_mod(array(" + ','.join(inner_dpm) + self.CLOSE_AS + f"d{name_d}_" +\
            inner_dpm[0][prefix_len+1:] for inner_dpm in list_dpm]) + " from df_aux_d" + name_d
        #self.sqlContext.registerDataFrameAsTable(df, "df_aux_d" + name_d)
        df.createOrReplaceTempView("df_aux_d" + name_d)
        df_aux = self.sqlContext.sql(app_d_mod_dpm)
        self._write_parquet(df_aux, self.path, f"d{name_d}_features")
        df_aux.unpersist()

    def _ratiosmod_mtTransformation(self) -> None :
        """
        Calculates ratios and deltas between the combinations of min, max and mean of all variables.
        This transformation need the mean, min and max transformations.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def rt_mt_mod(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) < 2:
                return -99999.0
            if x[1] != 0.0:
                rt_mt=float((x[0] / x[1]))
            else:
                rt_mt = 0.0
            return rt_mt
        def df_mt_mod(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) < 2:
                return -99999.0
            dif = float((x[0] - x[1]))
            return dif
        #print("Running ratiosmod_mt transformation")
        logging.info("Running Ratiosmod_mt transformations")
        self.sqlContext.udf.register("rt_mt_mod", rt_mt_mod, DoubleType())
        self.sqlContext.udf.register("df_mt_mod", df_mt_mod, DoubleType())
        elemental_trans = ["min_mt", "max_mt", "mean_mt", "std_mt", "sum_mt"]
        trans_rt_mt = [("min_mt","max_mt"),("min_mt","mean_mt"),("max_mt","mean_mt"),("std_mt","sum_mt"),("std_mt","mean_mt")]
        trans_d_mt = [("max_mt","min_mt"),("mean_mt","min_mt"),("max_mt","mean_mt")]
        not_performed_trans = []
        for trans in elemental_trans:
            if not is_successful(os.path.join(self.path, trans + "_features")):
                for element in trans_rt_mt.copy():
                    if trans in element:
                        trans_rt_mt.remove(element)
                for element in trans_d_mt.copy():
                    if trans in element:
                        trans_d_mt.remove(element)
        thread_pool = ThreadPool(self.number_threads)
        fun_1 = partial(self._run_ratios_mt)
        fun_2 = partial(self._run_deltas_mt)
        thread_pool.map(fun_1, trans_rt_mt)
        thread_pool.map(fun_2, trans_d_mt)
        thread_pool.close()
        #print("Ending ratiosmod_mt transformation")
        logging.info("Ending Ratiosmod_mt transformations")
    def _rms_mtTransformation(self) -> None:
        """
        Calculates root mean square using groups of months defined by <period> according the formula:

        :math:`\\sqrt{\\frac{1}{n} \\sum_{i=1}^{n} x_{i}^2}`

        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def rms_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            return float(np.sqrt(np.mean(np.square(x))))
        self._runUdfTransformation("rms_mt", ['rms_mt'], [rms_mt], "list_mvars", DoubleType())
    def _cmpx_mtTransformation(self) -> None:
        """
        Calculates complexity of all months using the formula:

        :math:`\\sqrt{\\sum_{i=1}^{n} (x_{i+1}-x_{i})^2}`

        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def complexity(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            x = np.diff(x)
            return float(np.sqrt(np.dot(x, x)))
        self._runUdfTransformation("cmpx_mt", ['cmpx_mt'], [complexity], "list_mvars", DoubleType())
    def _skew_mtTransformation(self) -> None:
        """
        Returns the sample skew_mtness across all the months(calculated with the adjusted Fisher-Pearson
        standardized moment coefficient G1)
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def skew_mtness(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) < 4:
                value = 0.0
            else:
                x = pd.Series(x)
                value = float(pd.Series.skew(x))
            return value
        self._runUdfTransformation("skew_mt", ['skew_mt'], [skew_mtness], "list_mvars", DoubleType())
    def _curt_mtTransformation(self) -> None:
        """
        Returns the kurtosis of all months (calculated with the adjusted Fisher-Pearson standardized
        moment coefficient G2)
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def curt_mtosis(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) < 4:
                value = 0.0
            else:
                x = pd.Series(x)
                value = float(pd.Series.kurtosis(x))
            return value
        self._runUdfTransformation("curt_mt", ['curt_mt'], [curt_mtosis], "list_mvars", DoubleType())
    def _weighted_mean_mtTransformation(self) -> None:
        """
        Calculates the weighted mean using the weights defined in the class
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def create_weighted_mean_functions(weights: Sequence[float], ignore_value: float) -> Callable[..., float]:
            def weighted_mean_mt(x: Sequence[float]) -> float:
                # ignore values
                filtered_x = []
                filtered_weights = []
                for i, val in enumerate(x):
                    if val != ignore_value:
                        filtered_x.append(val)
                        filtered_weights.append(weights[i])
                #
                if len(filtered_x) == 0:
                    return -99999.0
                return float(np.average(filtered_x, weights=filtered_weights))
            return weighted_mean_mt
        name = "weighted_mean_mt"
        keys = [f"weighted_mean_mt_{convert_to_three_digit_string(last_weight)}" for last_weight, weights in self.parent.
        weights_dict.items() ]
        transformations = [create_weighted_mean_functions(weights, ignore_value) for last_weight, weights in self.parent.
        weights_dict.items()]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())

def convert_to_three_digit_string(value: float) -> str:
    # Multiplicar el valor por 1000 y convertirlo a entero
    int_value = int(value * 1000)
    # Formatear el entero a una cadena de tres dígitos con ceros a la izquierda
    formatted_value = f"{int_value:03d}"
    return formatted_value

class OnesAndZerosMethods(ExternalTransformations):
    def _entropy_onzTransformation(self) -> None:
        """
        Shannon entropy is a measure of the uncertainty or randomness in a set of data. It quantifies the average amount of
        information produced by a stochastic source of data.

        The formula for Shannon entropy, as used in the selected function, is:

        [ H(X) = -\\sum_{i=1}^{n} p(x_i) \\log_2 p(x_i) ]

        where:

        ( H(X) ) is the entropy,
        ( p(x_i) ) is the probability of occurrence of each unique value ( x_i )
        in the dataset.
        """
        def entropy_onz(x: Sequence[float]) -> float:
            # Calcula la frecuencia de cada valor en el array
            values, counts = np.unique(x, return_counts=True)
            probabilities = counts / counts.sum()
            # Calcula la entropía de Shannon
            entropy = float(-np.sum(probabilities * np.log2(probabilities)))
            if entropy == -0.0:
                entropy = 0.0
            return float(entropy)
        #
        self._runUdfTransformation("entropy_onz", ['entropy_onz'], [entropy_onz], "list_mvars", DoubleType())
    # Payroll Streaks functions para buscar en cadenas de 0s y 1s y 9s
    def _first_1_onzTransformation(self) -> None:
        """
        Is the position of the first one, starting in 1.

        Examples:
        -   [0,1,0,1,1] -> 2
        -   [0,0,0,0,0] -> 0
        -   [] -> -99999
        """
        default = self.parent.default_non_found_value
        def first_1_onz(x: Sequence[float]) -> float:
            try:
                return float(x.index(1) + 1)
            except:
                return float(default)
        self._runUdfTransformation("first_1_onz", ['first_1_onz'], [first_1_onz], "list_mvars", DoubleType())
    def _longest_1_streak_onzTransformation(self) -> None:
        """The length of the longest one's streak.
        """
        default = self.parent.default_non_found_value
        def longest_1_streak_onz(x: Sequence[float]) -> float:
            # Encuentra todas las cadenas de 1s
            streaks = [len(list(group)) for key, group in it_groupby(x) if key == 1]
            # Devuelve la longitud de la cadena más larga o 0 si no hay cadenas de 1s
            return float(max(streaks, default=default))
        self._runUdfTransformation("longest_1_streak_onz", ['longest_1_streak_onz'], [longest_1_streak_onz], "list_mvars",
        DoubleType())

    def _first_streaks_onzTransformation(self) -> None:
        """This function creates and applies a series of transformations to identify the first occurrence of a streak of '1's
        of varying lengths within a list. If the streak is found, it returns the position of the first element of the streak.
        If the streak is not found or the list is too short, it returns a default value.
        """
        default = self.parent.default_non_found_value
        range_ = range(2, 8)
        def create_streak_function(length: int, default: float) -> Callable[..., float]:
            def first_lo1_streak_onz(x: Sequence[float]) -> float:
                if len(x) < length:
                    return float(default)  # Devuelve -1 si la longitud de x es menor que length
                count = 0
                for i, value in enumerate(x):
                    if value == 1.0:
                        count += 1
                        if count == length:
                            return float(i - length + 2)
                    else:
                        count = 0
                return float(default)  # Devuelve -1 si no se encuentra ninguna secuencia de la longitud dada
            return first_lo1_streak_onz
        name = "first_streaks_onz"
        keys = [f"first_{length}o1_streak_onz" for length in range_]
        transformations = [create_streak_function(length, default) for length in range_]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())

    def _find_pattern_onzTransformation(self) -> None:
        patterns = [[1, 0, 0, 0], [0, 1, 0, 0]]
        default = self.parent.default_non_found_value
        def create_pattern_function(pattern: Sequence[int], pattern_length: int, default: float) -> Callable[..., float]:
            def first_pattern_onz(x: Sequence[float]) -> float:
                if len(x) < pattern_length:
                    return float(default)  # Devuelve -1 si la longitud de x es menor que el patrón
                for i in range(len(x) - pattern_length + 1):
                    if x[i:i + pattern_length] == pattern:
                        return float(i+1)
                return float(default)  # Devuelve -1 si no se encuentra el patrón
            return first_pattern_onz
        #
        name = "find_pattern_onz"
        keys = [f"first_{''.join(map(str, pattern))}_pattern_onz" for pattern in patterns]
        transformations = [create_pattern_function(pattern, len(pattern), default) for pattern in patterns]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())
    #
    def _transitions_onzTransformation(self) -> None:
        """This function creates and applies a transformation to count the number of transitions between different values
        in a list. A transition is defined as a change from one value to another in consecutive elements of the list.
        """
        default = self.parent.default_non_found_value
        def transitions_onz(x: Sequence[float]) -> float:
            if len(x) < 2:
                return default # No hay transiciones si la longitud de x es menor que 2
            x = np.array(x)
            transitions = np.sum(x[:-1] != x[1:])
            return float(transitions)
        self._runUdfTransformation("transitions_onz", ["transitions_onz"], [transitions_onz], "list_mvars", DoubleType())
    #
    def _autocorrelation_onzTransformation(self) -> None:
        """This function creates and applies a transformation to
        calculate the autocorrelation of a list, ignoring a specified value.
        Autocorrelation measures the similarity between a signal and a delayed
        version of itself over varying time intervals.
        """
        def autocorrelation_onz(x: Sequence[float]) -> float:
            if len(x) < 2:
                return 0.0  # No hay autocorrelación si la longitud de x es menor que 2
            x = np.array(x)
            x_mean = np.mean(x)
            x = x - x_mean
            result = np.correlate(x, x, mode='full')
            max_corr = result[result.size // 2:].max()
            return float(max_corr)
        self._runUdfTransformation("autocorrelation_onz", ["autocorrelation_onz"], [autocorrelation_onz], "list_mvars", DoubleType
        ())
    #
    def _kolmogorov_complexity_onzTransformation(self) -> None:
        """Also known as algorithmic complexity, is a measure of the
        complexity of a sequence of data. In simple terms, it refers to
        the length of the shortest possible program that can generate
        that sequence when run on a computer.

        Compressing a string is a practical way to approximate this
        measure because compression algorithms look for patterns and
        redundancies in the data to reduce its size.

        1.  The array is converted to a string: [0,1,0,1,1] -> 01011.
        2.  The string is compressed with zlib library from Python, which uses the DEFLATE algorithm.
        3.  The final compresed string is measured.
        """
        def kolmogorov_complexity_onz(x: Sequence[float]) -> float:
            if len(x) == 0:
                return 0.0  # No hay complejidad si la longitud de x es 0
            x_str = ''.join(map(str, x))
            compressed_length = len(zlib.compress(x_str.encode('utf-8')))
            return float(compressed_length)
        self._runUdfTransformation("kolmogorov_complexity_onz", ["kolmogorov_complexity_onz"], [kolmogorov_complexity_onz],
        "list_mvars", DoubleType())

    #
    def _pattern_diversity_onzTransformation(self) -> None:
        def pattern_diversity_onz(x: Sequence[float]) -> float:
            if len(x) == 0:
                return 0.0  # No hay diversidad si la longitud de x es 0
            x_str = ''.join(map(str, x))
            unique_patterns = set()
            for i in range(len(x_str)):
                for j in range(i + 1, len(x_str) + 1):
                    unique_patterns.add(x_str[i:j])
            return float(len(unique_patterns))
        self._runUdfTransformation("pattern_diversity_onz", ["pattern_diversity_onz"], [pattern_diversity_onz], "list_mvars",
        DoubleType())


class OnesAndZerosMethodsMT(ExternalTransformations):
    """This class contains all the methods to calculate the features
    with missing value removal gnore_this_value_when_calculating_mt_features
    """
    def _entropy_onz_mtTransformation(self) -> None:
        """

        Shannon entropy is a measure of the uncertainty or randomness in a set of data. It quantifies the average amount of
        information produced by a stochastic source of data.

        The formula for Shannon entropy, as used in the selected function, is:

        [ H(X) = -\\sum_{i=1}^{n} p(x_i) \\log_2 p(x_i) ]

        where:

        ( H(X) ) is the entropy.
        ( p(x_i) ) is the probability of occurrence of each unique value ( x_i )
        in the dataset.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def entropy_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            # Calcula la frecuencia de cada valor en el array
            values, counts = np.unique(x, return_counts=True)
            probabilities = counts / counts.sum()
            # Calcula la entropía de Shannon
            entropy = float(-np.sum(probabilities * np.log2(probabilities)))
            if entropy == -0.0:
                entropy = 0.0
            return float(entropy)
        self._runUdfTransformation("entropy_onz_mt", ['entropy_onz_mt'], [entropy_onz_mt], "list_mvars", DoubleType())
    # Payroll Streaks functions para buscar en cadenas de 0s y 1s y 9s
    def _first_1_onz_mtTransformation(self) -> None:
        """
        Is the position of the first one, starting in 1.

        Examples:
        -   [0,1,0,1,1] -> 2
        -   [0,0,0,0,0] -> 0
        -   [] -> -99999
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        default = self.parent.default_non_found_value
        def first_1_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            try:
                return float(x.index(1) + 1)
            except:
                return float(default)
        self._runUdfTransformation("first_1_onz_mt", ['first_1_onz_mt'], [first_1_onz_mt], "list_mvars", DoubleType())
    def _longest_1_streak_onz_mtTransformation(self) -> None:
        """The length of the longest one's streak.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        default = self.parent.default_non_found_value
        def longest_1_streak_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            # Encuentra todas las cadenas de 1s
            streaks = [len(list(group)) for key, group in it_groupby(x) if key == 1]
            # Devuelve la longitud de la cadena más larga o 0 si no hay cadenas de 1s
            return float(max(streaks, default=default))
        self._runUdfTransformation("longest_1_streak_onz_mt", ['longest_1_streak_onz_mt'], [longest_1_streak_onz_mt], "list_mvars",
        DoubleType())

    #
    def _first_streaks_onz_mtTransformation(self) -> None:
        """
        This function creates and applies a series of transformations to identify the first occurrence of a streak of '1's
        of varying lengths within a list, ignoring a specified value. If the streak is found, it returns the position of the
        first element of the streak. If the streak is not found or the list is too short, it returns a default value.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        default = self.parent.default_non_found_value
        range_ = range(2,8)
        def create_streak_function(length: int, default: float, ignore_value: float) -> Callable[..., float]:
            def first_lo1_streak_onz_mt(x: Sequence[float]) -> float:
                x = [val for val in x if val != ignore_value]
                if len(x) == 0:
                    return -99999.0
                if len(x) < length:
                    return float(default)  # Devuelve -1 si la longitud de x es menor que length
                count = 0
                for i, value in enumerate(x):
                    if value == 1.0:
                        count += 1
                        if count == length:
                            return float(i - length + 2)
                    else:
                        count = 0
                return float(default)  # Devuelve -1 si no se encuentra ninguna secuencia de la longitud dada
            return first_lo1_streak_onz_mt
        #
        name = "first_streaks_onz_mt"
        keys = [f"first_{length}o1_streak_onz_mt" for length in range_]
        transformations = [create_streak_function(length, default, ignore_value) for length in range_]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())

    def _find_pattern_onz_mtTransformation(self) -> None:
        patterns = [[1, 0, 0, 0], [0, 1, 0, 0]]
        default = self.parent.default_non_found_value
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def create_pattern_function(pattern: Sequence[int], pattern_length: int, default: float, ignore_value: float) -> Callable[..., float]:
            def first_pattern_onz_mt(x: Sequence[float]) -> float:
                x = [val for val in x if val != ignore_value]
                if len(x) == 0:
                    return -99999.0
                if len(x) < pattern_length:
                    return float(default)  # Devuelve -1 si la longitud de x es menor que el patrón
                for i in range(len(x) - pattern_length + 1):
                    if x[i:i + pattern_length] == pattern:
                        return float(i+1)
                return float(default)  # Devuelve -1 si no se encuentra el patrón
            return first_pattern_onz_mt
        #
        name = "find_pattern_onz_mt"
        keys = [f"first_{''.join(map(str, pattern))}_pattern_onz_mt" for pattern in patterns]
        transformations = [create_pattern_function(pattern, len(pattern), default, ignore_value) for pattern in patterns]
        self._runUdfTransformation(name, keys, transformations, "list_mvars", DoubleType())
    #
    def _transitions_onz_mtTransformation(self) -> None:
        """This function creates and applies a transformation to count the number of transitions between different values
        in a list. A transition is defined as a change from one value to another in consecutive elements of the list.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        default = self.parent.default_non_found_value
        def transitions_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) < 2:
                return default # No hay transiciones si la longitud de x es menor que 2
            x = np.array(x)
            transitions = np.sum(x[:-1] != x[1:])
            return float(transitions)
        self._runUdfTransformation("transitions_onz_mt", ["transitions_onz_mt"], [transitions_onz_mt], "list_mvars", DoubleType())
    #
    def _kolmogorov_complexity_onz_mtTransformation(self) -> None:
        """Also known as algorithmic complexity, is a measure of the
        complexity of a sequence of data. In simple terms, it refers to
        the length of the shortest possible program that can generate
        that sequence when run on a computer.

        Compressing a string is a practical way to approximate this
        measure because compression algorithms look for patterns and
        redundancies in the data to reduce its size.

        1.  The array is converted to a string: [0,1,0,1,1] -> 01011.
        2.  The string is compressed with Zlib library from Python, which uses the DEFLATE algorithm.
        3.  The final compresed string is measured.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def kolmogorov_complexity_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            x_str = ''.join(map(str, x))
            compressed_length = len(zlib.compress(x_str.encode('utf-8')))
            return float(compressed_length)
        self._runUdfTransformation("kolmogorov_complexity_onz_mt", ["kolmogorov_complexity_onz_mt"],
        [kolmogorov_complexity_onz_mt], "list_mvars", DoubleType())

    #
    def _autocorrelation_onz_mtTransformation(self) -> None:
        """This function creates and applies a transformation to
        calculate the autocorrelation of a list, ignoring a specified value.
        Autocorrelation measures the similarity between a signal and a delayed version of itself over varying time intervals.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def autocorrelation_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) < 2:
                return 0.0  # No hay autocorrelación si la longitud de x es menor que 2
            x = np.array(x)
            x_mean = np.mean(x)
            x = x - x_mean
            result = np.correlate(x, x, mode='full')
            max_corr = result[result.size // 2:].max()
            return float(max_corr)
        self._runUdfTransformation("autocorrelation_onz_mt", ["autocorrelation_onz_mt"], [autocorrelation_onz_mt], "list_mvars",
        DoubleType())
    #
    def _pattern_diversity_onz_mtTransformation(self) -> None:
        """
        This function creates and applies a transformation to calculate
        the pattern diversity of a list, ignoring a specified value.

        Pattern diversity measures the number of unique substrings
        within the list when treated as a string.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def pattern_diversity_onz_mt(x: Sequence[float]) -> float:
            x = [val for val in x if val != ignore_value]
            if len(x) == 0:
                return -99999.0
            if len(x) == 0:
                return 0.0  # No hay diversidad si la longitud de x es 0
            x_str = ''.join(map(str, x))
            unique_patterns = set()
            for i in range(len(x_str)):
                for j in range(i + 1, len(x_str) + 1):
                    unique_patterns.add(x_str[i:j])
            return float(len(unique_patterns))
        self._runUdfTransformation("pattern_diversity_onz_mt", ["pattern_diversity_onz_mt"], [pattern_diversity_onz_mt],
        "list_mvars", DoubleType())
    #
    def _is_new_onz_mtTransformation(self) -> None:
        """
        This function creates and applies a transformation to determine
        whether a list exhibits a specific pattern based on its values,
        ignoring a specified value.

        The transformation checks if the sum of the first portion of the
        list (after ignoring the specified value) is greater than 0, while
        the sum of the remaining portion is 0. If this condition is met,
        the function assigns a value of 1.0; otherwise, it assigns 0.0.

        The length of the first portion is calculated proportionally based
        on the ratio of the original list length to the length after
        removing the ignored values.
        """
        ignore_value = self.parent.ignore_this_value_when_calculating_mt_features
        def is_new_onz_mt(x: Sequence[float]) -> float:
            len_previous_mt = len(x)
            x = [val for val in x if val != ignore_value]
            len_after_mt = len(x)
            first_month_index = round(4/(len_previous_mt/len_after_mt))
            sum_first_month = sum(x[:first_month_index])
            sum_else = sum(x[first_month_index:])
            return 1.0 if (sum_first_month > 0.0) & (sum_else == 0.0) else 0.0
        #
        self._runUdfTransformation("is_new_onz_mt", ["is_new_onz_mt"], [is_new_onz_mt], "list_mvars", DoubleType())
