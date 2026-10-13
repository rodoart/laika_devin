"""
Project: Payroll Streaks Mobs
Developer: @rg49392
Update Date: 2025-02-21
Version: 0.1

Description:
This file contains the functions to fill the features with the information of the previous month.
"""

# Libraries
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from .payroll_income import PreviousFixerPayroll

# Log configuration
logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class PreviousFixerPayrollStreaks(PreviousFixerPayroll):
    def __init__(
        self,
        path_mobs_fe:str,
        features_path:str,
        write_mobs:str,
        feat_list:List[List[str]],
        periodo:int,
        n_mis:int,
        n_threads:int,
        sql_context:SparkSession,
        mb_ty:Optional[List[str]] = ["contiguous_mobs_tefs", "discontiguous_mobs_tefs", "contiguous_mobs_tef_1",
        "discontiguous_mobs_tef_1", "contiguous_mobs_tef_2", "discontiguous_mobs_tef_2"],
        is_dynamic:Optional[bool] = False,
        mode:Optional[str] = "overwrite"
    ) -> None:
        """
        Inicializa una instancia de la clase con los parámetros proporcionados.

        Args:
            path_mobs_fe (str): Ruta para los archivos de mobs.
            features_path (str): Ruta para los archivos de características.
            write_mobs (str): Ruta para escribir los archivos de mobs imputados.
            feat_list (List[List[str]]): Lista de listas de características.
            periodo (int): Periodo para el cálculo de mobs.
            n_mis (int): Número de meses de información.
            n_threads (int): Número de hilos para la ejecución en paralelo.
            sql_context (SparkSession): Contexto de Spark para la lectura y escritura de datos.
            mb_ty (Optional[List[str]], optional): Lista de tipos de mobs. Por defecto es ["mobs_tef_1", "mobs_tef_2", "mobs_tefs"].
            is_dynamic (Optional[bool], optional): Indicador de si el cálculo es dinámico. Por defecto es False.
            mode (Optional[str], optional): Modo de escritura de los archivos. Por defecto es "overwrite".

        Returns:
            None
        """
        super().__init__(
            path_mobs_fe=path_mobs_fe,
            features_path=features_path,
            write_mobs=write_mobs,
            feat_list=feat_list,
            periodo=periodo,
            n_mis=int(n_mis)*4,
            n_threads=n_threads,
            sql_context=sql_context,
            mb_ty=mb_ty,
            is_dynamic=is_dynamic,
            mode=mode
        )

    @staticmethod
    def payroll_mob_imputation(df: DataFrame, features: List[str], lista_meses:List[int], reverse:Optional[bool]=False) -> DataFrame:#
        """
        Cambia las variables de los features de acuerdo a las reglas:
        1. Si la variable discontiguous_mobs_tefs es 0, entonces la variable es -99999.

        Args:
            df (DataFrame): El DataFrame de PySpark que contiene los datos.
            features (list): Lista de nombres de características.
            lista_meses (list): Lista de meses.
            reverse (bool): Si es True, procesa de menor a mayor, de lo contrario, de mayor a menor.

        Returns:
            DataFrame: El DataFrame procesado.
        """
        if reverse:
            lista_meses = sorted(lista_meses)
        else:
            lista_meses = sorted(lista_meses, reverse=True)
        #
        for feature in features:
            for mes in lista_meses:
                column_name = f"{feature}_{mes}m"
                if 'tefs' in feature:
                    df = df.withColumn(column_name, when(col('discontiguous_mobs_tefs') == 0, -99999).otherwise(col(column_name)))
                elif 'tef_1' in feature:
                    df = df.withColumn(column_name, when(col('discontiguous_mobs_tef_1') == 0, -99999).otherwise(col(column_name)))
                elif 'tef_2' in feature:
                    df = df.withColumn(column_name, when(col('discontiguous_mobs_tef_2') == 0, -99999).otherwise(col(column_name)))
                else:
                    df = df.withColumn(column_name, when(col('discontiguous_mobs_tefs') == 0, -99999).otherwise(col(column_name)))
                #
        #
        df.drop("contiguous_mobs_tefs", "discontiguous_mobs_tefs", "contiguous_mobs_tef_1", "discontiguous_mobs_tef_1",
        "contiguous_mobs_tef_2", "discontiguous_mobs_tef_2")
        return df
