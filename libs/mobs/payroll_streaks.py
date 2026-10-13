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
    """Imputador de MOBs para ``payroll_streaks``.

    Solo difiere de la base en:

    - :attr:`DEFAULT_MB_TY` — usa las columnas contiguas/discontiguas del
      parquet ``calculate_mobs``.
    - :attr:`N_MIS_FACTOR = 4` — sus meses están forzados a 4 semanas, así
      que ``n_mis`` se multiplica por 4.
    - :meth:`payroll_mob_imputation` — regla ``discontiguous_mobs_* == 0``
      -> ``-99999``.
    """

    #: Columnas de mobs por defecto del parquet ``calculate_mobs``.
    DEFAULT_MB_TY = ["contiguous_mobs_tefs", "discontiguous_mobs_tefs",
                     "contiguous_mobs_tef_1", "discontiguous_mobs_tef_1",
                     "contiguous_mobs_tef_2", "discontiguous_mobs_tef_2"]
    #: Meses forzados a 4 semanas -> ``n_mis`` se multiplica por 4.
    N_MIS_FACTOR = 4
    #: Columnas de mobs que se eliminan del DataFrame de salida.
    MOBS_DROP_COLUMNS = DEFAULT_MB_TY

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
        lista_meses = PreviousFixerPayroll._sorted_meses(lista_meses, reverse)
        #
        for feature in features:
            mobs_col = f"discontiguous_mobs_{PreviousFixerPayroll._mobs_key(feature)}"
            for mes in lista_meses:
                column_name = f"{feature}_{mes}m"
                df = df.withColumn(column_name, when(col(mobs_col) == 0, -99999).otherwise(col(column_name)))
        #
        df = df.drop("contiguous_mobs_tefs", "discontiguous_mobs_tefs", "contiguous_mobs_tef_1", "discontiguous_mobs_tef_1",
        "contiguous_mobs_tef_2", "discontiguous_mobs_tef_2")
        return df
