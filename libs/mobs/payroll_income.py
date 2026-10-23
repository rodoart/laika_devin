"""
Project: Payroll Income Mobs
Developer: @rg49392
Update Date: 2025-02-21
Version: 0.1

Description:
This file contains the functions to fill the features with the information of the previous month.
"""

# Libraries
import logging
import os
import re
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from ..aggregattes.payroll_income import cached_property
from ..hive_path import is_successful
from ..steps import save_parquet

# Log configuration
logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


def get_months(periodo: Union[str, int], n_mis: Union[str, int]) -> List[int]:
    """Months period list ``[3,6,9,12]``, ``[2,4,6,...]``, etc.

    Multiples of ``periodo`` up to ``n_mis`` (always ending exactly at
    ``n_mis`` even when it is not a multiple).
    """
    periodo = int(periodo)
    n_mis = int(n_mis)
    months_list = []
    for month in range(0, n_mis, periodo):
        if (month+periodo) < n_mis:
            months_list.append(month+periodo)
        else:
            months_list.append(n_mis)
    return months_list


class PreviousFixerPayroll:
    """Base de los imputadores de features por MOBs de las fuentes payroll.

    Las subclases solo necesitan sobreescribir:

    - :attr:`DEFAULT_MB_TY` — columnas de mobs que se leen del parquet
      ``calculate_mobs``.
    - :attr:`N_MIS_FACTOR` — multiplicador de ``n_mis`` (``4`` en streaks
      porque sus meses están forzados a 4 semanas).
    - :meth:`payroll_mob_imputation` — las reglas de imputación.
    """
    auxiliar_id = "id_msf_tools_v2_12315"
    id_column = "numcliente"
    vintage_column = "vintage_date"
    month_suffix_pattern = r"_\d+m$"

    #: Columnas de mobs por defecto del parquet ``calculate_mobs``.
    DEFAULT_MB_TY = ["mobs_tef_1", "mobs_tef_2", "mobs_tefs"]
    #: Multiplicador de ``n_mis`` (4 en streaks: meses de 4 semanas).
    N_MIS_FACTOR = 1
    #: Columnas de mobs que se eliminan del DataFrame de salida.
    MOBS_DROP_COLUMNS = DEFAULT_MB_TY

    def __init__(
        self,
        path_mobs_fe: str,
        features_path: str,
        write_mobs: str,
        feat_list: List[List[str]],
        periodo: int,
        n_mis: int,
        n_threads: int,
        sql_context: SparkSession,
        mb_ty: Optional[List[str]] = None,
        is_dynamic: Optional[bool] = False,
        mode: Optional[str] = "overwrite"
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
            mb_ty (Optional[List[str]], optional): Lista de tipos de mobs. Por defecto
                es ``DEFAULT_MB_TY`` de la clase.
            is_dynamic (Optional[bool], optional): Indicador de si el cálculo es dinámico. Por defecto es False.
            mode (Optional[str], optional): Modo de escritura de los archivos. Por defecto es "overwrite".

        Returns:
            None
        """
        # Required attributes
        self.path_mobs_fe = path_mobs_fe
        self.features_path = features_path
        self.write_mobs = write_mobs
        self.feat_list = feat_list
        self.periodo = int(periodo)
        self.n_mis = int(n_mis) * self.N_MIS_FACTOR
        self.n_threads = int(n_threads)
        self.sql_context = sql_context
        # Optional attributes (class-level defaults)
        self.mb_ty = mb_ty or list(self.DEFAULT_MB_TY)
        self.is_dynamic = bool(is_dynamic)
        self.mode = mode or "overwrite"
        # Create the parquets paths dictionary
        self.hdfs = {
            "mobs": os.path.join(self.path_mobs_fe, "calculate_mobs"), # path_mobs
            "pathwrite": os.path.join(self.write_mobs, "imputate_mobs"), #pathwrite
            "features": self.features_path, # features_path
            "raw_feature": os.path.join(self.features_path, "raw_features") # features_path
        }
        # Other attributes
        self._columns_not_to_be_renamed = [self.id_column, self.vintage_column, self.auxiliar_id]
        self._lst_months = self._get_months(self.periodo, self.n_mis)
    #
    @cached_property
    def calculated_mobs(self) -> DataFrame:
        """
        Lee y devuelve un DataFrame con los mobs calculados.

        Esta función lee un archivo Parquet desde una ruta especificada en HDFS,
        selecciona las columnas relevantes (ID de la columna, columna vintage y mb_ty),
        y devuelve el DataFrame resultante.

        Returns:
            DataFrame: Un DataFrame que contiene el ID de la columna, la columna vintage y mb_ty.
        """
        result = self.sql_context.read.parquet(self.hdfs["mobs"])
        result = result.select([self.id_column, self.vintage_column] + self.mb_ty)
        return result
    #
    @cached_property
    def raw_feature(self) -> DataFrame:
        """
        Lee y devuelve un DataFrame con el id y el auxiliar_id, y el vintage

        Returns:
            DataFrame: Un DataFrame que contiene el ID auxiliar, el ID de la columna y la columna vintage.
        """
        result = self.sql_context.read.parquet(self.hdfs["raw_feature"])
        result = result.select(self.auxiliar_id, self.id_column, self.vintage_column)
        return result
    #
    @cached_property
    def calculated_mobs_with_auxilar_id(self) -> DataFrame:
        """
        Calcula y devuelve un DataFrame con los mobs calculados y el ID auxiliar.

        Esta propiedad lee el dataframe raw features y el dataframe de mobs calculados, y realiza un join.

        Returns:
            DataFrame: Un DataFrame que contiene los mobs calculados junto con el ID auxiliar.
        """
        result = self.raw_feature.join(self.calculated_mobs, [self.id_column, self.vintage_column], "left")
        result = result.drop(self.id_column).drop(self.vintage_column)
        return result
    #
    def imputate_and_save_a_feature(self, feature:str) -> None:
        """
        Realiza la imputación y guarda una característica específica.

        Esta función lee un DataFrame de características, realiza la imputación de ingresos de nómina
        utilizando mobs, y guarda el DataFrame resultante en una ruta especificada.

        Args:
            feature (str): El nombre de la característica a imputar y guardar.

        Returns:
            None

        Raises:
            Exception: Si ocurre una excepción durante la lectura, imputación o guardado del DataFrame,
                se captura y se imprime.
        """
        output_path = os.path.join(self.hdfs["pathwrite"], feature)
        # reload intermedio: si la feature ya está imputada (directorio
        # con _SUCCESS) se salta — equivale al decorador `dynamic`
        if self.is_dynamic and is_successful(output_path):
            logging.info(f"Feature {feature} reloaded from {output_path}")
            return
        logging.info(f"Starting imputation for {feature}")
        # Read one feature datframe
        feature_df = self.sql_context.read.parquet(os.path.join(self.hdfs["features"], feature))
        # Get variables
        columns_without_id = [c for c in feature_df.columns if c != self.auxiliar_id]
        variables = list(set(self.remove_suffix(columns_without_id)))
        #
        # join with mobs
        feature_with_mobs = feature_df.join(self.calculated_mobs_with_auxilar_id, on=self.auxiliar_id, how="left")
        # do de imputation
        feature_imputated = self.payroll_mob_imputation(
            df=feature_with_mobs,
            features=variables,
            lista_meses=self._lst_months
        )
        # save file
        self._save_parquet(feature_imputated, output_path)
        # unpersist dataframes
        feature_imputated.unpersist()
        feature_with_mobs.unpersist()
        feature_df.unpersist()
        logging.info(f"Ending imputation for {feature}")
    #
    def save_raw_feature_to_output_path(self) -> None:
        """Copia la carpeta raw features al path de salida
        """
        feature = Path(self.hdfs["raw_feature"]).name
        output_path = os.path.join(self.hdfs["pathwrite"], feature)
        if self.is_dynamic and is_successful(output_path):
            logging.info(f"Feature {feature} reloaded from {output_path}")
            return
        logging.info(f"Starting imputation for {feature}")
        self._save_parquet(self.raw_feature, output_path)
        logging.info(f"Ending imputation for {feature}")
    #
    def multi_thread_imputate_features(self) -> None:
        """
        Ejecuta el método imputate_and_save_a_feature en paralelo utilizando múltiples hilos.

        Esta función utiliza un grupo de hilos para ejecutar el método imputate_and_save_a_feature
        concurrentemente para cada característica en la primera lista de feat_list. El número de hilos
        utilizados se determina por el atributo n_threads de la clase.

        Args:
            None

        Returns:
            None

        Raises:
            Exception: Si ocurre una excepción durante la ejecución de imputate_and_save_a_feature,
                se captura y se imprime.
        """
        thread_pool = ThreadPool(self.n_threads)
        #
        def worker(feature:str) -> None:
            try:
                self.imputate_and_save_a_feature(feature)
            except Exception as exc:
                print(f'Generated an exception: {exc}')
        #
        thread_pool.map(worker, self.feat_list[0])
        self.save_raw_feature_to_output_path()
    #
    @staticmethod
    def _get_months(periodo: Union[str, int], n_mis: Union[str, int]) -> List[int]:
        """
        Description:
        Method to create the months period ([3,6,9,12],[2,4,6,...])
        (delega en :func:`get_months`).

        Parameters
        ---------
        periodo: int
            number of feature's period (3,4,5,..,etc)

        Returns
        -------
        list of integers
            List with the months [3,6,9,12]
        """
        return get_months(periodo, n_mis)
    #
    @staticmethod
    def _mobs_key(feature: str) -> str:
        """Clave de mobs de una feature: ``'tefs'``, ``'tef_1'`` o ``'tef_2'``
        (``'tefs'`` por defecto)."""
        for key in ("tefs", "tef_1", "tef_2"):
            if key in feature:
                return key
        return "tefs"
    #
    @staticmethod
    def _sorted_meses(lista_meses: List[int], reverse: bool) -> List[int]:
        """Meses ordenados: de mayor a menor salvo ``reverse=True``."""
        return sorted(lista_meses, reverse=not reverse)
    #
    @staticmethod
    def payroll_mob_imputation(df: DataFrame, features: List[str], lista_meses:List[int], reverse:Optional[bool]=False) -> DataFrame:#
        """
        Imputa valores de mobs en el DataFrame dado para las características especificadas y los meses proporcionados.

        Args:
            df (DataFrame): DataFrame de entrada que contiene los datos a imputar.
            features (List[str]): Lista de nombres de columnas en el DataFrame que representan las características a imputar.
            lista_meses (List[int]): Lista de enteros que representan los meses para los cuales se realizará la imputación.
            reverse (Optional[bool], optional): Si es True, la imputación se realizará en orden inverso. Por defecto es False.

        Returns:
            DataFrame: DataFrame con los valores de mobs imputados para las características especificadas y los meses proporcionados.

        La función realiza los siguientes pasos:
        1. Itera sobre cada característica en la lista `features`.
        2. Para cada característica, itera sobre cada mes en la lista `lista_meses`.
        3. Imputa los valores de mobs en el DataFrame para la característica y el mes actual.
        4. Si `reverse` es True, la imputación se realiza en orden inverso.
        5. Devuelve el DataFrame con los valores de mobs imputados.

        Nota: Esta función aún no está implementada y lanzará un NotImplementedError si se llama.
        """
        raise NotImplementedError
    #
    def remove_suffix(self, variables: List[str]) -> List[str]:
        """
        Elimina el sufijo '_Xm' de cada variable en la lista, donde 'X' es cualquier número.

        Args:
            variables (List[str]): Lista de nombres de variables.

        Returns:
            List[str]: Lista de nombres de variables sin el sufijo '_Xm'.
        """
        pattern = re.compile(self.month_suffix_pattern)
        return [pattern.sub("", var) for var in variables]
    #
    def _save_parquet(self, df_save:DataFrame, path:str) -> None:
        """
        Guarda los datos en formato parquet en la ubicación deseada.

        La corrección específica se realiza utilizando la función 'write.mode().parquet',
        que guarda los datos en formato parquet en la ruta especificada.

        Args:
            df_save (DataFrame): Datos a ser guardados.
            path (str): Ruta donde se almacenarán los datos.

        Returns:
            None
        """
        save_parquet(df_save, path, self.mode)


class PreviousFixerPayrollIncome(PreviousFixerPayroll):
    """Imputador de MOBs para ``payroll_income``.

    Hereda toda la orquestación de :class:`PreviousFixerPayroll` y solo
    redefine las reglas de imputación: ``mobs_* <= 2`` -> ``-99999`` y,
    si el valor queda en 0, se toma el del mes anterior.
    """

    @staticmethod
    def payroll_mob_imputation(df: DataFrame, features: List[str], lista_meses:List[int], reverse:Optional[bool]=False) -> DataFrame:#
        """
        Cambia las variables de los features de acuerdo a las reglas:

        1. Cuando el valor de las columnas tef_1, tef_2 o tefs para ese cliente es menor o igual a 2,
        se reemplaza por -99999.
        2. Si el valor de la columna es 0, se reemplaza por el valor de la columna anterior.

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
            mobs_col = f"mobs_{PreviousFixerPayroll._mobs_key(feature)}"
            for mes in lista_meses:
                column_name = f"{feature}_{mes}m"
                df = df.withColumn(column_name, when(col(mobs_col) <= 2, -99999).otherwise(col(column_name)))
            for i in range(1, len(lista_meses)):
                current_col = f"{feature}_{lista_meses[i]}m"
                previous_col = f"{feature}_{lista_meses[i-1]}m"
                df = df.withColumn(current_col, when(col(current_col) == 0, col(previous_col)).otherwise(col(current_col)))
            #
        df = df.drop("mobs_tef_1", "mobs_tef_2", "mobs_tefs")
        return df
