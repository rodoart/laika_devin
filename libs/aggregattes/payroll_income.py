"""
Project: Payroll Income Aggregation
Developer: @rg49392
Update Date: 2025-02-19
Version: 0.1

Class and Functions required to aggregate (payroll income)
"""

# GROUPBY DEVELOPING START

# Libraries
from datetime import date
from dateutil.relativedelta import relativedelta
from functools import wraps
from pathlib import PosixPath
from typing import Callable, Dict, List, Optional, Union, Any
import re
import logging

from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, greatest, least,  when, lit
from pyspark.sql.types import DoubleType
from .base import (columns_with_prefix, columns_with_prefixes,
                   parse_config_bool, sort_df_columns)
from ..steps import cached_property, path_delete

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

# FUNCTIONS


def dynamic(path_key: str) -> Callable:
    """
    Decorador que gestiona la carga y almacenamiento de datos en formato Parquet de manera dinámica.

    Args:
        path_key (str): Clave para obtener la ruta del HDFS desde el diccionario `hdfs`.

    Returns:
        Callable: Función decoradora.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self) -> DataFrame:
            """
            Envuelve la función original para gestionar la carga y almacenamiento de datos.

            Args:
                self: Instancia de la clase que contiene el método decorado.

            Returns:
                DataFrame: DataFrame leído desde el archivo Parquet.
            """
            hdfs = self.hdfs
            is_dynamic = self.is_dynamic
            path = hdfs[path_key]
            try:
                if not is_dynamic:
                    raise Exception
                result = self.sqlContext.read.parquet(path)
                logging.info(f"INFO: {path_key.capitalize()} data reloaded from {path}")
            except:
                self._save_parquet(func(self), path)
                result = self.sqlContext.read.parquet(path)
                logging.info(f"INFO: {path_key.capitalize()} data saved on {path}")
            return result
        return wrapper
    return decorator


def tfrom_to_month(tfrom:int, vintage_date:date) -> int:
    """
    Calcula el mes correspondiente a una fecha restando un número de meses a una fecha dada.

    Args:
        tfrom (int): Número de meses a restar de la fecha dada.
        vintage_date (date): Fecha de referencia desde la cual se restarán los meses.

    Returns:
        int: El mes (como un número entero) de la fecha resultante después de restar los meses.
    """
    tfrom_date = vintage_date - relativedelta(months=tfrom)
    return tfrom_date.month


def calculate_average_excluding_max(df: DataFrame, columns: List[str], final_output_column:Optional[str]=None) -> DataFrame:
    """
    Calcula el promedio de las filas de cada columna excluyendo el valor más grande.

    Args:
        df (DataFrame): DataFrame de PySpark.
        columns (list): Lista de nombres de columnas.

    Returns:
        DataFrame: DataFrame con el promedio de cada fila excluyendo el valor más grande.
    """
    #
    if final_output_column is None:
        final_output_column = "avg_excluding_max"
    if len(columns) == 0:
        raise ValueError("La lista de columnas no puede estar vacía.")
    if len(columns) == 1:
        return df.withColumn(final_output_column, col(columns[0]))
    if len(columns) == 2:
        return df.withColumn(final_output_column, least(col(columns[0]), col(columns[1])))
    # if len(columns) > 2:
    #
    # Crear una columna con el valor máximo de cada fila
    max_col = greatest(*[col(c) for c in columns]).alias('max_value')
    # Crear una columna con la suma de los valores de cada fila
    sum_col = sum([col(c) for c in columns]).alias('sum_value')
    # Calcular el número de columnas
    num_columns = len(columns)
    # Crear una nueva columna con el promedio excluyendo el valor máximo
    avg_excluding_max_col = (col('sum_value') - col('max_value')) / (num_columns - 1)
    # Seleccionar las columnas originales y la nueva columna de promedio
    result_df = df.withColumn('max_value', max_col) \
        .withColumn('sum_value', sum_col) \
        .withColumn(final_output_column, avg_excluding_max_col) \
        .drop('max_value', 'sum_value')
    return result_df


def fix_2_imputate(df: DataFrame, columns: List[str], final_output_column:Optional[str]=None) -> DataFrame:
    """
    Imputa valores en un DataFrame basado en reglas específicas para columnas dadas.

    Args:
        df (DataFrame): DataFrame de entrada.
        columns (List[str]): Lista de nombres de columnas a utilizar para la imputación.
        final_output_column (Optional[str]): Nombre de la columna de salida. Si no se proporciona, se usa 'fix_2_imputate'.

    Returns:
        DataFrame: DataFrame con la columna imputada.

    Reglas de imputación:
        - Si el valor del mes actual es cero, entonces se queda en cero.
        - Si el valor del mes anterior es cero y el mes anteanterior es cero, entonces se queda con el valor del mes actual.
        - Si el valor del mes anterior es cero, entonces se queda con el menor de los valores del mes actual y el mes anteanterior.
        - Si el valor del mes anteanterior es cero, entonces se queda con el menor de los valores del mes actual y el mes anterior.
        - En otro caso, hace la suma de los tres valores y le resta el mayor de los tres y lo divide entre dos.
    """
    # Establecer el nombre de la columna de salida si no se proporciona
    if final_output_column is None:
        final_output_column = "fix_2_imputate"

    # Asegurarse de que el número de columnas esté entre 1 y 3
    assert (len(columns) > 0) & (len(columns) < 4), "The number of columns should be between 1 and 3."
    #
    col_0 = columns[0]
    #
    # Rellenar hasta 3 columnas con fakes en 0 si faltan
    col_1, col_2 = list(columns[1:3]) + ["fake_col_1", "fake_col_2"][len(columns)-1:]
    fake_cols = [c for c in (col_1, col_2) if c.startswith("fake_col")]
    for fake in fake_cols:
        df = df.withColumn(fake, lit(0.0))
    #
    # Crear la columna de salida basada en las reglas de imputación
    df = df.withColumn(final_output_column,
        # Si el valor del mes actual es cero, entonces se queda en cero
        when(col(col_0)==0, lit(0.0))
        # Si el valor del mes anterior es cero y el mes anteanterior es cero, entonces se queda con el valor del mes actual
        .otherwise(when((col(col_1)==0) & (col(col_2)==0), col(col_0))
        # Si el valor del mes anterior es cero, entonces se queda con el menor de los valores del mes actual y el mes anteanterior
        .otherwise(when(col(col_1)==0, least(col(col_0), col(col_2)))
        # Si el valor del mes anteanterior es cero, entonces se queda con el menor de los valores del mes actual y el mes anterior
        .otherwise(when(col(col_2)==0, least(col(col_0), col(col_1)))
        # En otro caso, hace la suma de los tres valores y le resta el mayor de los tres y lo divide entre dos
        .otherwise((col(col_0) + col(col_1) + col(col_2) - greatest(col(col_0), col(col_1), col(col_2))) / 2.0)
        ))))
    #
    # Eliminar columnas falsas si fueron creadas
    if fake_cols:
        df = df.drop(*fake_cols)
    #
    return df


def cap_and_floor_by_column_exact(
    df: DataFrame,
    column_name:str,
    output_column:str,
    q_floor:Optional[float]=0.0,
    q_cap:Optional[float]=0.995
) -> DataFrame:
    """
    Aplica límites superior e inferior a una columna de un DataFrame basado en percentiles exactos.

    Args:
        df (DataFrame): DataFrame de entrada.
        column_name (str): Nombre de la columna a la que se aplicarán los límites.
        output_column (str): Nombre de la columna de salida con los valores limitados.
        q_floor (Optional[float]): Percentil inferior para el límite (por defecto 0.0).
        q_cap (Optional[float]): Percentil superior para el límite (por defecto 0.995).

    Returns:
        DataFrame: DataFrame con la columna limitada.
    """
    # Calcular los percentiles especificados para la columna
    quantiles = df.selectExpr(f'percentile({column_name}, array({q_floor}, {q_cap})) as quantiles').collect()[0].quantiles
    floor,cap = quantiles
    #
    # Crear una nueva columna con los valores limitados.
    df_capped = df.withColumn(output_column,
        when(col(column_name) > cap, cap)
        .otherwise(when(col(column_name) < floor, floor)
        .otherwise(col(column_name))
    ))
    #
    return df_capped


def normalize_multiple_columns(
    df: DataFrame,
    columns_to_scale:List[str],
    out_prefix:Optional[str]='scaled'
) -> DataFrame:
    """
    Normaliza multiples columnas de un DataFrame utilizando MinMaxScaler.

    Args:
        df (DataFrame): DataFrame de entrada.
        columns_to_scale (List[str]): Lista de nombres de columnas a escalar.
        out_prefix (Optional[str]): Prefijo para las columnas de salida escaladas (por defecto 'scaled').

    Returns:
        Dataframe: DataFrame con las columnas escaladas.
    """
    intermediate_prefix = "vec"
    vector_columns = [f'{intermediate_prefix}_{col_name}' for col_name in columns_to_scale]
    output_columns = [f'{out_prefix}_{col_name}' for col_name in columns_to_scale]
    #
    vectorizers = [VectorAssembler(inputCols=[col_name], outputCol=col_vector) for col_name, col_vector in zip(columns_to_scale,
    vector_columns)]
    scalers = [MinMaxScaler(inputCol=col_vector, outputCol=col_output) for col_vector, col_output in zip(vector_columns,
    output_columns)]
    #
    pipeline = Pipeline(stages=vectorizers + scalers)
    pipeline_fitted = pipeline.fit(df)
    scaled_df = pipeline_fitted.transform(df)
    scaled_df = scaled_df.drop(*vector_columns)
    #
    return scaled_df

# PRINCIPAL CLASS
class PayrollIncomeAggregate:
    id_column = "numcliente"
    vintage_column = "vintage_date"
    tfrom_suffix_pattern = r'_\d+$'
    extra_months = 2
    def __init__(self,
        previous_path:str,
        agg_dict: Dict[str, Union[str, int, dict]],
        n_mis: Union[int, str],
        period: Union[int, str],
        sqlContext: SparkSession,
        mode:Optional[str]="overwrite",
        is_dynamic:Optional[bool] = True
    ) -> None:
        """
        Inicializa una instancia de la clase PayrollIncomeAggregate.
        Aplica los mentodos
        de los métodos de pre-procesamiento

        Args:
            previous_path (str): Ruta al archivo anterior.
            agg_dict (Dict[str, Union[str, int, dict]]): Diccionario de agregación con configuraciones.
            n_mis (Union[int, str]): Número de meses.
            period (Union[int, str]): Periodo de tiempo.
            sqlContext (SparkSession): Contexto de Spark.
            mode (Optional[str], optional): Modo de operación. Por defecto es "overwrite".
            is_dynamic (Optional[bool], optional): Indicador de si es dinámico. Por defecto es False.

        Ejemplo del diccionario de configuracion:
        ```python
        agg_dict =  {
            "rename":{
                "enable": True,
                "columns_to_rename": {
                    "sum_importe": "income_tefs",
                    "sum_importe_1": "income_tef_1",
                    "sum_importe_2": "income_tef_2"
                }
            },
            "fix_1":{
                "enable":True,
                "months_to_fix_1": [5,11,12],
                "prefix": "fix_1",
                "prefixes_to_apply": [
                    "None"
                ],
                "prefixes_to_drop": [
                ]
            },
            "fix_2":{
                "enable":True,
                "prefix": "fix_2",
                "prefixes_to_apply": [
                    "fix_1"
                ],
                "prefixes_to_drop": [
                    "fix_1"
                ]
            },
            "cap_and_floor": {
                "enable":True,
                "q_floor": 0,
                "q_cap": 0.995,
                "error": 0.00001,
                "prefix": "capped",
                "prefixes_to_apply": [
                    "fix_2"
                ],
                "prefixes_to_drop": [
                ]
            },
            "normalize": {
                "enable":True,
                "prefix": "norm",
                "prefixes_to_apply": [
                    "None",
                    "capped"
                ],
                "prefixes_to_drop": [
                    "capped"
                ]
            },
        }
        """
        # attributes
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.n_mis = int(n_mis)
        self.period = int(period)
        self.sqlContext = sqlContext
        self._columns_not_to_be_renamed = [self.id_column, self.vintage_column]
        #
        # Mode of operation
        if mode is None:
            self.mode = "overwrite"
        self.mode = mode
        self.is_dynamic = is_dynamic
        #
        # paths
        workspace = PosixPath(self.previous_path).parent
        self.hdfs = {
            "pivoted": self.previous_path,
            "aggregate": str(workspace.joinpath("groupby")),
            "fixed": str(workspace.joinpath("fixed")),
            "capped": str(workspace.joinpath("capped")),
            # "normalized": str(workspace.joinpath("normalized"
        }
        # calculated attributes
        self.tfroms =  [tfrom for tfrom in range(0, self.n_mis)]
        #
        # read subdictionaries
        self._rename_config = self.agg_dict["rename"]
        self._fix_1_config = self.agg_dict["fix_1"]
        self._fix_2_config = self.agg_dict["fix_2"]
        self._cap_and_floor_config = self.agg_dict["cap_and_floor"]
        self._normalize_config = self.agg_dict["normalize"]
        self.prefixes = [
            self._fix_1_config['prefix'],
            self._fix_2_config['prefix'],
            self._cap_and_floor_config['prefix'],
            self._normalize_config['prefix']
        ]
        #
        # check if all prefixes are defined and not "vec" or "none"
        assert all([prefix is not None for prefix in self.prefixes]), "All prefixes should be defined."
        assert all([prefix.lower() not in ["vec", "none"] for prefix in self.prefixes]), "All prefixes should be different from 'vec' and 'none'."
        #
        # check if all predixes are different
        assert len(self.prefixes) == len(set(self.prefixes)), "All prefixes should be different."
    ################################################################################
    # STEPS

    @cached_property
    def pivoted(self) -> DataFrame:
        """Reads the pivoted step from the previous path

        Returns:
            DataFrame: pivoted DataFrame
        """
        return self.sqlContext.read.parquet(self.hdfs["pivoted"])

    @cached_property
    def vintage_dates(self) -> List[date]:
        """
        Obtiene una lista de fechas únicas de la columna self.vintage_column del DataFrame 'pivoted'.

        Returns:
            List[date]: Una lista de fechas únicas presentes en la columna self.vintage_column.
        """
        return [row[self.vintage_column] for row in self.pivoted.select(self.vintage_column).distinct().collect()]

    @cached_property
    def renamed(self) -> DataFrame:
        """
        Renombra las columnas del DataFrame 'pivoted' según la configuración especificada en '_rename_config'.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas renombradas.
        """
        if not self._rename_config["enable"]:
            return self.pivoted
        #
        column_rename_relation = self._rename_config["columns_to_rename"]
        #
        expanded_column_rename_relation = {f"{column}_{tfrom}": f"{column_rename_relation[column]}_{tfrom}" for column in list
        (column_rename_relation.keys()) for tfrom in self.tfroms}
        #
        column_renames = [col(column).alias(expanded_column_rename_relation[column]) if column in list
        (expanded_column_rename_relation.keys()) else column for column in self.pivoted.columns]
        renamed = self.pivoted.select(*column_renames)
        return renamed

    @cached_property
    def fix_1(self) -> DataFrame:
        """
        Aplica una corrección especifica a las columnas del DataFrame según la configuración.

        La corrección especifica se realiza utilizando la función `calculate_average_excluding_max`,
        que calcula el promedio de las filas de cada columna excluyendo el valor más grande.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        step_object = self._FixStep(self, self.renamed, self._fix_1_config, self._apply_fix_1)
        return step_object.apply()

    @cached_property
    @dynamic(path_key="fixed")
    def fix_2(self) -> DataFrame:
        """
        Aplica una corrección especifica a las columnas del DataFrame según la configuración.

        La corrección especifica se realiza utilizando la función `fix_2_imputate`,
        que imputa valores en un DataFrame basado en reglas específicas para columnas dadas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        step_object = self._FixStep(self, self.fix_1, self._fix_2_config, self._apply_fix_2)
        return step_object.apply()

    @cached_property
    def remove_last_months(self) -> DataFrame:
        """
        Elimina los últimos meses de las columnas del DataFrame según la configuración.

        Returns:
            DataFrame: Un nuevo DataFrame sin las columnas de los últimos meses.
        """
        result = self.fix_2
        # Get columns without suffixes
        columns_to_apply = [column for column in result.columns if column not in self._columns_not_to_be_renamed]
        columns_to_apply = self.remove_tfrom_suffix(columns_to_apply)
        columns_to_apply = list(set(columns_to_apply))
        #
        result = self._apply_remove_last_months(result, columns_to_apply)
        return result

    @cached_property
    @dynamic(path_key="capped")
    def capped(self) -> DataFrame:
        """
        Aplica limites superior e inferior a las columnas del DataFrame según la configuración.

        La corrección especifica se realiza utilizando la función `cap_and_floor_by_column_exact`,
        que aplica un límite inferior y superior a los valores de las columnas especificadas,
        basando los límites en percentiles exactos.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        step_object = self._Step(self, self.remove_last_months, self._cap_and_floor_config, self._apply_cap_and_floor)
        return step_object.apply()

    @cached_property
    def normalized(self) -> DataFrame:
        """
        Aplica la normalización a las columnas del DataFrame según la configuración.

        La corrección especifica se realiza utilizando la función `normalize_multiple_columns`,
        que normaliza los valores de las columnas especificadas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas normalizadas.
        """
        step_object = self._Step(self, self.capped, self._normalize_config, self._apply_normalization)
        return step_object.apply()

    @cached_property
    def vectors_removed(self) -> DataFrame:
        normalize_enable = parse_config_bool(
            self._normalize_config["enable"], "normalize enable")
        #
        if not normalize_enable:
            return self.capped
        #
        prefix = str(self._normalize_config["prefix"])
        df = self.normalized
        columns_to_apply = self.get_columns_with_prefixes(df, [prefix])
        #
        result = self._apply_vector_removal(df, columns_to_apply)
        return result

    def aggregate(self) -> None:
        """
        Agrega los datos normalizados y los guarda en formato parquet.

        La corrección especifica se realiza utilizando la función `_save_parquet`,
        que guarda los datos normalizados en la ruta especificada en formato parquet.

        Returns:
            None
        """
        result = self.vectors_removed
        self._save_parquet(result, self.hdfs["aggregate"])
        self._clean()

    # FUNCTIONS

    def remove_tfrom_suffix(self, columns: List[str]) -> List[str]:
        """
        Elimina el sufijo '_<número>' de los nombres de las columnas.

        La corrección específica se realiza utilizando una expresión regular que elimina
        el sufijo numérico al final de cada nombre de columna.

        Args:
            columns (List[str]): Lista de nombres de columnas.

        Returns:
            List[str]: Lista de nombres de columnas sin el sufijo numérico.
        """
        return [re.sub(self.tfrom_suffix_pattern, '', column) for column in columns]

    def sort_columns(self, df: DataFrame) -> DataFrame:
        """``protected`` primero, resto ordenado alfabéticamente."""
        return sort_df_columns(df, self._columns_not_to_be_renamed)

    def _get_columns_with_prefix(self, df: DataFrame, prefix:Optional[str]="None") -> List[str]:
        """Columnas de ``df`` con el prefijo dado (ver
        :func:`libs.aggregattes.base.columns_with_prefix`)."""
        return columns_with_prefix(
            df.columns, self.prefixes, self._columns_not_to_be_renamed,
            prefix)

    def get_columns_with_prefixes(self, df: DataFrame, prefixes:List[str]) -> List[str]:
        """Unión sin duplicados de las columnas de cada prefijo."""
        return columns_with_prefixes(
            df, self.prefixes, self._columns_not_to_be_renamed, prefixes)

    #
    class _Step:
        """
        Representa un paso en el proceso de transformación de datos.

        La corrección específica se realiza aplicando un método de transformación a un DataFrame,
        basado en la configuración del paso.

        Args:
            parent: El objeto padre que contiene métodos auxiliares.
            df (DataFrame): El DataFrame de entrada.
            step_config (Dict[str, Union[str, int, dict, list]]): Configuración del paso, que incluye
                si el paso está habilitado, el prefijo para las columnas de salida, los prefijos de
                las columnas a aplicar y los prefijos de las columnas a eliminar.
            step_method (Callable[[DataFrame, List[str], str], DataFrame]): Método de transformación
                que se aplicará al DataFrame.

        Attributes:
            parent: El objeto padre que contiene métodos auxiliares.
            df (DataFrame): El DataFrame de entrada.
            step_config (Dict[str, Union[str, int, dict, list]]): Configuración del paso.
            enabled (bool): Indica si el paso está habilitado.
            prefix (str): Prefijo para las columnas de salida.
            prefixes_to_apply (List[str]): Prefijos de las columnas a aplicar.
            step_method (Callable[[DataFrame, List[str], str], DataFrame]): Método de transformación.
            prefixes_to_drop (List[str]): Prefijos de las columnas a eliminar.
            columns_to_apply (List[str]): Columnas a las que se aplicará la transformación.

        Methods:
            _get_columns_to_apply() -> List[str]: Obtiene las columnas del DataFrame que tienen los
                prefijos especificados en la configuración del paso.
            apply() -> DataFrame: Aplica el método de transformación al DataFrame y realiza las
                operaciones adicionales según la configuración del paso.
        """
        def __init__(
            self,
            parent,
            df: DataFrame,
            step_config: Dict[str, Union[str, int, dict, list]],
            step_method: Callable[[DataFrame, List[str], str], DataFrame]
        ) -> None:
            self.parent = parent
            self.df = df
            self.step_config = step_config
            self.prefix = step_config["prefix"]
            self.prefixes_to_apply = step_config["prefixes_to_apply"]
            self.step_method = step_method
            #
            self.enabled = parse_config_bool(step_config["enable"], "enable")
            self.prefixes_to_drop = step_config.get("prefixes_to_drop", [])
            self.columns_to_apply = self._get_columns_to_apply()
            #
        def _get_columns_to_apply(self) -> List[str]:
            return self.parent.get_columns_with_prefixes(self.df, self.prefixes_to_apply)
            #
        def apply(self) -> DataFrame:
            if not self.enabled:
                return self.df
            result = self.step_method(df=self.df, columns_to_apply=self.columns_to_apply, prefix=self.prefix)
            if len(self.prefixes_to_drop) > 0:
                columns_to_drop = self.parent.get_columns_with_prefixes(result, self.prefixes_to_drop)
                result = result.drop(*columns_to_drop)
            #
            result = self.parent.sort_columns(result)
            return result

    class _FixStep(_Step):
        def __init__(
            self,
            parent,
            df: DataFrame,
            step_config: Dict[str, Union[str, int, dict]],
            step_method: Callable[[DataFrame, List[str], str], DataFrame]
        ) -> None:
            """
            Como la otra clase pero con la particularidad de que se eliminan los sufijos _tfrom de las columnas a aplicar.
            """
            #
            super().__init__(parent, df, step_config, step_method)
            self.columns_to_apply = self._get_columns_to_apply()
            #
        def _get_columns_to_apply(self) -> List[str]:
            result = super()._get_columns_to_apply()
            result = self.parent.remove_tfrom_suffix(result)
            result = list(set(result))
            return result

    def _apply_fix_1(self, df: DataFrame, columns_to_apply:List[str], prefix:str) -> DataFrame:
        """
        Aplica una corrección específica a las columnas del DataFrame según la configuración.

        La corrección específica se realiza utilizando la función `calculate_average_excluding_max`,
        que calcula el promedio de las filas de cada columna excluyendo el valor más grande.

        Args:
            df (DataFrame): El DataFrame de entrada.
            columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la corrección.
            prefix (str): Prefijo para las columnas de salida corregidas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        logging.info("Performing fix 1")
        try:
            months_to_apply_fix_1 = self._fix_1_config["months_to_fix_1"]
        except:
            months_to_apply_fix_1 = [i for i in range(1,13)]
        #
        # Registrar el DataFrame como una tabla temporal
        df.createOrReplaceTempView("temp_table")
        #
        for k, vintage_date in enumerate(self.vintage_dates):
            t_froms_with_fix = [tfrom for tfrom in self.tfroms if tfrom_to_month(tfrom, vintage_date) in months_to_apply_fix_1]
            # Usar un query SQL para filtrar los datos
            query = f"""
                SELECT *
                FROM temp_table
                WHERE CAST({self.vintage_column} AS STRING) = '{str(vintage_date)}'
                """
            filtered = self.sqlContext.sql(query)
            for input_col_name in columns_to_apply:
                # column_names
                step_input_column = input_col_name
                step_output_column = f"{prefix}_{input_col_name}"
                for tfrom in self.tfroms:
                    if tfrom not in t_froms_with_fix:
                        filtered = filtered.withColumn(f'{step_output_column}_{tfrom}', col(f'{step_input_column}_{tfrom}'))
                    else:
                        # next tfroms
                        tfrom_1 = tfrom + 1
                        tfrom_2 = tfrom + 2
                        # columns to apply fix
                        tfrom_columns_to_apply_fix = [f'{step_input_column}_{tfrom}']
                        #
                        if tfrom_1 < self.n_mis:
                            tfrom_columns_to_apply_fix += [f'{step_input_column}_{tfrom_1}']
                        if tfrom_2 < self.n_mis:
                            tfrom_columns_to_apply_fix += [f'{step_input_column}_{tfrom_2}']
                        #
                        filtered = filtered.withColumn(f"is_{step_input_column}_{tfrom}_zero", col(f'{step_input_column}_{tfrom}') == 0)
                        filtered = calculate_average_excluding_max(
                            df=filtered,
                            columns=tfrom_columns_to_apply_fix,
                            final_output_column=f'{step_output_column}_{tfrom}'
                        )
                        # replace with 0 if the original value was 0
                        filtered = filtered.withColumn(f'{step_output_column}_{tfrom}',
                            when(col(f"is_{step_input_column}_{tfrom}_zero"), lit(0.0))
                            .otherwise(col(f'{step_output_column}_{tfrom}'))
                        )
                        filtered = filtered.drop(f"is_{step_input_column}_{tfrom}_zero")
            if k == 0:
                result = filtered
            else:
                result = result.union(filtered)
        #
        return result

    def _apply_fix_2(self, df: DataFrame, columns_to_apply:List[str], prefix:str) -> DataFrame:
        """
        Aplica una corrección específica a las columnas del DataFrame según la configuración.

        La corrección específica se realiza utilizando la función `fix_2_imputate`,
        que imputa valores en un DataFrame basado en reglas específicas para columnas dadas.

        Args:
            df (DataFrame): El DataFrame de entrada.
            columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la corrección.
            prefix (str): Prefijo para las columnas de salida corregidas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        logging.info("Performing fix 2")
        filtered = df
        for input_col_name in columns_to_apply:
            # column_names
            step_input_column = input_col_name
            step_output_column = f"{prefix}_{input_col_name}"
            for tfrom in self.tfroms:
                # next tfroms
                tfrom_1 = tfrom + 1
                tfrom_2 = tfrom + 2
                #
                # columns to apply fix
                tfrom_columns_to_apply_fix = [f'{step_input_column}_{tfrom}']
                #
                if tfrom_1 < self.n_mis:
                    tfrom_columns_to_apply_fix += [f'{step_input_column}_{tfrom_1}']
                if tfrom_2 < self.n_mis:
                    tfrom_columns_to_apply_fix += [f'{step_input_column}_{tfrom_2}']
                #
                filtered = fix_2_imputate(
                    df=filtered,
                    columns=tfrom_columns_to_apply_fix,
                    final_output_column=f'{step_output_column}_{tfrom}'
                )
        #
        return filtered

    def _apply_remove_last_months(self, df: DataFrame, columns_to_apply:List[str]) -> DataFrame:
        filtered = df
        for input_col_name in columns_to_apply:
            # column_names
            step_input_column = input_col_name
            for tfrom in self.tfroms[-self.extra_months:]:
                filtered = filtered.drop(f'{step_input_column}_{tfrom}')
        # change tfroms
        self.tfroms = self.tfroms[:-self.extra_months]
        self.n_mis = len(self.tfroms)
        return filtered

    def _apply_cap_and_floor(self, df: DataFrame, columns_to_apply:List[str], prefix:str) -> DataFrame:
        """
        Aplica una corrección específica a las columnas del DataFrame según la configuración.

        La corrección específica se realiza utilizando la función `cap_and_floor_by_column_exact`,
        que aplica un límite inferior y superior a los valores de las columnas especificadas,
        basando los límites en percentiles exactos.

        Args:
            df (DataFrame): El DataFrame de entrada.
            columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la corrección.
            prefix (str): Prefijo para las columnas de salida corregidas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas corregidas.
        """
        logging.info("Performing cap and floor")
        try:
            q_floor = float(self._cap_and_floor_config["q_floor"])
        except:
            q_floor = 0.0
        #
        try:
            q_cap = float(self._cap_and_floor_config["q_cap"])
        except:
            q_cap = 0.995
        #
        filtered = df
        for input_col_name in columns_to_apply:
            # column_names
            step_input_column = input_col_name
            step_output_column = f"{prefix}_{input_col_name}"
            filtered = cap_and_floor_by_column_exact(
                df=filtered,
                column_name=step_input_column,
                output_column=step_output_column,
                q_floor=q_floor,
                q_cap=q_cap
            )
        #
        return filtered

    def _apply_normalization(self, df: DataFrame, columns_to_apply:List[str], prefix:str) -> DataFrame:
        """
        Aplica la normalización a las columnas del DataFrame según la configuración.

        La corrección específica se realiza utilizando la función `normalize_multiple_columns`,
        que normaliza los valores de las columnas especificadas.

        Args:
            df (DataFrame): El DataFrame de entrada.
            columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la normalización.
            prefix (str): Prefijo para las columnas de salida normalizadas.

        Returns:
            DataFrame: Un nuevo DataFrame con las columnas normalizadas.
        """
        logging.info("Performing normalization")
        return normalize_multiple_columns(df=df, columns_to_scale=columns_to_apply, out_prefix=prefix)

    def _apply_vector_removal(self, df: DataFrame, columns_to_apply:List[str]) -> DataFrame:
        logging.info("Performing vector removal")
        # col names
        intermediate_prefix = "sstmpss"
        intermediate_columns = [f'{intermediate_prefix}_{col_name}' for col_name in columns_to_apply]
        final_col_renames = self._columns_not_to_be_renamed + [col(col_tmp).alias(col_name) for col_name, col_tmp in zip
        (columns_to_apply, intermediate_columns)]
        #
        # Registrar UDF y tabla
        #
        # Definir una función para extraer el primer elemento del vector
        def extract_first_element(vector: Any) -> Union[float, None]:
            return float(vector[0]) if vector else None
        #
        self.sqlContext.udf.register("extract_first_element", extract_first_element, DoubleType())
        df.createOrReplaceTempView("tmp_vector_removal")
        #
        # Construir query de sql
        select_expr = ", ".join([f"extract_first_element({col_name}) as {col_tmp}" for col_name, col_tmp in zip(columns_to_apply,
        intermediate_columns)])
        query = f"SELECT *, {select_expr} FROM tmp_vector_removal"
        #
        # Ejecutar query
        result = self.sqlContext.sql(query)
        result = result.drop(*columns_to_apply)
        result = result.select(*final_col_renames)
        #
        return result

    #
    def _save_parquet(self, df_save:DataFrame, path:str) -> None:
        """
        Guarda los datos en formato parquet en la ubicación deseada.

        La corrección específica se realiza utilizando la función `write.mode().parquet`,
        que guarda los datos en formato parquet en la ruta especificada.

        Args:
            df_save (DataFrame): Datos a ser guardados.
            path (str): Ruta donde se almacenarán los datos.

        Returns:
            None
        """
        df_save.write.mode(self.mode).parquet(path)
    #
    @staticmethod
    def _delete_intermediate(path:str) -> None:
        """Borra ``path`` si existe (ver :func:`libs.steps.path_delete`)."""
        path_delete(path)
    #
    def _clean(self) -> None:
        """
        Limpia los datos intermedios y libera recursos.

        La corrección específica se realiza eliminando las rutas intermedias en HDFS utilizando la función `_delete_intermediate`,
        y liberando los DataFrames en caché.

        Args:
            None

        Returns:
            None
        """
        if self.is_dynamic:
            return None
        #
        intermediate_hdfs = ["fixed", "capped"]
        # for path in intermediate_hdfs:
        #     try:
        #         self._delete_intermediate(self.hdfs[path])
        #     except:
        #         pass
        # #
        self.pivoted.unpersist()
        self.fix_1.unpersist()
        self.fix_2.unpersist()
        self.capped.unpersist()
        self.normalized.unpersist()
