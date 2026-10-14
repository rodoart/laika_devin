"""
Project: Payroll Income Aggregation
Developer: @rg49392
Update Date: 2025-02-19
Version: 0.1

Class and Functions required to aggregate (payroll streaks)
"""

# Libraries
from dateutil.relativedelta import relativedelta
from pathlib import PurePosixPath as PosixPath
from typing import Callable, Dict, List, Optional, Union

from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql.functions import (array, array_contains, coalesce, col,
collect_list, sort_array, first, greatest, last, lit, row_number,
min as spark_min, sum as spark_sum, trunc, when, max as spark_max,
struct, expr, lag)

from .payroll_income import cached_property
from ..weeks import WeekOfMonthPayroll
from .base import (columns_with_prefix, columns_with_prefixes,
                   parse_config_bool, sort_df_columns)
from ..steps import dynamic, path_delete


# PRINCIPAL CLASS
class PayrollStreaksAggregate:
    col_names = {
        "tef": "tef",
        "tef_1": "tef_1",
        "tef_2": "tef_2",
        "tefs": "tefs",
        "tfrom": "tfrom",
        "tfrom_date": "tfrom_date",
        "tfrom_month": "tfrom_month",
        "process_date": "process_date",
        "importe": "importe",
        "numcliente": "numcliente",
        "vintage_date": "vintage_date",
        "n_tef": "n_tef",
    }
    def __init__(self,
        previous_path:str,
        pivot_path:str,
        tables:List[str],
        agg_dict: Dict[str, Union[str, int, dict]],
        n_mis: Union[int, str],
        period: Union[int, str],
        sqlContext: SparkSession,
        mode:Optional[str]="overwrite",
        is_dynamic:Optional[bool] = False
    ) -> None:
        """
        Inicializa una instancia de la clase PayrollIncomeAggregate.
        Aplica los mentodos

        Args:
            previous_path (str): Ruta al archivo anterior.
            agg_dict (Dict[str, Union[str, int, dict]]): Diccionario de agregación con configuraciones.
            n_mis (Union[int, str]): Número de meses.
            period (Union[int, str]): Periodo de tiempo.
            sqlContext (SparkSession): Contexto de Spark.
            mode (Optional[str], optional): Modo de operación. Por defecto es "overwrite".
            is_dynamic (Optional[bool], optional): Indicador de si es dinámico. Por defecto es False.

        Ejemplo del dicionario de configuracion:
        """
        # attributes
        self.previous_path = previous_path
        self.tables = tables
        self.agg_dict = agg_dict
        self.n_mis = int(n_mis)
        self.period = int(period)
        self.sqlContext = sqlContext
        self._columns_not_to_be_renamed = [
            "numcliente", "vintage_date", "n_tef", "tfrom", "tfrom_date", "tfrom_month",
            "week_of_month", "count_paid_in_month", "weeks_of_month",
            "payment_type_per_month", "client_payment_type",
        ]
        #
        # Mode of operation
        if mode is None:
            self.mode = "overwrite"
        self.mode = mode
        # self.is_dynamic = is_dynamic
        # TODO: REMOVE THIS
        self.is_dynamic = True
        #
        # paths
        workspace = PosixPath(self.previous_path).parent
        self.hdfs = {
            "aggregate": str(workspace.joinpath("groupby")),
            "week_tef": str(workspace.joinpath("tmp/week_tef")),
            "filtered": str(workspace.joinpath("tmp/filtered")),
            "payment_frequencies": str(workspace.joinpath("tmp/payment_frequencies")),
            # "middle_patterned_check_point" : str(workspace.joinpath("tmp/middle_patterned_check_point")),
            "patterned": str(workspace.joinpath("tmp/patterned")),
            "pivot": pivot_path,
        }
        #
        self.hdfs = {**self.hdfs, **self._find_tables_and_make_paths()}
        # calculated attributes
        self.tfroms =  [tfrom for tfrom in range(0, self.n_mis*4)]
        self.tef_list =[self._c("tef_1"), self._c("tefs")]
        #
        # read subdictionaries
        self._pattern_keys = [
            "rename",
            "raw_pattern",
            # Common
            "sorted_shortened_pattern_forced_weekly",
            "shortened_pattern_forced_weekly",
            # Pesimist
            "sorted_shortened_pattern_forced_monthly",
            "sorted_shortened_pattern_forced_fortnightly",
            "sorted_shortened_pattern",
            "shortened_pattern_forced_monthly",
            "shortened_pattern_forced_fortnightly",
            "shortened_pattern",
            # Optimist
            "sorted_shortened_pattern_forced_monthly_optimist",
            "sorted_shortened_pattern_forced_fortnightly_optimist",
            "sorted_shortened_pattern_optimist",
            "shortened_pattern_forced_monthly_optimist",
            "shortened_pattern_forced_fortnightly_optimist",
            "shortened_pattern_optimist",
            # Special
            "payment_type_pattern",
            "unusual_pattern",
            "reverse_pattern",
            "inverse_pattern"
        ]
        for key in self._pattern_keys:
            setattr(self, f"_{key}_config", self.agg_dict[key])
        #
        self.prefixes = [getattr(self, f"_{key}_config")["prefix"] for key in self._pattern_keys if key != "rename"]
        #
        # Assertions
        assert all([prefix is not None for prefix in self.prefixes]), "All prefixes should be defined."
        assert all([prefix.lower() not in ["vec", "none"] for prefix in self.prefixes]), "All prefixes should be different from 'vec' and 'none'."
        # check if all prefixes are different
        assert len(self.prefixes) == len(set(self.prefixes)), "All prefixes should be different."
    ################################################################################
    # STEPS
    #

    @cached_property
    def filter_step(self) -> "_FilterStep":
        """
        Lee las tablas de entrada, Asigna Nomina y Nomina.

        Con Asigna Nomina se obtiene la información de los clientes y
        sus dos tefs principales, dando la prioridad al mes más reciente,
        pero si no hay información, se toma la de los meses anteriores,
        priorizando la información más reciente.

        Se usa Asigna Nomina para filtrar solo los tefs principales de Nomina.

        Se marca con tef_1 al tef principal y tef_2 al secundario.
        """
        return _FilterStep(self)

    #
    @cached_property
    def tagger_step(self) -> "_TaggerStep":
        """
        Etiqueta los pagos por semana, quincena y mes.

        Se etiqueta el pago con la semana del año, el dia de la semana,
        la quincena del mes y la semana del mes.

        Se cuenta el número de pagos en la semana, quincena y mes.
        """
        return _TaggerStep(self)

    #
    @cached_property
    def renamer_step(self) -> "_RenamerStep":
        """
        Renombra las columnas.

        Se renombran las columnas con los nombres especificados en el diccionario.
        """
        return _RenamerStep(parent=self, df=self.tagger_step.final, rename_dict=self._rename_config)

    #
    @cached_property
    def patterns_step(self) -> "_PatternStep":
        """
        Regresa el último patrón pero almacena todos los demás.

        Se aplican los patrones de pago.

        """
        previous_df = self.renamer_step.final
        previous_prefix = ""
        for pattern_name in self._pattern_keys:
            if pattern_name in ["rename"]:
                continue
            method = globals()[f"{pattern_name}_rule"]
            #
            if pattern_name == "reverse_pattern":
                method = (
                lambda df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix:
                reverse_pattern_rule(
                    df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix,
                    vintage_date_column=self._c("vintage_date"),
                    n_tef_column=self._c("n_tef"),
                    numcliente_column=self._c("numcliente")
                )
                )
            pattern_step_object = _PatternStep(
                parent=self,
                input_df=previous_df,
                step_config=getattr(self, f"_{pattern_name}_config"),
                step_method=method,
                previous_prefix=previous_prefix,
            )
            previous_df = pattern_step_object.final
            previous_prefix = pattern_step_object.prefix
            # if pattern_name == "payment_type_pattern":
            #     # CHECK POINT
            #     self._save_parquet(previous_df, self.hdfs["middle_patterned_check_point"])
            #     previous_df =  self.sqlContext.read.parquet(self.hdfs["middle_patterned_check_point"])
            setattr(self, f"{pattern_name}_step", pattern_step_object)
        # Only last pattern
        return getattr(self, f"{pattern_name}_step")

    #
    @cached_property
    def pivot_num_tef_step(self) -> "_PivotNumTefStep":
        """
        La columna n_tef se convierte pivotea y se distribuye la información
        a las columnas terminadas en tef_1, tef_2 y tefs.

        Se preparan los datos para el Feature Engineering

        """
        return _PivotNumTefStep(self)

    #
    def aggregate(self) -> DataFrame:
        result:DataFrame = self.pivot_num_tef_step.final
        self._clean()
        return result

    #
    def _c(self, col_name:str) -> str:
        """
        gets the col_name
        """
        return self.col_names[col_name]

    #
    def _find_tables_and_make_paths(self) -> Dict[str,str]:
        previous_hdfs = PosixPath(self.previous_path)
        result = {}
        assert len(self.tables) == 2, "There should only two tables in the list."
        # asigna nomina
        # Revisa cual de las tablas en la lista tiene la palabra asigna
        asigna_name = [table for table in self.tables if "asigna" in table]
        if len(asigna_name) == 0:
            raise ValueError("There should be a table with the word 'asigna'.")
        #
        asigna_path = str(previous_hdfs.joinpath(asigna_name[0]))
        result["asigna"] = asigna_path if asigna_path.endswith("_historic") else asigna_path + "_historic"
        # nomina
        # Devuelve la tabla que no tiene la palabra asigna
        nomina_name = [table for table in self.tables if "asigna" not in table]
        if len(nomina_name) == 0:
            raise ValueError("There should be a table without the word 'asigna'.")
        #
        nomina_path = str(previous_hdfs.joinpath(nomina_name[0]))
        result["nomina"] = nomina_path if nomina_path.endswith("_historic") else nomina_path + "_historic"
        #
        return result

    #
    def _save_parquet(self, df_save:DataFrame, path:str) -> None:
        """
        Guarda los datos en formato parquet en la ubicación deseada.
        """
        df_save.write.mode(self.mode).parquet(path)

    #
    def _delete_intermediate(self, path: str) -> None:
        """Borra ``path`` si existe (ver :func:`libs.steps.path_delete`)."""
        path_delete(path)

    #
    def _clean(self) -> None:
        """
        Limpia los datos intermedios y libera recursos.
        """
        intermediate_hdfs = ["week_tef", "filtered", "payment_frequencies"]
        for path in intermediate_hdfs:
            try:
                self._delete_intermediate(self.hdfs[path])
            except:
                pass


class _Step:
    """
    Representa un paso en el proceso de transformación de datos.
    """
    def __init__(self, parent:"PayrollStreaksAggregate") -> None:
        self.parent = parent
        self.hdfs = self.parent.hdfs
        self.sqlContext = self.parent.sqlContext
        self.is_dynamic = self.parent.is_dynamic
        return None
    #
    def _c(self, col_name:str) -> str:
        """
        gets the col_name
        """
        return self.parent._c(col_name)
    #
    def _save_parquet(self, df_save:DataFrame, path:str) -> None:
        """
        Guarda los datos en formato parquet en la ubicación deseada.
        """
        return self.parent._save_parquet(df_save, path)


class _FilterStep(_Step):
    """Mandatory Step
    Lee las tablas de entrada, Asigna Nomina y Nomina.

    Con Asigna Nomina se obtiene la información de los clientes y
    sus dos tefs principales, dando la prioridad al mes más reciente,
    pero si no hay información, se toma la de los meses anteriores,
    priorizando la información más reciente.

    Se usa Asigna Nomina para filtrar solo los tefs principales de Nomina.

    Se marca con tef_1 al tef principal y tef_2 al secundario.

    Args:
        _Step (_type_): _description_
    """
    def __init__(self, parent:"PayrollStreaksAggregate") -> None:
        """_summary_

        Args:
            parent (PayrollStreaksAggregate): _description_

        Returns:
            _type_: _description_
        """
        super().__init__(parent)
        self.is_dynamic = True
        return None
    #
    @cached_property
    def asigna(self) -> DataFrame:
        """"""
        result = self.parent.sqlContext.read.parquet(self.hdfs["asigna"])
        result = result.withColumn(self._c("tef"), result[self._c("tef")].cast("long"))
        result = result.withColumn(self._c("tef_1"), result[self._c("tef_1")].cast("long"))
        result = result.withColumn(self._c("tef_2"), result[self._c("tef_2")].cast("long"))
        result = result.withColumn(self._c("tfrom_month"), trunc(self._c("tfrom_date"), "month"))
        result = result.drop(self._c("process_date"))
        return result
    #
    @cached_property
    def nomina(self) -> DataFrame:
        """"""
        result = self.sqlContext.read.parquet(self.hdfs["nomina"])
        result = result.withColumn(self._c("tef"), result[self._c("tef")].cast("long"))
        result = result.withColumn(self._c("tfrom_month"), trunc(self._c("tfrom_date"), "month"))
        result = result.drop(self._c("process_date"))
        return result
    #
    @cached_property
    def asigna_client_tefs(self) -> DataFrame:
        """"""
        result = fill_and_select_latest(
            df = self.asigna.select(self._c("vintage_date"), self._c("numcliente"), self._c("tef_1"), self._c("tef_2"), self._c("tfrom_month")),
            id_cols = [self._c("vintage_date"), self._c("numcliente")],
            date_col = self._c("tfrom_month"),
            fill_cols = [self._c("tef_1"), self._c("tef_2")]
        )
        result.distinct()
        #
        return result
    #
    @cached_property
    def tef_stack(self) -> DataFrame:
        """"""
        df:DataFrame = self.asigna_client_tefs.select(self._c("vintage_date"), self._c("numcliente"), self._c("tef_1"), self._c("tef_2"))
        tef_cols = [self._c("tef_1"), self._c("tef_2")]
        #
        # Crear una lista de expresiones para pivotar las columnas
        exprs = [f"stack({len(tef_cols)}, " + ", ".join([f"'{col_name}', {col_name}" for col_name in tef_cols]) + f") as ({self._c('n_tef')}, {self._c('tef')})"]
        # Aplicar selectExpr para pivotar las columnas
        df_transformed = df.selectExpr(self._c("vintage_date"), self._c("numcliente"), *exprs)
        # Filtrar las filas con valores nulos
        df_filtered = df_transformed.filter(col(self._c("tef")).isNotNull())
        #
        df_filtered = df_filtered.distinct()
        #
        return df_filtered
    #
    @cached_property
    def nomina_filtered_by_tef_principals(self) -> DataFrame:
        """"""
        nomina: DataFrame = self.nomina.select(
            self._c("numcliente"), self._c("tef"), self._c("importe"),
            self._c("tfrom_date"), self._c("tfrom"), self._c("tfrom_month"), self._c("vintage_date"),
        )
        # Castear la columna vintage_date a string para el join
        nomina = nomina.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        #
        tef_stack: DataFrame = (
            self.tef_stack
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        )
        # Realizar el join utilizando las columnas casteadas como string
        joined_tef_1 = nomina.join(
            tef_stack,
            on=[self._c("vintage_date"), self._c("numcliente"), self._c("tef")],
            how="left"
        )
        # Volver a castear la columna vintage_date a tipo date
        joined_tef_1 = joined_tef_1.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("date"))
        # Eliminar filas con valores nulos
        joined_tef_1 = joined_tef_1.na.drop(subset=[self._c("n_tef")])
        #
        return joined_tef_1
    #
    @cached_property
    def add_tefs(self) -> DataFrame:
        """"""
        result:DataFrame = self.nomina_filtered_by_tef_principals
        #
        columns_to_aggregate = [self._c("importe"), self._c("tef")]
        groupby_columns = [column for column in result.columns if column not in columns_to_aggregate + [self._c("n_tef")]]
        aggregate = [spark_sum(self._c("importe")).alias(self._c("importe")), first(self._c("tef")).alias(self._c("tef"))]
        #
        # Group by columns
        grouped = result\
            .groupBy(*groupby_columns)\
            .agg(*aggregate)
        #
        # add" n_tef"
        grouped = grouped.withColumn(self._c("n_tef"), lit(self._c("tefs")))
        #
        # Select and reorder columns to match the original DataFrame
        grouped = grouped.select(result.columns)
        #
        # Union the original DataFrame with the new DataFrame containing the aggregated values
        result = result.union(grouped)
        # Remove tef_2
        result = result.filter(col(self._c("n_tef")) != self._c("tef_2"))
        return result
    #
    @cached_property
    @dynamic(path_key="week_tef")
    def groupby_week_tef(self) -> DataFrame:
        """"""
        result:DataFrame = self.add_tefs\
            .groupBy(self._c("vintage_date"), self._c("numcliente"), self._c("tfrom"),  self._c("n_tef")).agg(
                spark_sum(self._c("importe")).alias("sum_importe"),
                spark_min(self._c("tfrom_date")).alias(self._c("tfrom_date")),
                spark_min(self._c("tfrom_month")).alias(self._c("tfrom_month"))
            )
        result = result.withColumn("paid_in_week", when(col("sum_importe") > 0, 1).otherwise(0).cast("tinyint"))
        result = result.drop("sum_importe")
        return result
    #
    @cached_property
    def dates_collection(self) -> list:
        """"""
        result:DataFrame = self.groupby_week_tef
        vintage_dates = [row["vintage_date"] for row in result.select("vintage_date").distinct().collect()]
        day_of_week_starts = [22, 16, 8, 1]
        #
        tfroms = self.parent.tfroms
        tfroms.sort()
        last_tfrom = tfroms[-1]
        #
        collection = []
        # TODO: Correct tfroms, weeks, module.
        for vintage_date in vintage_dates:
            tfrom = float(tfroms[0])
            tfrom_in_months = 0
            # first day of the month
            tfrom_month = vintage_date - relativedelta(day=1) + relativedelta(months=1)
            while tfrom <= last_tfrom:
                tfrom_month = tfrom_month - relativedelta(months = 1, day=1)
                for day_of_week_start in day_of_week_starts:
                    tfrom_date = tfrom_month - relativedelta(day=day_of_week_start)
                    collection.append(Row(vintage_date=vintage_date, tfrom_date=tfrom_date, tfrom=tfrom, tfrom_month=tfrom_month))
                    tfrom += 1.0
                tfrom_in_months += 1
        return collection
    #
    @cached_property
    def original_pivot(self) -> DataFrame:
        """"""
        pivot = self.sqlContext.read.parquet(self.hdfs["pivot"])
        try:
            pivot = pivot.withColumnRenamed("numcliente", self._c("numcliente"))
        except:
            pass
        #
        try:
            pivot = pivot.withColumnRenamed("vintage", self._c("vintage_date"))
        except:
            pass
        #
        pivot = pivot.select(self._c("numcliente"), self._c("vintage_date"))
        return pivot
    #
    @cached_property
    def pivot(self) -> DataFrame:
        """"""
        pivot_asigna:DataFrame = self.asigna
        pivot_asigna = pivot_asigna.select(self._c("numcliente")).distinct()
        #
        pivot_nomina:DataFrame = self.nomina
        pivot_nomina = pivot_nomina.select(self._c("numcliente")).distinct()
        #
        original_pivot:DataFrame = self.original_pivot
        original_pivot_clients = original_pivot.select(self._c("numcliente")).distinct()
        #
        pivot = pivot_nomina.union(pivot_asigna).union(original_pivot_clients).distinct()
        #
        for k, row in enumerate(self.dates_collection):
            for j, tef_value in enumerate(self.parent.tef_list):
                row_df = pivot
                row_df = row_df.withColumn(self._c("vintage_date"), lit(row[self._c("vintage_date")]))
                row_df = row_df.withColumn(self._c("tfrom_date"), lit(row[self._c("tfrom_date")]))
                row_df = row_df.withColumn(self._c("tfrom_month"), lit(row[self._c("tfrom_month")]))
                row_df = row_df.withColumn(self._c("tfrom"), lit(row[self._c("tfrom")]))
                row_df = row_df.withColumn(self._c("n_tef"), lit(tef_value))
                if (k == 0) and (j == 0):
                    result = row_df
                else:
                    result = result.union(row_df)
        #
        # Generate new pivot with vintage dates
        pivot_with_vintage_date_for_join = original_pivot.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        result_for_join = result.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        #
        filtered_pivot = pivot_with_vintage_date_for_join.join(
            other=result_for_join,
            on=[self._c("vintage_date"), self._c("numcliente")],
            how="left"
        )
        #
        filtered_pivot = filtered_pivot.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("date"))
        return filtered_pivot
    #
    @cached_property
    def expand_missing_info(self) -> DataFrame:
        """
        Crea todos los tfroms para todos los clientes de acuerdo a su vintage_date, tomando como referencia
        la columna dates_collection.

        Para luego hacer un left join con la tabla groupby_week_tef para rellenar los tfroms faltantes.
        """
        pivot_for_join:DataFrame = (
            self.pivot
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
            .withColumn(self._c("tfrom_date"), col(self._c("tfrom_date")).cast("string"))
            .withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("string"))
        )
        groupby_week_tef_for_join: DataFrame = (
            self.groupby_week_tef
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
            .withColumn(self._c("tfrom_date"), col(self._c("tfrom_date")).cast("string"))
            .withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("string"))
        )
        # Realizar el join utilizando las columnas casteadas como string
        result = pivot_for_join.join(
            groupby_week_tef_for_join,
            on=[
                self._c("vintage_date"),
                self._c("numcliente"),
                self._c("tfrom_date"),
                self._c("tfrom"),
                self._c("tfrom_month"),
                self._c("n_tef"),
            ],
            how="left"
        )
        # Volver a castear las columnas a tipo date
        result = (
            result
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("date"))
            .withColumn(self._c("tfrom_date"), col(self._c("tfrom_date")).cast("date"))
            .withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("date"))
        )
        return result
    #
    @cached_property
    @dynamic(path_key="filtered")
    def groupby_week_tef_missing_treatment(self) -> DataFrame:
        """"""
        result:DataFrame = self.expand_missing_info
        result = result.withColumn("paid_in_week", coalesce(col("paid_in_week"), lit(0)))
        result = result.withColumn(self._c("n_tef"), coalesce(self._c("n_tef"), lit(-99999)))
        return result
    #
    @cached_property
    def final(self) -> DataFrame:
        return self.groupby_week_tef_missing_treatment


class _TaggerStep(_Step):
    """Mandatory step
    Etiqueta los pagos por semana, quincena y mes.

    Se etiqueta el pago con la semana del año, el dia de la semana,
    la quincena del mes y la semana del mes.

    Se cuenta el número de pagos en la semana, quincena y mes.
    """
    def __init__(self, parent:"PayrollStreaksAggregate") -> None:
        super().__init__(parent)
        return None
    #
    @cached_property
    def with_weeks_of_month(self) -> DataFrame:
        """"""
        result:DataFrame = self.parent.filter_step.final
        result = self._get_new_week_of_year_day_of_month_week_of_month_object(
            df=result,
            date_column=self._c("tfrom_date"),
            week_of_year_column="week_of_year",
            day_of_week_column="day_of_week",
        )
        result = result.drop("day_of_week", "week_of_year")
        return result
    #
    @cached_property
    def payment_frequencies(self) -> DataFrame:
        """"""
        result:DataFrame = self.with_weeks_of_month
        # Agrupar por mes y calcular el conteo de pagos y recoger week_of_month en un array
        result = result.groupBy(self._c("vintage_date"), self._c("numcliente"), self._c("n_tef"), self._c("tfrom_month"))\
            .agg(
                spark_sum("paid_in_week").alias("count_paid_in_month"),
                sort_array(collect_list(when(col("paid_in_week") == 1, col("week_of_month")).otherwise(None))).alias("weeks_of_month"),
            )
        #
        return result
    #
    @cached_property
    def payment_type_per_month(self) -> DataFrame:
        """"""
        result:DataFrame = self.payment_frequencies
        # Add new column with type of payment
        result = result.withColumn(
            "payment_type_per_month",
            when(col("count_paid_in_month") == 4, "semanal")
            .when(col("count_paid_in_month") == 3,
                when((array_contains(col("weeks_of_month"), 1)) & (array_contains(col("weeks_of_month"), 2)) & (array_contains(col("weeks_of_month"), 4)), "semanal")
                .otherwise("quincenal"))
            .when(col("count_paid_in_month") == 2,
                when((array_contains(col("weeks_of_month"), 1) & array_contains(col("weeks_of_month"), 4)), "mensual")
                .otherwise("quincenal"))
            .when(col("count_paid_in_month") == 1, "mensual")
            .otherwise("indeterminado")
        )
        return result
    #
    @cached_property
    def client_payment_type(self) -> DataFrame:
        """"""
        df:DataFrame = self.payment_type_per_month
        #
        # Group by vintage_date, numcliente, n_tef and count payment_type
        df_grouped = df.groupBy(self._c("vintage_date"), self._c("numcliente"), self._c("n_tef"), "payment_type_per_month").count()
        #
        # Pivot the table to get counts of each payment_type
        df_pivot = df_grouped.groupBy(self._c("vintage_date"), self._c("numcliente"), self._c("n_tef")).pivot("payment_type_per_month").sum("count").fillna(0)
        #
        # Add a column to determine the most frequent payment_type based on the given priority
        df_pivot = df_pivot.withColumn(
            "max_payment_type",
            greatest(col("semanal"), col("quincenal"), col("mensual"))
        )
        # Fix in case there are not present all payment_type in pivot. (Suggested by MONICA TOLEDO)
        payment_type_columns = ["semanal", "quincenal", "mensual"]
        for payment_type in payment_type_columns:
            if payment_type not in df_pivot.columns:
                df_pivot = df_pivot.withColumn(payment_type, lit(0).cast("long"))
            else:
                pass
        # Add a column to determine the most frequent payment_type based on the given priority
        result = df_pivot.withColumn(
            "client_payment_type",
            when((col("max_payment_type") == col("mensual")), "mensual")
            .when((col("max_payment_type") == col("quincenal")), "quincenal")
            .when((col("max_payment_type") == col("semanal")), "semanal")
            .otherwise("indeterminado")
        )
        # Replace "indeterminado" with "mensual"
        result = result.withColumn(
            "client_payment_type",
            when(col("client_payment_type") == "indeterminado", "mensual").otherwise(col("client_payment_type"))
        )
        #
        result = result.drop("max_payment_type")
        return result
    #
    @cached_property
    @dynamic(path_key="payment_frequencies")
    def deaggregate(self) -> DataFrame:
        """"""
        df_payment_type_per_month: DataFrame = self.payment_type_per_month
        df_client_payment_type: DataFrame = self.client_payment_type.drop("semanal", "quincenal", "mensual", "indeterminado")
        df_filter_step: DataFrame = self.with_weeks_of_month
        #
        # Castear columnas a string para el join
        df_payment_type_per_month = df_payment_type_per_month.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        df_client_payment_type = df_client_payment_type.withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
        #
        # Realizar el primer join
        result = df_payment_type_per_month.join(
            df_client_payment_type,
            on=[self._c("vintage_date"), self._c("numcliente"), self._c("n_tef")],
            how="left"
        )
        #
        # Castear columnas a string para el segundo join
        df_filter_step = (
            df_filter_step
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("string"))
            .withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("string"))
        )
        result = result.withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("string"))
        # Realizar el segundo join
        final_result = df_filter_step.join(
            result,
            on=[self._c("vintage_date"), self._c("numcliente"), self._c("n_tef"), self._c("tfrom_month")],
            how="left"
        )
        # Volver a castear las columnas a tipo original
        final_result = (
            final_result
            .withColumn(self._c("vintage_date"), col(self._c("vintage_date")).cast("date"))
            .withColumn(self._c("tfrom_month"), col(self._c("tfrom_month")).cast("date"))
        )
        return final_result
    #
    @staticmethod
    def _get_new_week_of_year_day_of_month_week_of_month_object(df: DataFrame, date_column:str, week_of_year_column:str, day_of_week_column:str) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y dia de la semana usando un objeto especifico."""
        # Forced 4 weeks months
        date_df = df
        columns_before = date_df.columns
        test_object = WeekOfMonthPayroll(
            df=date_df,
            date_column=date_column
        )
        tagged_columns = [WeekOfMonthPayroll.new_week_of_year_column, WeekOfMonthPayroll.new_day_of_week_column, "new_week_of_month"]
        result:DataFrame = test_object.new_week_of_year_day_of_month
        result = result.select(columns_before + tagged_columns)
        result = result.withColumnRenamed(existing=WeekOfMonthPayroll.new_week_of_year_column, new=week_of_year_column)
        result = result.withColumnRenamed(existing=WeekOfMonthPayroll.new_day_of_week_column, new=day_of_week_column)
        result = result.withColumnRenamed(existing="new_week_of_month", new="week_of_month")
        return result
    #
    @cached_property
    def final(self) -> DataFrame:
        return self.deaggregate


class _RenamerStep(_Step):
    """Optional step
    Utiliza un diccionario de remobrado para cambiar el nombre de las columnas.
    """
    def __init__(self, parent:"PayrollStreaksAggregate", df: DataFrame, rename_dict:dict) -> None:
        self.df = df
        self.columns_to_rename:Dict[str,str] = rename_dict["columns_to_rename"]
        self.is_enabled = parse_config_bool(
            rename_dict["enable"], "is_enabled", none_is_false=True)
        #
        super().__init__(parent)
        return None
    #
    @cached_property
    def renamed(self) -> DataFrame:
        if not self.is_enabled:
            return self.df
        #
        result:DataFrame = self.df
        for old_name, new_name in self.columns_to_rename.items():
            result = result.withColumnRenamed(old_name, new_name)
        return result
    #
    @cached_property
    def final(self) -> DataFrame:
        return self.renamed


class _PatternStep(_Step):
    """Optional step
    Aplica un patrón de pago a las columnas del DataFrame.
    """
    def __init__(self, parent:"PayrollStreaksAggregate", input_df:DataFrame, step_config:Dict[str, Union[str, int, dict]], step_method:Callable[[DataFrame, List[str], str], DataFrame], previous_prefix:str) -> None:
        self.df = input_df
        self.step_config = step_config
        self.prefix = self.step_config["prefix"]
        self.prefixes_to_apply = self.step_config["prefixes_to_apply"]
        self.step_method = step_method
        # enabled
        self.enabled = parse_config_bool(step_config["enable"], "enable")
        self.prefixes_to_drop = self.step_config.get("prefixes_to_drop", [])
        # optional attributes
        self.optional_attributes = {
            "remove_previous_prefix": self.step_config.get("remove_previous_prefix", False)
        }
        self.remove_previous_prefix = parse_config_bool(
            self.step_config.get("remove_previous_prefix", "false"),
            "remove_previous_prefix")
        self.previous_prefix = previous_prefix
        self._columns_not_to_be_renamed = parent._columns_not_to_be_renamed
        #
        super().__init__(parent)
        return None
    #
    def _get_columns_to_apply(self, df: DataFrame) -> List[str]:
        """Obtiene las columnas a las que se aplicará el patrón."""
        return self.get_columns_with_prefixes(df, self.prefixes_to_apply)
    #
    def _get_columns_with_prefix(self, df: DataFrame, prefix:Optional[str]="None") -> List[str]:
        """Columnas de ``df`` con el prefijo dado (ver
        :func:`libs.aggregattes.base.columns_with_prefix`)."""
        return columns_with_prefix(
            df.columns, self.parent.prefixes,
            self._columns_not_to_be_renamed, prefix)
    #
    def get_columns_with_prefixes(self, df: DataFrame, prefixes:List[str]) -> List[str]:
        """Unión sin duplicados de las columnas de cada prefijo."""
        return columns_with_prefixes(
            df, self.parent.prefixes, self._columns_not_to_be_renamed,
            prefixes)
    #
    def sort_columns(self, df: DataFrame) -> DataFrame:
        """``protected`` primero, resto ordenado alfabéticamente."""
        return sort_df_columns(df, self._columns_not_to_be_renamed)
    #
    @cached_property
    def columns_to_apply(self) -> List[str]:
        return self._get_columns_to_apply(self.df)
    #
    @cached_property
    def final(self) -> DataFrame:
        if not self.enabled:
            return self.df
        result = self.step_method(
            df=self.df,
            columns_to_apply=self.columns_to_apply,
            prefix=self.prefix,
            remove_previous_prefix=self.remove_previous_prefix,
            previous_prefix=self.previous_prefix,
        )
        if len(self.prefixes_to_drop) > 0:
            columns_to_drop = self.get_columns_with_prefixes(result, self.prefixes_to_drop)
            result = result.drop(*columns_to_drop)
        #
        result = self.sort_columns(result)
        return result


class _PivotNumTefStep(_Step):
    #
    def __init__(self, parent:"PayrollStreaksAggregate") -> None:
        """
        Inicializa una instancia de la clase _PivotNumTefStep.

        Args:
            parent (PayrollStreaksAggregate): Instancia del objeto padre.
        """
        self.column_not_to_be_renamed:List[str] = parent.patterns_step._columns_not_to_be_renamed
        super().__init__(parent)
        return None
    #
    @cached_property
    @dynamic(path_key="patterned")
    def raw_patterned(self) -> DataFrame:
        """Obtiene el DataFrame final del paso de patrones.

        Returns:
            DataFrame: DataFrame resultante del paso de patrones.
        """
        return self.parent.patterns_step.final
    #
    @cached_property
    def patterned(self) -> DataFrame:
        """Obtiene el DataFrame con las columnas innecesarias eliminadas."""
        columns_to_drop = [
            self._c("tfrom_month"),
            "count_paid_in_month",
            "payment_type_per_month",
            "client_payment_type",
            "weeks_of_month",
            "week_of_month"
        ]
        #
        result:DataFrame = self.raw_patterned
        result = result.drop(*columns_to_drop)
        return result
    #
    @cached_property
    @dynamic(path_key="aggregate")
    def pivot(self) -> DataFrame:
        """Realiza el pivoteo de la columna n_tef y agrega las columnas correspondientes.

        Returns:
            DataFrame: DataFrame con las columnas pivoteadas y agregadas.
        """
        result:DataFrame = self.patterned
        # Column reading
        group_by_columns = [self._c("vintage_date"), self._c("numcliente"), self._c("tfrom"), self._c("tfrom_date")]
        pivot_columns = [self._c("n_tef")]
        aggregate_columns = [column for column in result.columns if column not in group_by_columns + pivot_columns]
        assert len(aggregate_columns) > 1, "El metodo de pivot tef requiere al menos dos columnas para pivotea?"
        aggregate_opps = [first(column).alias(column) for column in aggregate_columns]
        #
        # renaming
        pivoted_renames =  group_by_columns + [
            col(f"{tef}_{column}").alias(f"{column}_{tef}")
            for column in aggregate_columns
            for tef in self.parent.tef_list
        ]
        #
        result = result.groupBy(*group_by_columns) \
            .pivot(pivot_columns[0]) \
            .agg(*aggregate_opps)
        # change name
        result = result.select(*pivoted_renames)
        return result
    #
    @cached_property
    def final(self) -> DataFrame:
        """Obtiene el DataFrame final después de realizar el pivoteo."""
        return self.pivot


def raw_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """Copia el patrón sin modificar a las nuevas columnas.

    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.

    Returns:
        DataFrame: DataFrame con las nuevas columnas con el patrón sin modificar.
    """
    # __GAP__ rango no fotografiado (líneas ~1480-1510): se reconstruye como passthrough del patrón
    result = df
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col = f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        result = result.withColumn(output_col, col(column))
    return result


def sorted_shortened_pattern_forced_monthly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Ordena e imputa y aplica a todos los clientes el patrón mensual sin importar su tipo.

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    # __GAP__ rango no fotografiado (líneas ~1510-1557): reconstruido siguiendo la plantilla de las otras reglas
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("count_paid_in_month") >= 1)
                & col("week_of_month").isin([4,3]), 1))
        result = result.withColumn("fill_null", when(col("week_of_month").isin([1, 2]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def sorted_shortened_pattern_forced_fortnightly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Ordena e imputa y aplica a todos los clientes el patrón quincenal sin importar su tipo.

        Reglas:
        - client_payment_type es "mensual"
            - `count_paid_in_month` >= 1
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
        - client_payment_type es "quincenal"
            - `count_paid_in_month` >= 3
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 2
                - (4 y 3) o (1 y 2) están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 0
                    - `week_of_month` == 4 -> 1
                - otros:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 1
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 0
                - `week_of_month` == 4 -> 1
        - client_payment_type es "semanal"
            - `count_paid_in_month` == 4
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 3
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 0
                - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 2
                - [2,1] o [3,4] están en `weeks_of_month`
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 0
                    - `week_of_month` == 4 -> 1

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("client_payment_type") == "mensual")
                & (col("count_paid_in_month") >= 1)
                & col("week_of_month").isin([4,3]), 1)
            .when((col("client_payment_type") == "quincenal")
                , when((col("count_paid_in_month") >=3)
                    & col("week_of_month").isin([4,3]), 1)
                .when(col("count_paid_in_month") == 2
                    , when((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3))
                        | (array_contains("weeks_of_month", 1) & array_contains("weeks_of_month", 2))
                        , when(col("week_of_month").isin([4]), 1)
                        .otherwise(when(col("week_of_month").isin([4,3]), 1))))
                .when((col("count_paid_in_month") == 1)
                    & col("week_of_month").isin([4]), 1))
            .when((col("client_payment_type") == "semanal")
                , when(col("count_paid_in_month") == 4
                    , when(col("week_of_month").isin([4,3]), 1))
                .when(col("count_paid_in_month") == 3
                    , when(((array_contains("weeks_of_month", 3) & array_contains("weeks_of_month", 2) & array_contains("weeks_of_month", 1))
                        | (array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 2) & array_contains("weeks_of_month", 1)))
                        & col("week_of_month").isin([4]),1)
                    .when((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3) & array_contains("weeks_of_month", 1))
                        & col("week_of_month").isin([4]),1))
                .when((col("count_paid_in_month") == 2)
                    , when((array_contains("weeks_of_month", 2) & array_contains("weeks_of_month", 1))
                        & col("week_of_month").isin([4]), 1)
                    .when((array_contains("weeks_of_month", 3) & array_contains("weeks_of_month", 4))
                        & col("week_of_month").isin([4]), 1))
            )
        )
        result = result.withColumn("fill_null", when(col("week_of_month").isin([1, 2]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def sorted_shortened_pattern_forced_weekly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Ordena e imputa y aplica a todos los clientes el patrón semanal sin importar su tipo.

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("client_payment_type") == "mensual")
                , when(col("count_paid_in_month") >= 1
                    , 1))
            .when((col("client_payment_type") == "quincenal")
                , when(col("count_paid_in_month") >= 3
                    , 1)
                .when(col("count_paid_in_month") == 2
                , when(array_contains("weeks_of_month", 4) | array_contains("weeks_of_month", 3)
                    , when(col("week_of_month").isin([4,3]), 1))
                .when(array_contains("weeks_of_month", 1) | array_contains("weeks_of_month", 2)
                    , when(col("week_of_month").isin([4,3]), 1))
                    .otherwise(1)))
            .when((col("client_payment_type") == "semanal"),
                when((col("count_paid_in_month") == 4), 1)
                .when((col("count_paid_in_month") == 3) & col("week_of_month").isin([4, 3, 2]), 1)
                .when((col("count_paid_in_month") == 2) & col("week_of_month").isin([4, 3]), 1)
                .when((col("count_paid_in_month") == 1) & col("week_of_month").isin([4]), 1))
            )
        result = result.withColumn(output_col, coalesce(col(output_col), lit(0)))
        #
    return result


def sorted_shortened_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """
Ordena e imputa y aplica a los clientes el patrón correspondiente a su tipo.

Reglas:
- Cuando client_payment_type es "mensual":
    - Es el shortened mensual para todos los casos.
- Cuando client_payment_type es "quincenal":
    - Es el shortened quincenal para todos los casos.
- Cuando client_payment_type es "semanal":
    - Es el shortened semanal para todos los casos.

Args:
    df (DataFrame): El DataFrame de entrada.
    columns_to_apply (List[str]): Lista de columnas a las que se les aplicará el prefijo.
    prefix (str): Prefijo a aplicar a las columnas.
    remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior.
    previous_prefix (Optional[str]): Prefijo anterior a eliminar.

Returns:
    DataFrame: Un nuevo DataFrame con las columnas renombradas.
"""
    result = df
    # client_payment_type
    tmp_prefix_mensual = f"tmp_{prefix}_mensual"
    tmp_prefix_quincenal = f"tmp_{prefix}_quincenal"
    tmp_prefix_semanal = f"tmp_{prefix}_semanal"
    result = sorted_shortened_pattern_forced_monthly_rule(result, columns_to_apply, tmp_prefix_mensual, remove_previous_prefix,
    previous_prefix)
    result = sorted_shortened_pattern_forced_fortnightly_rule(result, columns_to_apply, tmp_prefix_quincenal,
    remove_previous_prefix, previous_prefix)
    result = sorted_shortened_pattern_forced_weekly_rule(result, columns_to_apply, tmp_prefix_semanal, remove_previous_prefix,
    previous_prefix)

    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when(col("client_payment_type") == "mensual", col(output_tmp_mensual))
            .when(col("client_payment_type") == "quincenal", col(output_tmp_quincenal))
            .otherwise(col(output_tmp_semanal))
        )
        result = result.drop(output_tmp_mensual, output_tmp_quincenal, output_tmp_semanal)
        #
    return result


def shortened_pattern_forced_monthly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Imputa y aplica a todos los clientes el patrón mensual sin importar su tipo.

        Reglas:
        - `count_paid_in_month` >= 1
            - `week_of_month` == 1 -> 9
            - `week_of_month` == 2 -> 9
            - `week_of_month` == 3 -> 0
            - `week_of_month` == 4 -> 1

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("count_paid_in_month") >= 1)
                & col("week_of_month").isin([4]), 1))
        result = result.withColumn("fill_null", when(col("week_of_month").isin([1, 2, 3]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def shortened_pattern_forced_fortnightly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Imputa y aplica a todos los clientes el patrón quincenal sin importar su tipo.

        Reglas:
        - `count_paid_in_month` >= 3
            - `week_of_month` == 1 -> 9
            - `week_of_month` == 2 -> 9
            - `week_of_month` == 3 -> 1
            - `week_of_month` == 4 -> 1
        - `count_paid_in_month` == 2
            - 4 y 3 están en `weeks_of_month`:
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 0
                - `week_of_month` == 4 -> 1
            - 1 y 2 están en `weeks_of_month`:
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 0
            - otros:
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
        - `count_paid_in_month` == 1
            - 4 o 3 están en `weeks_of_month`:
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 0
                - `week_of_month` == 4 -> 1
            - 1 o 2 están en `weeks_of_month`:
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 0

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("count_paid_in_month") >=3)
                & col("week_of_month").isin([4,3]), 1)
            .when((col("count_paid_in_month") == 2)
                , when((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3))
                    & col("week_of_month").isin([4]), 1)
                .when((array_contains("weeks_of_month", 1) & array_contains("weeks_of_month", 2))
                    & col("week_of_month").isin([3]), 1)
                .when(col("week_of_month").isin([4,3]), 1))
            .when((col("count_paid_in_month") == 1)
                , when((array_contains("weeks_of_month", 4) | array_contains("weeks_of_month", 3))
                    & col("week_of_month").isin([4]), 1)
                .when((array_contains("weeks_of_month", 1) | array_contains("weeks_of_month", 2))
                    & col("week_of_month").isin([3]), 1))
            )
        result = result.withColumn("fill_null", when(col("week_of_month").isin([1, 2]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def shortened_pattern_forced_weekly_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Imputa y aplica a todos los clientes el patrón semanal sin importar su tipo.

        Reglas:
        - client_payment_type es "mensual"
            - `count_paid_in_month` >= 1
                - `week_of_month` == 1 -> 1
                - `week_of_month` == 2 -> 1
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
        - client_payment_type es "quincenal"
            - `count_paid_in_month` >= 3
                - `week_of_month` == 1 -> 1
                - `week_of_month` == 2 -> 1
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 2
                - 4 y 3 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 0
                    - `week_of_month` == 2 -> 0
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 1
                - 1 y 2 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 1
                    - `week_of_month` == 2 -> 1
                    - `week_of_month` == 3 -> 0
                    - `week_of_month` == 4 -> 0
                - otros:
                    - `week_of_month` == 1 -> 1
                    - `week_of_month` == 2 -> 1
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 1
                - 4 o 3 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 0
                    - `week_of_month` == 2 -> 0
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 1

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("client_payment_type") == "mensual")
                & (col("count_paid_in_month") >= 1), 1)
            .when((col("client_payment_type") == "quincenal")
                , when((col("count_paid_in_month") >= 3), 1)
                .when(col("count_paid_in_month") == 2
                , when((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3))
                    & col("week_of_month").isin([4,3]), 1)
                .when((array_contains("weeks_of_month", 1) & array_contains("weeks_of_month", 2))
                    & col("week_of_month").isin([2,1]), 1)
                .otherwise(1))
                .when((col("count_paid_in_month") == 1)
                    & (array_contains("weeks_of_month", 4) | array_contains("weeks_of_month", 3))
                    & col("week_of_month").isin([4,3]), 1))
            .when((col("client_payment_type") == "semanal"),
                when((col("count_paid_in_month") == 4), 1)
                .when((col("count_paid_in_month") == 3) & col("week_of_month").isin([4, 3, 2]), 1)
                .when((col("count_paid_in_month") == 2) & col("week_of_month").isin([4, 3]), 1)
                .when((col("count_paid_in_month") == 1) & col("week_of_month").isin([4]), 1))
            )
        result = result.withColumn(output_col, coalesce(col(output_col), lit(0)))
        #
    return result


def shortened_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """
Imputa y aplica a los clientes el patrón correspondiente a su tipo.

Reglas:
- Cuando client_payment_type es "mensual":
    - Es el shortened mensual para todos los casos.
- Cuando client_payment_type es "quincenal":
    - Es el shortened quincenal para todos los casos.
- Cuando client_payment_type es "semanal":
    - Es el shortened semanal para todos los casos.

Args:
    df (DataFrame): El DataFrame de entrada.
    columns_to_apply (List[str]): Lista de columnas a las que se les aplicará el prefijo.
    prefix (str): Prefijo a aplicar a las columnas.
    remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior.
    previous_prefix (Optional[str]): Prefijo anterior a eliminar.

Returns:
    DataFrame: Un nuevo DataFrame con las columnas renombradas.
"""
    result = df
    # client_payment_type
    tmp_prefix_mensual = f"tmp_{prefix}_mensual"
    tmp_prefix_quincenal = f"tmp_{prefix}_quincenal"
    tmp_prefix_semanal = f"tmp_{prefix}_semanal"
    result = shortened_pattern_forced_monthly_rule(result, columns_to_apply, tmp_prefix_mensual, remove_previous_prefix,
    previous_prefix)
    result = shortened_pattern_forced_fortnightly_rule(result, columns_to_apply, tmp_prefix_quincenal, remove_previous_prefix,
    previous_prefix)
    result = shortened_pattern_forced_weekly_rule(result, columns_to_apply, tmp_prefix_semanal, remove_previous_prefix,
    previous_prefix)

    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when(col("client_payment_type") == "mensual", col(output_tmp_mensual))
            .when(col("client_payment_type") == "quincenal", col(output_tmp_quincenal))
            .otherwise(col(output_tmp_semanal))
        )
        result = result.drop(output_tmp_mensual, output_tmp_quincenal, output_tmp_semanal)
        #
    return result


# optimist
def sorted_shortened_pattern_forced_monthly_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Ordena e imputa y aplica a todos los clientes el patrón mensual sin importar si su tipo.
        Es el mismo que shortened_pattern_forced_monthly_rule

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    return shortened_pattern_forced_monthly_optimist_rule(df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix)


def sorted_shortened_pattern_forced_fortnightly_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Ordena e imputa y aplica a todos los clientes el patrón quincenal sin importar su tipo.
        Es el mismo que shortened_pattern_forced_fortnightly_rule

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    return shortened_pattern_forced_fortnightly_optimist_rule(df, columns_to_apply, prefix, remove_previous_prefix, previous_prefix)


def sorted_shortened_pattern_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """
Ordena e imputa y aplica a los clientes el patrón correspondiente a su tipo.

Reglas:
- Cuando client_payment_type es "mensual":
    - Es el shortened mensual para todos los casos.
- Cuando client_payment_type es "quincenal":
    - Es el shortened quincenal para todos los casos.
- Cuando client_payment_type es "semanal":
    - Es el shortened semanal para todos los casos.

Args:
    df (DataFrame): El DataFrame de entrada.
    columns_to_apply (List[str]): Lista de columnas a las que se les aplicará el prefijo.
    prefix (str): Prefijo a aplicar a las columnas.
    remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior.
    previous_prefix (Optional[str]): Prefijo anterior a eliminar.

Returns:
    DataFrame: Un nuevo DataFrame con las columnas renombradas.
"""
    result = df
    # client_payment_type
    tmp_prefix_mensual = f"tmp_{prefix}_mensual"
    tmp_prefix_quincenal = f"tmp_{prefix}_quincenal"
    tmp_prefix_semanal = f"tmp_{prefix}_semanal"
    result = sorted_shortened_pattern_forced_monthly_optimist_rule(result, columns_to_apply, tmp_prefix_mensual,
    remove_previous_prefix, previous_prefix)
    result = sorted_shortened_pattern_forced_fortnightly_optimist_rule(result, columns_to_apply, tmp_prefix_quincenal,
    remove_previous_prefix, previous_prefix)
    result = sorted_shortened_pattern_forced_weekly_rule(result, columns_to_apply, tmp_prefix_semanal, remove_previous_prefix,
    previous_prefix)

    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when(col("client_payment_type") == "mensual", col(output_tmp_mensual))
            .when(col("client_payment_type") == "quincenal", col(output_tmp_quincenal))
            .otherwise(col(output_tmp_semanal))
        )
        result = result.drop(output_tmp_mensual, output_tmp_quincenal, output_tmp_semanal)
        #
    return result


def shortened_pattern_forced_monthly_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Imputa y aplica a todos los clientes el patrón mensual sin importar su tipo.

        Reglas:
        - `count_paid_in_month` >= 1
            - `week_of_month` == 1 -> 9
            - `week_of_month` == 2 -> 9
            - `week_of_month` == 3 -> 0
            - `week_of_month` == 4 -> 1
        - `count_paid_in_month` == 2
            - No (4 y 3) ni (1 y 2) están en `weeks_of_month`
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 0
                - `week_of_month` == 4 -> 1

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("count_paid_in_month") >= 3)
                & col("week_of_month").isin([4]), 1)
            .when((col("count_paid_in_month") == 2)
                & (~((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3)) |
                (array_contains("weeks_of_month", 1) & array_contains("weeks_of_month", 2))))
                & col("week_of_month").isin([4]), 1)
        )
        result = result.withColumn("fill_null", when(col("week_of_month").isin([1, 2, 3]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def shortened_pattern_forced_fortnightly_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str,
    remove_previous_prefix:bool, previous_prefix:Optional[str]) -> DataFrame:
    """
        Imputa y aplica a los clientes el patrón quincenal.

        Reglas:
        - client_payment_type es "mensual"
            - `count_paid_in_month` >= 1
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
        - client_payment_type es "quincenal" o "semanal"
            - `count_paid_in_month` >= 3
                - `week_of_month` == 1 -> 9
                - `week_of_month` == 2 -> 9
                - `week_of_month` == 3 -> 1
                - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 2
                - 4 y 3 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 0
                    - `week_of_month` == 4 -> 1
                - 1 y 2 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 0
                - otros:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 1
            - `count_paid_in_month` == 1
                - 4 o 3 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 0
                    - `week_of_month` == 4 -> 1
                - 1 o 2 están en `weeks_of_month`:
                    - `week_of_month` == 1 -> 9
                    - `week_of_month` == 2 -> 9
                    - `week_of_month` == 3 -> 1
                    - `week_of_month` == 4 -> 0

    Returns:
        DataFrame: Un nuevo DataFrame con las columnas renombradas.
    """
    result = df
    # client_payment_type
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when((col("client_payment_type") == "mensual")
                & (col("count_paid_in_month") >= 1) & col("week_of_month").isin([4,3]), 1)
            .when((col("client_payment_type") == "quincenal") | (col("client_payment_type") == "semanal")
                , when((col("count_paid_in_month") >=3) & col("week_of_month").isin([4,3]), 1)
            .when(col("count_paid_in_month") == 2
                , when((array_contains("weeks_of_month", 4) & array_contains("weeks_of_month", 3))
                    , when(col("week_of_month").isin([4]), 1))
                .when((array_contains("weeks_of_month", 1) & array_contains("weeks_of_month", 2))
                    , when(col("week_of_month").isin([3]), 1))
                .otherwise(when(col("week_of_month").isin([3,4]), 1))
            )
            .when(col("count_paid_in_month") == 1
                , when((array_contains("weeks_of_month", 4) | array_contains("weeks_of_month", 3))
                    & col("week_of_month").isin([4]),1)
                .when((array_contains("weeks_of_month", 1) | array_contains("weeks_of_month", 2))
                    & col("week_of_month").isin([3]),1))
            )
        )
        result = result.withColumn("fill_null",  when(col("week_of_month").isin([1, 2]), 9).otherwise(0))
        result = result.withColumn(output_col, coalesce(col(output_col), col("fill_null")))
        result = result.drop("fill_null")
        #
    return result


def shortened_pattern_optimist_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """Imputa y aplica a los clientes el patrón de cada tipo de cliente

    Se le conoce como FIX 2

    Este método crea nuevas columnas en el DataFrame que indican el tipo de pago acortado
    basado en la columna `client_payment_type`. Las nuevas columnas se llenan con valores
    específicos según las reglas definidas.

    Reglas Aplicadas:
    - Cuando `client_payment_type` es "mensual", se aplica el patrón acortado mensual.
    - Cuando `client_payment_type` es "quincenal", se aplica el patrón acortado quincenal.
    - Cuando `client_payment_type` es "semanal" o "indeterminado", se aplica el patrón acortado semanal.

    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.

    Returns:
        DataFrame: DataFrame con las nuevas columnas que indican el tipo de pago acortado.
    """
    result = df
    # client_payment_type
    tmp_prefix_mensual = f"tmp_{prefix}_mensual"
    tmp_prefix_quincenal = f"tmp_{prefix}_quincenal"
    tmp_prefix_semanal = f"tmp_{prefix}_semanal"
    result = shortened_pattern_forced_monthly_optimist_rule(result, columns_to_apply, tmp_prefix_mensual,
    remove_previous_prefix, previous_prefix)
    result = shortened_pattern_forced_fortnightly_optimist_rule(result, columns_to_apply, tmp_prefix_quincenal,
    remove_previous_prefix, previous_prefix)
    result = shortened_pattern_forced_weekly_rule(result, columns_to_apply, tmp_prefix_semanal, remove_previous_prefix,
    previous_prefix)

    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col =  f"{prefix}_{column}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_mensual = f"{tmp_prefix_mensual}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_quincenal = f"{tmp_prefix_quincenal}_{column.replace(f'{previous_prefix}_', '')}"
            output_tmp_semanal = f"{tmp_prefix_semanal}_{column.replace(f'{previous_prefix}_', '')}"
        #
        result = result.withColumn(output_col,
            when(col("client_payment_type") == "mensual", col(output_tmp_mensual))
            .when(col("client_payment_type") == "quincenal", col(output_tmp_quincenal))
            .otherwise(col(output_tmp_semanal))
        )
        result = result.drop(output_tmp_mensual, output_tmp_quincenal, output_tmp_semanal)
        #
    return result


def payment_type_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """Son 4 patrones de tipo mensual, que simplemente marcan uno sin un cliente
    fue indeterminado, semanal, quincenal o mensual.

    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.

    Returns:
        DataFrame: DataFrame con las nuevas columnas que indican el tipo de pago por semana.
    """
    result = df
    col_to_apply = "payment_type_per_month"

    result = result.withColumn("fill_null",  when(col("week_of_month").isin([1, 2, 3]), 9).otherwise(0))
    result = result.withColumn(f"{prefix}_indeterminated",
        when((col(col_to_apply) == "indeterminado") & (col("week_of_month").isin([4])), 1))
    result = result.withColumn(f"{prefix}_indeterminated", coalesce(col(f"{prefix}_indeterminated"), col("fill_null")))
    #
    result = result.withColumn(f"{prefix}_weekly",
        when((col(col_to_apply) == "semanal") & (col("week_of_month").isin([4])), 1))
    result = result.withColumn(f"{prefix}_weekly", coalesce(col(f"{prefix}_weekly"), col("fill_null")))
    #
    result = result.withColumn(f"{prefix}_fortnightly",
        when((col(col_to_apply) == "quincenal") & (col("week_of_month").isin([4])), 1))
    result = result.withColumn(f"{prefix}_fortnightly", coalesce(col(f"{prefix}_fortnightly"), col("fill_null")))
    #
    result = result.withColumn(f"{prefix}_monthly",
        when((col(col_to_apply) == "mensual") & (col("week_of_month").isin([4])), 1))
    result = result.withColumn(f"{prefix}_monthly", coalesce(col(f"{prefix}_monthly"), col("fill_null")))
    #
    result = result.drop("fill_null")
    return result


def unusual_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """Marca 1 si el patrón es inusual, 0 si es regular en el mes.
    Unusual es cuando:
        - count_paid_in_month -> 3
        - count_paid_in_month -> 2
            - No (1,3) o (2,4) están en weeks_of_month
    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.

    Returns:
        DataFrame: DataFrame con las nuevas columnas que indican el tipo de pago por semana.
    """
    # corregir prefijo
    if prefix.endswith("_"):
        prefix = prefix[:-1]
    #
    result = df
    result = result.withColumn("fill_null",  when(col("week_of_month").isin([1, 2, 3]), 9).otherwise(0))
    result = result.withColumn(prefix,
        when((col("count_paid_in_month") == 3)  & (col("week_of_month").isin([4])), 1)
        .when((col("count_paid_in_month") == 2) & (col("week_of_month").isin([4])),
            when(~((array_contains(col("weeks_of_month"), 1) & array_contains(col("weeks_of_month"), 3))
                | (array_contains(col("weeks_of_month"), 2) & array_contains(col("weeks_of_month"), 4)))
                , 1)))
    result = result.withColumn(prefix, coalesce(col(prefix), col("fill_null")))
    result = result.drop("fill_null")
    return result


def get_when_otherwise_nested(conditions: List[Column], true_operations: List[Column]) -> "Column":
    if len(conditions) == 1:
        return when(conditions[0], true_operations[0])
    else:
        return when(conditions[0], true_operations[0]).otherwise(get_when_otherwise_nested(conditions[1:], true_operations[1:]))


def reverse_pattern_rule(
    df: DataFrame,
    columns_to_apply: List[str],
    prefix:str,
    remove_previous_prefix:bool,
    previous_prefix:Optional[str],
    vintage_date_column: Optional[str]="vintage_date",
    n_tef_column: Optional[str]="n_tef",
    numcliente_column: Optional[str]="numcliente"
) -> DataFrame:
    """
    Cambia el orden del patron, de tal forma que el patron de la ultima semana sea
    el patron de la primera, el de la segunda sea el de la penultima, etc.

    Input:
    +------------------+----------+-----+----+----+----+
    |vintage_date|numcliente|tfrom|col1|col2|col3|
    +------------------+----------+-----+----+----+----+
    |  2023-01-01|     1|  0| 10| 20| 30|
    |  2023-01-01|     1|  1| 15| 25| 35|
    |  2023-01-01|     1|  2| 12| 22| 32|
    |  2023-01-01|     1|  3| 18| 28| 38|
    |  2023-01-01|     1|  4| 11| 21| 31|
    |  2023-01-01|     1|  5| 16| 26| 36|
    |  2023-01-01|     1|  6| 13| 23| 33|
    |  2023-01-01|     1|  7| 19| 29| 39|
    |  2023-01-01|     1|  8| 14| 24| 34|
    |  2023-01-01|     1|  9| 17| 27| 37|
    |  2023-01-01|     1| 10| 10| 20| 30|
    |  2023-01-01|     1| 11| 15| 25| 35|
    +------------------+----------+-----+----+----+----+

    Output:
    +------------------+----------+-----+----+----+----+
    |vintage_date|numcliente|tfrom|col1|col2|col3|
    +------------------+----------+-----+----+----+----+
    |  2023-01-01|     1|  0| 15| 25| 35|
    |  2023-01-01|     1|  1| 10| 20| 30|
    |  2023-01-01|     1|  2| 17| 27| 37|
    |  2023-01-01|     1|  3| 14| 24| 34|
    |  2023-01-01|     1|  4| 19| 29| 39|
    |  2023-01-01|     1|  5| 13| 23| 33|
    |  2023-01-01|     1|  6| 16| 26| 36|
    |  2023-01-01|     1|  7| 11| 21| 31|
    |  2023-01-01|     1|  8| 18| 28| 38|
    |  2023-01-01|     1|  9| 12| 22| 32|
    |  2023-01-01|     1| 10| 15| 25| 35|
    |  2023-01-01|     1| 11| 10| 20| 30|
    +------------------+----------+-----+----+----+----+

    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.
        vintage_date_column (Optional[str], optional): Nombre de la columna que contiene las fechas de vintage. Por defecto es
            "vintage_date".
        numcliente_column (Optional[str], optional): Nombre de la columna que contiene los números de cliente. Por defecto es
            "numcliente".

    Returns:
        DataFrame: DataFrame con el patrón de las columnas especificadas invertido.
    """
    if vintage_date_column is None:
        vintage_date_column = "vintage_date"
    if n_tef_column is None:
        n_tef_column = "n_tef"
    if numcliente_column is None:
        numcliente_column = "numcliente"
    # cols to reverse
    # Tfrom max
    windowSpec = Window.orderBy(vintage_date_column, n_tef_column, numcliente_column, "int_tfrom").partitionBy(vintage_date_column,
    n_tef_column, numcliente_column)
    rowNumberWindowSpec = Window.orderBy("row_number").partitionBy(vintage_date_column, n_tef_column, numcliente_column)
    tfrom_max = int(df.select("tfrom").groupBy().max().collect()[0][0])
    lags = [-tfrom_max + 2*(i) for i in range(tfrom_max+1)]
    lag_conditions = [(col("lag") == lag_) for lag_ in lags]
    #
    def true_operations(column: str) -> "list[Column]":
        return [lag(col(column), lag_).over(rowNumberWindowSpec) for lag_ in lags]
    #
    def lag_operator(column: str) -> "Column":
        return get_when_otherwise_nested(lag_conditions, true_operations(column))
    #
    # Añadir la columna de numeración
    enumerated = df.withColumn("int_tfrom", col("tfrom").cast("int"))
    enumerated = enumerated.withColumn("row_number", row_number().over(windowSpec))
    enumerated = enumerated.withColumn("lag", -lit(tfrom_max) + 2*(col("int_tfrom")))
    #
    select_columns = [col("*")]
    for column in columns_to_apply:
        if not remove_previous_prefix:
            new_column = f"{prefix}_{column}"
        else:
            new_column = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        select_columns.append(lag_operator(column).alias(new_column))
    #
    result = enumerated.select(*select_columns)
    result = result.drop("row_number", "lag", "int_tfrom")
    return result


def inverse_pattern_rule(df: DataFrame, columns_to_apply: List[str], prefix:str, remove_previous_prefix:bool,
    previous_prefix:Optional[str]) -> DataFrame:
    """Cambia 1s por 0s y viceversa en las columnas especificadas.

    Args:
        df (DataFrame): DataFrame de entrada que contiene los datos.
        columns_to_apply (List[str]): Lista de nombres de columnas a las que se aplicará la regla.
        prefix (str): Prefijo que se añadirá a las nuevas columnas.
        remove_previous_prefix (bool): Indica si se debe eliminar el prefijo anterior de las columnas.
        previous_prefix (Optional[str]): Prefijo anterior que se debe eliminar si remove_previous_prefix es True.

    Returns:
        DataFrame: DataFrame con las columnas invertidas (1s por 0s y viceversa).
    """
    select_columns = [col("*")]
    for column in columns_to_apply:
        if not remove_previous_prefix:
            output_col = f"{prefix}_{column}"
        else:
            output_col = f"{prefix}_{column.replace(f'{previous_prefix}_', '')}"
        transformed_col = when(col(column) == 1, 0).otherwise(when(col(column) == 0, 1).otherwise(col(column)))
        select_columns.append(transformed_col.alias(output_col))
    #
    result = df.select(*select_columns)
    return result


def fill_and_select_latest(df: DataFrame, id_cols: Union[str, List[str]], date_col: str, fill_cols: list) -> DataFrame:
    if isinstance(id_cols, str):
        id_cols = [id_cols]
    #

    window_spec = Window.partitionBy(*id_cols).orderBy(col(date_col).asc())

    # Llenar los valores nulos con el valor del mes anterior para cada columna especificada
    for fill_col in fill_cols:
        df = df.withColumn(fill_col, last(fill_col, True).over(window_spec))

    # Seleccionar el registro más reciente por cliente
    window_spec_desc = Window.partitionBy(*id_cols).orderBy(col(date_col).desc())
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec_desc))
    df_filtered = df_with_row_num.filter(col("row_num") == 1)

    # Seleccionar las columnas necesarias
    result_df = df_filtered.select(*(id_cols +fill_cols))
    return result_df
