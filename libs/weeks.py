"""
Project: Tfrom Week ReTagger
Developer: @rg49392
Update Date: 2025-02-19
Version: 0.1

Changes tfrom column to weeks, in order to use DataPull as Weeks
"""

# Libraries
from collections import namedtuple
from datetime import date, datetime, timedelta
from pathlib import PurePosixPath as PosixPath
from typing import Dict, List, Optional, Tuple, Type, Union

from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, dayofweek, datediff, expr, floor, last_day, lit, max as spark_max,
    min as spark_min, month, to_date, trunc, weekofyear, when, year
)

from .aggregattes.payroll_income import cached_property
from .steps import dynamic, path_delete, save_parquet

# Functions

# Definir la Named Tuple
YearWeek = namedtuple('YearWeek', ['year', 'week_of_year'])

def date_to_week_of_year_pyspark_consistent(date:date) -> int:
    """Converts a date to the corresponding week of the year.

    Args:
        date (date): The date to convert.

    Returns:
        int: The week of the year.
    """
    return date.isocalendar()[1]


def date_to_yearweek(date:date) -> YearWeek:
    """_summary_

    Args:
        date (date): _description_

    Returns:
        YearWeek: _description_
    """
    return YearWeek(date.year, date_to_week_of_year_pyspark_consistent(date))


def yearweek_to_date(yearweek: YearWeek) -> date:
    """Converts a YearWeek object to a date (first day of the week, Monday).

    Args:
        yearweek (YearWeek): The YearWeek object to convert.

    Returns:
        date: The corresponding date.
    """
    # Convertir el año y la semana del año en una fecha (primer día de la semana, lunes)
    first_day_of_week = datetime.strptime(f'{yearweek.year}-W{int(yearweek.week_of_year)}-1', "%G-W%V-%u").date()
    return first_day_of_week


def add_weeks_to_yearweek(yearweek: YearWeek, weeks: int) -> YearWeek:
    """Adds a number of weeks to a YearWeek object.

    Args:
        yearweek (YearWeek): The original YearWeek object.
        weeks (int): The number of weeks to add.

    Returns:
        YearWeek: The new YearWeek object after adding the weeks.
    """
    # Calcular el primer día de la semana original (lunes)
    first_day_of_week = datetime.strptime(f'{yearweek.year}-W{int(yearweek.week_of_year)}-1', "%G-W%V-%u").date()
    # Sumar o restar las semanas
    new_date = first_day_of_week + timedelta(weeks=weeks)
    # Convertir la nueva fecha a YearWeek
    return date_to_yearweek(new_date)


def is_yearweek_complete_in_month(yearweek: YearWeek) -> bool:
    """Checks if a YearWeek is completely within a single month.

    Args:
        yearweek (YearWeek): The YearWeek object to check.

    Returns:
        bool: True if the YearWeek is completely within a single month, False otherwise.
    """
    # Calcular el primer día de la semana (lunes)
    first_day_of_week = datetime.strptime(f'{yearweek.year}-W{int(yearweek.week_of_year) - 1}-1', "%Y-W%U-%w").date()
    # Calcular el último día de la semana (domingo)
    last_day_of_week = first_day_of_week + timedelta(days=6)
    # Obtener el mes del primer día de la semana
    month = first_day_of_week.month
    # Verificar si ambos días están dentro del mismo mes
    return first_day_of_week.month == month and last_day_of_week.month == month


def change_date_to_first_monday_of_the_month_column(df: DataFrame, date_column: str, day_of_week_column:str) -> DataFrame:
    """Changes the date column to the first 1 of the day_of_week_column in the first week of the month.

    Args:
        df (DataFrame): The DataFrame containing the date column.
        date_column (str): The name of the date column.
        day_of_week_column (str): The name of the day of the week column.

    Returns:
        DataFrame: The DataFrame with the date column changed to the first Monday of the month.
    """
    # Asegurarse de que la columna de fecha esté en el formato de fecha
    result = df.withColumn("date_converted", to_date(col(date_column), "yyyy-MM-dd"))
    result = result.withColumn("start_date_of_month", trunc(col("date_converted"), "MM"))
    result = WeekOfMonth.day_of_week_starting_in_monday(output="start_day_of_week_of_month", df=result,
    column="start_date_of_month")
    result = WeekOfMonth.start_date_of_week(
        output="start_date_of_first_week_of_the_month",
        df=result,
        column="start_date_of_month",
        day_of_week_column="start_day_of_week_of_month"
    )
    result = result.withColumn("first_monday", when(
        month(col("start_date_of_first_week_of_the_month")) != month(col("start_date_of_month")), col("start_date_of_month")
    ).otherwise(col("start_date_of_first_week_of_the_month"))
    )
    # Eliminar las columnas extra
    result = result.drop("start_date_of_month", "start_day_of_week_of_month", "date_converted",
    "start_date_of_first_week_of_the_month")
    #
    return result


def change_date_first_monday_of_the_week_column(df: DataFrame, date_column: str, day_of_week_column:str) -> DataFrame:
    """Changes the date column to the first 1 of day_of_week_column

    Args:
        df (DataFrame): The DataFrame containing the date column.
        date_column (str): The name of the date column.
        day_of_week_column (str): The name of the day of the week column.

    Returns:
        DataFrame: The DataFrame with the date column changed to the first Monday of the week.
    """
    # Asegurarse de que la columna de fecha esté en el formato de fecha
    result = df.withColumn("date_converted", to_date(col(date_column), "yyyy-MM-dd"))
    # Calcular el primer lunes de la semana
    result = WeekOfMonth.start_date_of_week( output="first_monday", df=result, column="date_converted",
    day_of_week_column=day_of_week_column)
    # Eliminar las columnas extra
    result = result.drop("date_converted")
    return result


def weeks_difference(
    df: DataFrame,
    start_date_col:str,
    start_week_of_year_col:str,
    end_date_col:str,
    end_week_of_year_col:str,
    weeks_per_year:Optional[int] = 52
) -> DataFrame:
    """Calculates the difference in weeks between two years and weeks of the year.

    Args:
        df (DataFrame): The DataFrame containing the date columns.
        start_date_col (str): The name of the start date column.
        start_week_of_year_col (str): The name of the start week of the year column.
        end_date_col (str): The name of the end date column.
        end_week_of_year_col (str): The name of the end week of the year column.
        weeks_per_year (Optional[int], optional): The number of weeks per year. Defaults to 52.

    Returns:
        DataFrame: The DataFrame with the calculated weeks difference.
    """
    if weeks_per_year is None:
        weeks_per_year = 52

    # Asegurarse de que las columnas de fecha estén en el formato de fecha
    df = df.withColumn(start_date_col, to_date(col(start_date_col), "yyyy-MM-dd"))
    df = df.withColumn(end_date_col, to_date(col(end_date_col), "yyyy-MM-dd"))

    # Calcular el año y la semana del año para ambas fechas
    df = df.withColumn("start_year", year(col(start_date_col)))
    df = df.withColumn("start_week", col(start_week_of_year_col))
    df = df.withColumn("end_year", year(col(end_date_col)))
    df = df.withColumn("end_week", col(end_week_of_year_col))

    # Calcular la diferencia en semanas
    df = df.withColumn("weeks_diff",
        when((col("start_year") == col("end_year")) & (col("start_week") == col("end_week")), 0)
        .otherwise((col("end_year") - col("start_year")) * weeks_per_year + (col("end_week") - col("start_week"))))
    # Eliminar las columnas extra
    df = df.drop("start_year", "start_week", "end_year", "end_week")
    return df


# Clases


class WeekOfMonth:
    """
    Calculates the new week of the month,
    forcing it to always be 4 weeks per month.
    """
    new_week_of_year_column = "new_week_of_year"
    new_day_of_week_column = "new_day_of_week"
    weeks_per_month = 4
    def __init__(self,
        df: DataFrame,
        date_column:str
    ) -> None:
        """_summary_

        Args:
            df (DataFrame): _description_
            date_column (str): _description_
        """
        self.df = df
        self.date_column = date_column
    #
    @cached_property
    def weeks_and_dates(self) -> DataFrame:
        """
        Adds various date-related columns to the DataFrame.

        This function performs the following operations:
        1. Adds columns for the start and end dates of the month.
        2. Adds columns for the day of the week for the current date, start date of the month, and end date of the month.
        3. Adds columns for the start and end dates of the current week, first week of the month, and last week of the month.
        4. Adds a column for the current week of the year.
        5. Adds columns for the week of the month for the current date, start date of the month, and end date of the month.

        Columns added:
        - start_date_of_month
        - end_date_of_month
        - current_day_of_week
        - start_day_of_week_of_month
        - end_day_of_week_of_month
        - start_date_of_current_week
        - end_date_of_current_week
        - start_date_of_first_week_of_the_month
        - end_date_of_first_week_of_the_month
        - start_date_of_last_week_of_the_month
        - end_date_of_last_week_of_the_month
        - current_week_of_year
        - current_week_of_month
        - start_week_of_month
        - end_week_of_month

        Returns:
            DataFrame: The DataFrame with the new date-related columns.
        """
        # Add date columns
        result = self.df.withColumn("start_date_of_month", trunc(col(self.date_column), "MM"))
        result = result.withColumn("end_date_of_month", expr("last_day({})".format(self.date_column)))
        # Add day of week columns
        result = self.day_of_week_starting_in_monday(output="current_day_of_week", df=result, column=self.date_column)
        result = self.day_of_week_starting_in_monday(output="start_day_of_week_of_month", df=result, column="start_date_of_month")
        result = self.day_of_week_starting_in_monday(output="end_day_of_week_of_month", df=result, column="end_date_of_month")
        # Add start date of week columns
        result = self.start_date_of_week(output="start_date_of_current_week", df=result, column=self.date_column,
        day_of_week_column="current_day_of_week")
        result = self.end_date_of_week(output="end_date_of_current_week", df=result, column=self.date_column,
        day_of_week_column="current_day_of_week")
        result = self.start_date_of_week(output="start_date_of_first_week_of_the_month", df=result, column="start_date_of_month",
        day_of_week_column="start_day_of_week_of_month")
        result = self.end_date_of_week(output="end_date_of_first_week_of_the_month", df=result, column="start_date_of_month",
        day_of_week_column="start_day_of_week_of_month")
        result = self.start_date_of_week(output="start_date_of_last_week_of_the_month", df=result, column="end_date_of_month",
        day_of_week_column="end_day_of_week_of_month")
        result = self.end_date_of_week(output="end_date_of_last_week_of_the_month", df=result, column="end_date_of_month",
        day_of_week_column="end_day_of_week_of_month")
        # Add week of year columns
        result = result.withColumn("current_week_of_year", weekofyear(col(self.date_column)))
        # Add week of month columns
        result = self.week_of_month(output="current_week_of_month", df=result, column=self.date_column,
        start_date_of_month_column="start_date_of_month", start_date_of_week_column="start_date_of_current_week",
        start_date_of_week_of_start_of_month="start_date_of_first_week_of_the_month")
        result = self.week_of_month(output="start_week_of_month", df=result, column="start_date_of_month",
        start_date_of_month_column="start_date_of_month", start_date_of_week_column="start_date_of_first_week_of_the_month",
        start_date_of_week_of_start_of_month="start_date_of_first_week_of_the_month")
        result = self.week_of_month(output="end_week_of_month", df=result, column="end_date_of_month",
        start_date_of_month_column="start_date_of_month", start_date_of_week_column="start_date_of_last_week_of_the_month",
        start_date_of_week_of_start_of_month="start_date_of_first_week_of_the_month")
        return result
    #
    @cached_property
    def conditions(self) -> DataFrame:
        """
        Calculates various conditions related to weeks and dates.

        This function performs the following operations:
        1. Adds columns for the start and end dates of the month, and other date-related columns.
        2. Adds columns to determine if the current, start, and end weeks of the month are complete.
        3. Adds columns to handle appending incomplete weeks to adjacent weeks.
        4. Calculates the final week of the month after handling incomplete weeks.

        Returns:
            DataFrame: The DataFrame with the new condition columns.
        """
        result = self.weeks_and_dates
        # Add is_week_complete columns
        result = self._is_week_complete(
            output_column_name="is_current_week_complete",
            df=result,
            week_to_evaluate_column="current_week_of_month",
            start_date_of_week_to_evaluate_column="start_date_of_current_week",
            end_date_of_week_to_evaluate_column="end_date_of_current_week",
            start_date_of_month_column="start_date_of_month",
            end_date_of_month_column="end_date_of_month",
            end_week_of_month_column="end_week_of_month"
        )
        result = self._is_week_complete(
            output_column_name="is_start_week_complete",
            df=result,
            week_to_evaluate_column="start_week_of_month",
            start_date_of_week_to_evaluate_column="start_date_of_first_week_of_the_month",
            end_date_of_week_to_evaluate_column="end_date_of_first_week_of_the_month",
            start_date_of_month_column="start_date_of_month",
            end_date_of_month_column="end_date_of_month",
            end_week_of_month_column="end_week_of_month"
        )
        # add condition columns
        result = result.withColumn("week_length_before_appends", col("end_week_of_month"))
        return result
    #
    @cached_property
    def new_week_of_year_day_of_month(self) -> DataFrame:
        """
        Calculates the new week of the month after handling incomplete weeks.

        This function performs the following operations:
        1. Retrieves the DataFrame with various conditions related to weeks and dates.
        2. Selects the relevant columns including the date column and the new week of the month column.

        Returns:
            DataFrame: The DataFrame with the date column and the new week of the month column.
        """
        # Final week of month and year of month
        result = self._new_week_of_month(
            output_column="new_week_of_month",
            df=self.conditions,
            old_week_of_month_column="current_week_of_month",
            is_start_week_of_month_appended_to_next_week_column="is_start_week_of_month_appended_to_next_week",
            is_last_week_of_month_appended_to_previous_week_column="is_end_week_of_month_appended_to_previous_week"
        )
        result = self._new_week_of_year(
            output_column=self.new_week_of_year_column,
            df=result,
            new_week_of_month_column="new_week_of_month"
        )
        result = self._new_day_of_week(
            output_column=self.new_day_of_week_column,
            df=result,
            old_week_of_month_column="current_week_of_month",
            is_start_week_of_month_appended_to_next_week_column="is_start_week_of_month_appended_to_next_week",
            is_last_week_of_month_appended_to_previous_week_column="is_end_week_of_month_appended_to_previous_week",
            current_day_of_week="current_day_of_week",
            start_day_of_week_of_month="start_day_of_week_of_month"
        )
        return result
    #
    @staticmethod
    def day_of_week_starting_in_monday(df: DataFrame, column:str, output:str) -> DataFrame:
        """
        Adds a column with the day of the week starting from Monday.

        Args:
            df (DataFrame): The input DataFrame.
            column (str): The name of the column containing the date.
            output (str): The name of the output column to store the day of the week starting from Monday.

        Returns:
            DataFrame: The DataFrame with the new column containing the day of the week starting from Monday.
        """
        result = df.withColumn("pyspark_day_of_week", dayofweek(col(column)))
        result = result.withColumn(output, when(col("pyspark_day_of_week") == 1, 7).otherwise(col("pyspark_day_of_week") - 1))
        result = result.drop("pyspark_day_of_week")
        return result
    #
    @staticmethod
    def start_date_of_week(df: DataFrame, column: str, day_of_week_column: str, output: str) -> DataFrame:
        """
        Adds a column with the start date of the week for each date in the specified column.
        The day_of_week_column must be calculated first using the function day_of_week_starting_in_monday.

        Args:
            df (DataFrame): The input DataFrame.
            column (str): The name of the column containing the date.
            day_of_week_column (str): The name of the column containing the day of the week starting from Monday.
            output (str): The name of the output column to store the start date of the week.

        Returns:
            DataFrame: The DataFrame with the new column containing the start date of the week.
        """
        result = df.withColumn(
            output, when(col(day_of_week_column) == 1, col(column)).otherwise(expr(f"date_sub({column}, {day_of_week_column} - 1)"))
        )
        return result
    #
    @staticmethod
    def end_date_of_week(df: DataFrame, column: str, day_of_week_column: str, output: str) -> DataFrame:
        """
        Adds a column with the end date of the week for each date in the specified column.
        The day_of_week_column must be calculated first using the function day_of_week_starting_in_monday.

        Args:
            df (DataFrame): The input DataFrame.
            column (str): The name of the column containing the date.
            day_of_week_column (str): The name of the column containing the day of the week starting from Monday.
            output (str): The name of the output column to store the end date of the week.

        Returns:
            DataFrame: The DataFrame with the new column containing the end date of the week.
        """
        result = df.withColumn(
            output, when(col(day_of_week_column) == 7, col(column)).otherwise(expr(f"date_add({column}, 7 - {day_of_week_column})"))
        )
        return result
    #
    @staticmethod
    def week_of_month(df: DataFrame, column: str, start_date_of_month_column:str, start_date_of_week_column:str,
    start_date_of_week_of_start_of_month:str, output: str) -> DataFrame:
        """
        Adds a column with the week of the month for each date in the specified column.
        Utilizes the start_date_of_week_column which corresponds to the start of the week (day=1).
        Then calculates the difference in days between start_date_of_month_column and start_date_of_week_column,
        divides by 7, and rounds to get the week number.

        Args:
            df (DataFrame): The input DataFrame.
            column (str): The name of the column containing the date.
            start_date_of_month_column (str): The name of the column containing the start date of the month.
            start_date_of_week_column (str): The name of the column containing the start date of the week.
            start_date_of_week_of_start_of_month (str): The name of the column containing the start date of the week of the start
            of the month.
            output (str): The name of the output column to store the week of the month.

        Returns:
            DataFrame: The DataFrame with the new column containing the week of the month.
        """
        result = df.withColumn(
            output,
            # caso cuando el inicio del mes es igual al inicio de la semana
            when(col(start_date_of_month_column) == col(start_date_of_week_of_start_of_month),
            floor(datediff(col(start_date_of_week_column), col(start_date_of_month_column)) / 7) + 1)
            .otherwise(
                floor(datediff(col(start_date_of_week_column), col(start_date_of_month_column)) / 7) + 2
            )
        )
        return result

    def _is_week_complete(
        self,
        df: DataFrame,
        week_to_evaluate_column:str,
        start_date_of_week_to_evaluate_column:str,
        end_date_of_week_to_evaluate_column:str,
        start_date_of_month_column:str,
        end_week_of_month_column:str,
        end_date_of_month_column:str,
        output_column_name:str
    ) -> DataFrame:
        """
        Adds a column 'is_week_complete' to the DataFrame indicating if the week is complete within the month.

        Args:
            df (DataFrame): The input DataFrame.
            week_to_evaluate_column (str): The name of the column containing the week to evaluate.
            start_date_of_week_to_evaluate_column (str): The name of the column containing the start date of the week to evaluate.
            end_date_of_week_to_evaluate_column (str): The name of the column containing the end date of the week to evaluate.
            start_date_of_month_column (str): The name of the column containing the start date of the month.
            end_week_of_month_column (str): The name of the column containing the end week of the month.
            end_date_of_month_column (str): The name of the column containing the end date of the month.
            output_column_name (str): The name of the output column to store if the week is complete.

        Returns:
            DataFrame: The DataFrame with the new column indicating if the week is complete.
        """
        # Primera parte del mes
        result = df.withColumn(
            output_column_name,
            # Caso inicio del mes
            when((col(week_to_evaluate_column) == 1),
                # Caso en el que el día de inicio de la semana del mes no es 1
                when(col(start_date_of_week_to_evaluate_column) < col(start_date_of_month_column), False)
                .otherwise(True)
            )
            # caso final del mes
            .otherwise(when(col(week_to_evaluate_column) == col(end_week_of_month_column),
                # Caso en el que el día de fin de la semana del mes no es 7
                when(col(end_date_of_week_to_evaluate_column) > col(end_date_of_month_column), False)
                .otherwise(True)
            )
            # Caso en medio del mes
            .otherwise(True)
        ))
        return result
    #
    def _new_week_of_month(self,
        df: DataFrame,
        old_week_of_month_column:str,
        is_start_week_of_month_appended_to_next_week_column:str,
        is_last_week_of_month_appended_to_previous_week_column:str,
        output_column:str) -> None:
        """
        Main function of the class, calculates the new week of the month,
        forcing it to always be 4 weeks per month. To do this,
        it evaluates if the first and last weeks of the month are sufficiently
        complete. If they are not, it merges them with the second and penultimate
        weeks respectively.

        Args:
            df (DataFrame): The input DataFrame.
            old_week_of_month_column (str): The name of the column containing the old week of the month.
            is_start_week_of_month_appended_to_next_week_column (str): The name of the column indicating if the start week of the
            month is appended to the next week.
            is_last_week_of_month_appended_to_previous_week_column (str): The name of the column indicating if the last week of the
            month is appended to the previous week.
            output_column (str): The name of the output column to store the new week of the month.

        Returns:
            DataFrame: The DataFrame with the new column indicating the new week of the month.
        """
        raise NotImplementedError

    def _new_week_of_year(
        self,
        df: DataFrame,
        new_week_of_month_column:str,
        output_column:str
    ) -> DataFrame:
        """_summary_

        Args:
            df (DataFrame): _description_
            new_week_of_month_column (str): _description_
            output_column (str): _description_

        Returns:
            DataFrame: _description_
        """
        result = df
        # add months column
        result = result.withColumn("month", month(col(self.date_column)))
        # Calculate week_of_year
        result = result.withColumn(output_column, (col("month")-1)*self.weeks_per_month + col(new_week_of_month_column))
        result = result.drop("month")
        return result
    #
    def _new_day_of_week(self,
        df: DataFrame,
        old_week_of_month_column:str,
        is_start_week_of_month_appended_to_next_week_column:str,
        is_last_week_of_month_appended_to_previous_week_column:str,
        current_day_of_week:str,
        start_day_of_week_of_month:str,
        output_column:str
    ) -> None:
        """TODO
        Usar las columnas:
        - is_start_week_of_month_appended_to_next_week_column
        - is_last_week_of_month_appended_to_previous_week_column
        Basarse en _new_week_of_month
        - Los meses de enmedio no cambian
        - Las primeras y últimas semanas pueden unirse con la semana anterior o siguiente
            - Cuando la ultima semana es larga, a los días añadidos se le saca el día normal de la semana y se le suma 7.
            - Caundo la primera semana se queda.
                - Diferencia en dias: se utiliza el ultimo día de la semana  y se compara con el primer día del mes
                - Se resta esta diferencia para que ahora el primer día del mes sea el día 1.
            - Cuando la primer semana se pega a la siguiente.
                - Se numera la primer semana como en el caso anterior.
                - A la segunda semana se le añade la diferencia en días positiva.
        """
        raise NotImplementedError

    def tag_weeks(self) -> DataFrame:
        """
        Calculates the new week of the month after handling incomplete weeks.

        This function performs the following operations:
        1. Retrieves the DataFrame with various conditions related to weeks and dates.
        2. Selects the relevant columns including the date column and the new week of the month column.

        Returns:
            DataFrame: The DataFrame with the date column and the new week of the month column.
        """
        result = self.new_week_of_year_day_of_month
        return result.select(self.date_column, self.new_week_of_year_column, self.new_day_of_week_column)
    #

class WeekOfMonthFourWeeksForced(WeekOfMonth):
    """
    Calculates the new week of the month,
    forcing it to always be 4 weeks per month. To do this,
    it evaluates if the first and last weeks of the month are sufficiently
    complete. If they are not, it merges them with the second and penultimate
    weeks respectively.
    """
    weeks_per_month = 4

    def __init__(self,
        df: DataFrame,
        date_column:str
    ) -> None:
        """_summary_

        Args:
            df (DataFrame): _description_
            date_column (str): _description_
        """
        super().__init__(df, date_column)
    #
    @cached_property
    def conditions(self) -> DataFrame:
        """
        This function performs the following operations:
        1. Adds columns for the start and end dates of the month, and other date-related columns.
        2. Adds columns to determine if the current, start, and end weeks of the month are complete.
        3. Adds columns to handle appending incomplete weeks to adjacent weeks.
        4. Calculates the final week of the month after handling incomplete weeks.

        Returns:
            DataFrame: The DataFrame with the new condition columns.
        """
        result = super().conditions
        # First append, start of month.
        result = self._is_start_week_of_month_appended_to_next_week(
            output_column_name="is_start_week_of_month_appended_to_next_week",
            df=result, week_column_to_evaluate="start_week_of_month",
            week_length_before_appends="week_length_before_appends",
            is_week_complete_column="is_start_week_complete",
            start_day_of_week_of_month="start_day_of_week_of_month"
        )
        result = result.withColumn("week_length_of_month_after_first_append", col("week_length_before_appends") + when(col
        ("is_start_week_of_month_appended_to_next_week") == True, -1).otherwise(0))
        # Second append, end of month
        result = self._is_last_week_of_month_appended_to_previous_week(
            output_column_name="is_end_week_of_month_appended_to_previous_week",
            df=result ,
            week_column_to_evaluate="end_week_of_month",
            week_length_of_month_after_first_append="week_length_of_month_after_first_append",
            end_week_of_month_column="end_week_of_month"
        )
        result = result.withColumn("week_length_of_month_after_second_append", col("week_length_of_month_after_first_append") + when
        (col("is_end_week_of_month_appended_to_previous_week") == True, -1).otherwise(0))
        return result

    def _is_start_week_of_month_appended_to_next_week(self, df: DataFrame, week_column_to_evaluate:str,
    week_length_before_appends:str, is_week_complete_column:str, start_day_of_week_of_month:str, output_column_name:str) -> DataFrame:
        """
        Adds a new column 'is_date_appended_to_next_week' indicating if the start week of the month is appended to the
        previous week.
        This condition is used to determine if the last week of the month is appended to the previous week.
        The condition evaluates if the first week starts in the first 3 days of the month.

        Args:
            df (DataFrame): The input DataFrame.
            week_column_to_evaluate (str): The name of the column containing the week to evaluate.
            week_length_before_appends (str): The name of the column containing the week length before appends.
            is_week_complete_column (str): The name of the column indicating if the week is complete.
            start_day_of_week_of_month (str): The name of the column containing the start day of the week of the month.
            output_column_name (str): The name of the output column to store if the start week of the month is appended to the
            next week.

        Returns:
            DataFrame: The DataFrame with the new column indicating if the start week of the month is appended to the next week.
        """
        # Add new_day_of_year_modifier column
        df = df.withColumn(
            output_column_name,
            when((col(is_week_complete_column) == True) | (col(week_length_before_appends)==self.weeks_per_month), False)
            .when((col(week_column_to_evaluate) == 1) & (col(is_week_complete_column) == False) & (col(start_day_of_week_of_month).
            isin([1, 2, 3])), False)
            .otherwise(True)
        )
        return df

    def _is_last_week_of_month_appended_to_previous_week(self, df: DataFrame, week_column_to_evaluate:str,
    week_length_of_month_after_first_append:str,  end_week_of_month_column:str, output_column_name:str) -> DataFrame:
        """
        Adds a new column 'is_last_week_appended_to_previous_week' indicating if the last week of the month is appended to
        the previous week.
        This condition is used to determine if the last week of the month is appended to the previous week.

        Args:
            df (DataFrame): The input DataFrame.
            week_column_to_evaluate (str): The name of the column containing the week to evaluate.
            week_length_of_month_after_first_append (str): The name of the column containing the week length of the month after
            the first append.
            end_week_of_month_column (str): The name of the column containing the end week of the month.
            output_column_name (str): The name of the output column to store if the last week of the month is appended to the
            previous week.

        Returns:
            DataFrame: The DataFrame with the new column indicating if the last week of the month is appended to the previous
            week.
        """
        df = df.withColumn(output_column_name,
            # revisar si la columna es la final del mes
            when(col(week_column_to_evaluate) == col(end_week_of_month_column),
                # Revisar si la longitud del mes en semanasdespués de la primera adición es mayor a 4
                when(col(week_length_of_month_after_first_append) > lit(self.weeks_per_month), True)
                .otherwise(False)
            )
            .otherwise(False)
        )
        return df

    def _new_week_of_month(self,
        df: DataFrame,
        old_week_of_month_column:str,
        is_start_week_of_month_appended_to_next_week_column:str,
        is_last_week_of_month_appended_to_previous_week_column:str,
        output_column:str) -> DataFrame:
        """
        Main function of the class, calculates the new week of the month,
        forcing it to always be 4 weeks per month. To do this,
        it evaluates if the first and last weeks of the month are sufficiently
        complete. If they are not, it merges them with the second and penultimate
        weeks respectively.

        Args:
            df (DataFrame): The input DataFrame.
            old_week_of_month_column (str): The name of the column containing the old week of the month.
            is_start_week_of_month_appended_to_next_week_column (str): The name of the column indicating if the start week of
            the month is appended to the next week.
            is_last_week_of_month_appended_to_previous_week_column (str): The name of the column indicating if the last week of
            the month is appended to the previous week.
            output_column (str): The name of the output column to store the new week of the month.

        Returns:
            DataFrame: The DataFrame with the new column indicating the new week of the month.
        """
        #
        # asumiendo que las semanas requeridas son 4 y
        # Caso en que no cambian
        df = df.withColumn(output_column,
            when((col(is_start_week_of_month_appended_to_next_week_column) == False) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == False),
                # Modificacion para todas las columnas
                col(old_week_of_month_column)
            )
            # Caso en el que una de las semanas final o inicial se pega a la semana anterior o siguiente
            # Caso en el que la primer semana se pega a la siguiente
            .otherwise(when((col(is_start_week_of_month_appended_to_next_week_column) == True) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == False),
                # Modificacion para la primer semana
                when(col(old_week_of_month_column) == 1, 1)
                # Modificacion para las siguientes semanas
                .otherwise(col(old_week_of_month_column) -1 )
            )
            # Caso en que la ultima semana se pega a la anterior
            .otherwise(when((col(is_start_week_of_month_appended_to_next_week_column) == False) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == True),
                # Modificacion para la ultima semanas
                when(col(old_week_of_month_column) == self.weeks_per_month + 1, col(old_week_of_month_column) -1)
                # Modificacion para las otras semanas
                .otherwise(col(old_week_of_month_column))
            )
            # Caso en ambas, final e inicial se peguen a la semana anterior o siguiente
            .otherwise(
                # Modificacion para la ultima semanas
                when(col(old_week_of_month_column) == self.weeks_per_month + 2, col(old_week_of_month_column) -2)
                # Modificacion para la primer semanas
                .otherwise(when(col(old_week_of_month_column) == 1, 1)
                # Modificacion para las semanas de en medio
                .otherwise(col(old_week_of_month_column) -1)
        )))))
        return df

    def _new_day_of_week(self,
        df: DataFrame,
        old_week_of_month_column:str,
        is_start_week_of_month_appended_to_next_week_column:str,
        is_last_week_of_month_appended_to_previous_week_column:str,
        current_day_of_week:str,
        start_day_of_week_of_month:str,
        output_column:str
    ) -> DataFrame:
        """TODO
        Usar las columnas:
        - is_start_week_of_month_appended_to_next_week_column
        - is_last_week_of_month_appended_to_previous_week_column
        Basarse en _new_week_of_month
        - Los meses de enmedio no cambian, la semana sigue 1234567
        - Las primeras y últimas semanas dependen de si se pegan a la semana anterior o siguiente
            - Cuando la ultima semana es larga, a los días añadidos se le saca el día normal de la semana y se le suma 7.
            - Cuando la primera semana se queda.
                - Diferencia en dias: se utiliza el ultimo día de la semana  y se compara con el primer día del mes
                - Se resta esta diferencia para que ahora el primer día del mes sea el día 1.
            - Cuando la primer semana se pega a la siguiente.
                - Se numera la primer semana como en el caso anterior.
                - A la segunda semana se le añade la diferencia en días positiva.
        """
        # Caso en que no cambian
        result = df.withColumn(output_column,
            when((col(is_start_week_of_month_appended_to_next_week_column) == False) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == False),
                # Modificacion para todas las semanas
                col(current_day_of_week)
            )
            # Caso en el que una de las semanas final o inicial se pega a la semana anterior o siguiente
            # Caso en el que la primer semana se pega a la siguiente
            .otherwise(when((col(is_start_week_of_month_appended_to_next_week_column) == True) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == False),
                # Modificacion para la primer semana
                when(col(old_week_of_month_column) == 1, col(current_day_of_week) - col(start_day_of_week_of_month) + 1)
                # Modificacion para el siguiente dia de la semana
                .otherwise(when(col(old_week_of_month_column) == 2,  col(current_day_of_week) + 7 - col(start_day_of_week_of_month)
                + 1)
                # Modificacion para las siguientes semana
                .otherwise(col(current_day_of_week)))
            )
            # Caso en que la última semana se pega a la anterior
            .otherwise(when((col(is_start_week_of_month_appended_to_next_week_column) == False) & (col
            (is_last_week_of_month_appended_to_previous_week_column) == True),
                # Modificacion para el week de la semana
                when(col(old_week_of_month_column) == 1, col(current_day_of_week) - col(start_day_of_week_of_month) + 1)
                # Modificacion para la ultima semana
                .otherwise(when(col(old_week_of_month_column) == self.weeks_per_month + 1, col(current_day_of_week) + 7)
                # Modificacion para las otras semanas
                .otherwise(col(current_day_of_week)))
            )
            # Caso en ambas, final e inicial se peguen a la semana anterior o siguiente
            .otherwise(
                # Modificacion para la ultima semana
                when(col(old_week_of_month_column) == self.weeks_per_month + 2, col(current_day_of_week) + 7)
                # Modificacion para la primer semana
                .otherwise(when(col(old_week_of_month_column) == 1, col(current_day_of_week) - col(start_day_of_week_of_month) + 1)
                # Modificacion para la sgunda semana
                .otherwise(when(col(old_week_of_month_column) == 2,  col(current_day_of_week) + 7 - col(start_day_of_week_of_month)
                + 1)
                # Modificacion para las semanas de en medio
                .otherwise(col(current_day_of_week)))
            )
        ))))
        return result
    #

class WeekOfMonthPayroll(WeekOfMonth):
    """
    Calculates the new week of the month,
    forcing it to always be 4 weeks per month. To do this,
    uses a custom week definition for payroll weeks.

    | Pagos semanales | Rango de días   |
    |-----------------|-----------------|
    | Semana 1        | 0-7             |
    | Semana 2        | 8-15            |
    | Semana 3        | 16-21           |
    | Semana 4        | 22-31           |
    """
    weeks_per_month = 4
    def __init__(self,
        df: DataFrame,
        date_column:str
    ) -> None:
        """_summary_

        Args:
            df (DataFrame): _description_
            date_column (str): _description_
        """
        super().__init__(df, date_column)
    #
    @cached_property
    def conditions(self) -> DataFrame:
        """
        Calculates various conditions related to weeks and dates.

        This function performs the following operations:
        1. Adds columns for the start and end dates of the month, and other date-related columns.
        2. Adds columns to determine if the current, start, and end weeks of the month are complete.
        3. Adds columns to handle appending incomplete weeks to adjacent weeks.
        4. Calculates the final week of the month after handling incomplete weeks.

        Returns:
            DataFrame: The DataFrame with the new condition columns.
        """
        result = super().conditions
        #
        return result
    #
    @cached_property
    def new_week_of_year_day_of_month(self) -> DataFrame:
        """
        Calculates the new week of the month after handling incomplete weeks.

        This function performs the following operations:
        1. Retrieves the DataFrame with various conditions related to weeks and dates.
        2. Selects the relevant columns including the date column and the new week of the month column.

        Returns:
            DataFrame: The DataFrame with the date column and the new week of the month column.
        """
        # Final week of month and year of month
        result = self._new_week_of_month(
            output_column="new_week_of_month",
            df=self.conditions,
            current_date_column=self.date_column,
        )
        result = self._new_week_of_year(
            output_column=self.new_week_of_year_column,
            df=result,
            new_week_of_month_column="new_week_of_month"
        )
        result = self._new_day_of_week(
            output_column=self.new_day_of_week_column,
            df=result,
            current_date_column=self.date_column
        )
        return result
    def _new_week_of_month(self,
        df: DataFrame,
        current_date_column:str,
        output_column:str) -> DataFrame:
        """
        Main function of the class, calculates the new week of the month,
        forcing it to always be 4 weeks per month. To do this,
        it evaluates if the first and last weeks of the month are sufficiently
        complete. If they are not, it merges them with the second and penultimate
        weeks respectively.
        | Pagos semanales | Rango de días   |
        |-----------------|-----------------|
        | Semana 1        | 0-7             |
        | Semana 2        | 8-15            |
        | Semana 3        | 16-21           |
        | Semana 4        | 22-31           |
        Args:
            df (DataFrame): The input DataFrame.
            old_week_of_month_column (str): The name of the column containing the old week of the month.
            is_start_week_of_month_appended_to_next_week_column (str): The name of the column indicating if the start week of the
            month is appended to the next week.
            is_last_week_of_month_appended_to_previous_week_column (str): The name of the column indicating if the last week of the
            month is appended to the previous week.
            output_column (str): The name of the output column to store the new week of the month.

        Returns:
            DataFrame: The DataFrame with the new column indicating the new week of the month.
        """
        #
        result = df.withColumn(
            output_column,
            when(dayofmonth(col(current_date_column)).between(1, 7), 1)
            .otherwise(when(dayofmonth(col(current_date_column)).between(8, 15), 2)
            .otherwise(when(dayofmonth(col(current_date_column)).between(16, 21), 3)
            .otherwise(4)
            ))
        )
        return result
    #
    def _new_day_of_week(self,
        df: DataFrame,
        current_date_column:str,
        output_column:str
    ) -> DataFrame:
        """
        """
        result = df.withColumn(
            output_column,
            when(dayofmonth(col(current_date_column)).between(1, 7), dayofmonth(current_date_column))
            .otherwise(when(dayofmonth(col(current_date_column)).between(8, 15),  dayofmonth(current_date_column) - 7)
            .otherwise(when(dayofmonth(col(current_date_column)).between(16, 21),  dayofmonth(current_date_column) - 15)
            .otherwise( dayofmonth(current_date_column) - 21)
            ))
        )
        return result
    #


class WeeklyTFrom:
    """
    Clase para manejar la transformación semanal de tablas extraídas.

    Args:
        extracted_tables (List[str]): Lista de nombres de tablas extraídas.
        extracted_path (str): Ruta donde se encuentran las tablas extraídas.
        weeks_dict (Dict[str, Union[str, List[str]]]): Diccionario con información de las semanas.
        n_mis (int): Número de mis.
        sqlContext (SparkSession): Contexto de Spark.
        is_dynamic (bool): ``True`` -> reanudable por tabla: cada
            ``weekly_extract/<table>`` ya escrito se recarga en vez de
            recomputarse.
    """
    def __init__(
        self,
        extracted_tables:List[str],
        extracted_path:str,
        weeks_dict:Dict[str, Union[str,List[str]]],
        n_mis:int,
        sqlContext:SparkSession,
        is_dynamic:bool = False,
    ) -> None:
        self.extracted_tables = extracted_tables
        self.extracted_path = extracted_path
        self.extracted_hdfs = PosixPath(extracted_path)
        self.weeks_dict = weeks_dict
        self.n_mis = int(n_mis)
        self.sqlContext = sqlContext
        self.is_dynamic = bool(is_dynamic)

    def run(self) -> None:
        """
        Ejecuta la transformación semanal para cada tabla extraída.

        Itera sobre las tablas extraídas y aplica la transformación semanal
        utilizando la clase `WeeklyTFromParquetReTagger`.

        Returns:
            None
        """
        tfrom_method = self.weeks_dict["tfrom_method"]
        #
        for k in range(len(self.extracted_tables)):
            extracted_table_name = self.extracted_tables[k]
            extracted_path = str(self.extracted_hdfs.joinpath(extracted_table_name))
            output_path = str(self.extracted_hdfs.parent.joinpath("weekly_extract", extracted_table_name))
            date_column_for_weekly_tfrom = self.weeks_dict["date_column_for_weekly_tfrom"][k]
            type_of_date_column = self.weeks_dict["type_of_date_column"][k]
            weekly_tfrom_object = WeeklyTFromParquetReTagger(
                date_column_for_weekly_tfrom=date_column_for_weekly_tfrom,
                tfrom_method=tfrom_method,
                type_of_date_column=type_of_date_column,
                extracted_path=extracted_path,
                output_path=output_path,
                n_mis=self.n_mis,
                sqlContext=self.sqlContext,
                is_dynamic=self.is_dynamic
            )
            weekly_tfrom_object.run()


class WeeklyTFromParquetReTagger:
    id_column = 'numcliente'
    vintage_date_column = 'vintage_date'
    tfrom_date_column = 'tfrom_date'
    tfrom_column = 'tfrom'
    extra_months_per_run = 1
    mean_weeks_per_month = 365/12/7
    standard_weeks_per_year = 52
    forced_weeks_per_year = 12*4
    def __init__(
        self,
        date_column_for_weekly_tfrom:str,
        tfrom_method:str,
        type_of_date_column:str,
        extracted_path:str,
        output_path:str,
        n_mis:int,
        sqlContext:SparkSession,
        date_format:Optional[str]="yyyy-MM-dd",
        columns_to_drop_at_the_end:Optional[List[str]]=[],
        is_dynamic:Optional[bool]=False,
        mode:Optional[str]="overwrite"
    ) -> None:
        """
        Initialize the WeeklyTFromParquetReTagger class.

        Args:
            date_column_for_weekly_tfrom (str): The name of the date column to be used for weekly tfrom.
            tfrom_method (str): The method to calculate tfrom. Must be one of 'force_4_weeks', 'standard', or 'payroll'.
            type_of_date_column (str): The type of date column. Must be one of 'monthly', 'daily', or 'weekly'.
            extracted_path (str): The path to the extracted parquet file.
            output_path (str): The path to save the output parquet file.
            n_mis (int): Number of monts history
            sqlContext (SparkSession): The Spark session context.
            date_format (Optional[str], optional): The format of the date. Defaults to "yyyy-MM-dd".
            columns_to_drop_at_the_end (Optional[List[str]], optional): List of columns to drop at the end. Defaults to [].
            is_dynamic (Optional[bool], optional): Flag to indicate if the process is dynamic. Defaults to True.
            mode (Optional[str], optional): The mode for saving the output. Defaults to "overwrite".
        """
        # Assertions
        assert tfrom_method in ["force_4_weeks", "standard", "payroll"], "tfrom_method must be 'force_4_weeks' or 'standard' or 'payroll'"
        assert type_of_date_column in ["monthly","daily", "weekly"], "type_of_date_column must be either 'monthly' or 'daily' or 'weekly'"

        # Attributes
        self.date_column_for_weekly_tfrom = date_column_for_weekly_tfrom
        self.tfrom_method = tfrom_method
        self.type_of_date_column = type_of_date_column
        self.extracted_path = extracted_path
        self.output_path = output_path
        self.n_mis = int(n_mis)
        self.sqlContext = sqlContext
        #
        # Optional attributes
        if date_format is None:
            self.date_format = "yyyy-MM-dd"
        else:
            self.date_format = date_format
        #
        if columns_to_drop_at_the_end is None:
            self.columns_to_drop_at_the_end = []
        else:
            self.columns_to_drop_at_the_end = columns_to_drop_at_the_end
        #
        if is_dynamic is None:
            self.is_dynamic = False
        else:
            self.is_dynamic = is_dynamic
        #
        if mode is None:
            self.mode = "overwrite"
        else:
            self.mode = mode
        #
        # Path dict
        self.hdfs = {
            "extracted": extracted_path,
            "filtered": str(PosixPath(output_path).parent.joinpath(f"filtered_{PosixPath(output_path).name}")),
            "output": output_path
        }
    # RUN
    def run(self) -> None:
        """
        Run the process to clean and transform the data.
        """
        self.filled_columns
        self._clean()
    #
    # STEPS
    @cached_property
    def extracted(self) -> DataFrame:
        """
        Extract the data from the parquet file.

        Returns:
            DataFrame: The extracted data as a DataFrame.
        """
        result = self.sqlContext.read.parquet(self.hdfs["extracted"])
        if not result.schema[self.date_column_for_weekly_tfrom].dataType == "date":
            result = result.withColumn(self.tfrom_date_column, to_date(col(self.date_column_for_weekly_tfrom), self.date_format))
        else:
            result = result.withColumn(self.tfrom_date_column, col(self.date_column_for_weekly_tfrom))
        return result
    #
    @cached_property
    def extracted_with_weeks(self) -> DataFrame:
        """
        Add week of year and day of week columns to the extracted data.

        Returns:
            DataFrame: The extracted data with additional week and day columns.
        """
        result = self._get_new_week_of_year_day_of_month(
            df=self.extracted,
            date_column=self.tfrom_date_column,
            week_of_year_column="week_of_year",
            day_of_week_column="day_of_week"
        )
        #
        result = result.withColumn("last_day_vintage_date", last_day(col(self.vintage_date_column)))
        result = self._get_new_week_of_year_day_of_month(
            df=result,
            date_column="last_day_vintage_date",
            week_of_year_column="week_of_year_vintage_date",
            day_of_week_column="day_of_week_vintage_date"
        )
        #
        return result
    #
    @cached_property
    @dynamic(path_key="filtered")
    def filtered(self) -> DataFrame:
        """
        Filter the extracted data based on the date interval.

        Returns:
            DataFrame: The filtered data.
        """
        return self.extracted_with_weeks.filter((col(self.tfrom_date_column) >= str(self.date_interval[0]))
            & (col(self.tfrom_date_column) < str(self.date_interval[1])))
    #
    @cached_property
    @dynamic(path_key="output")
    def filled_columns(self) -> DataFrame:
        """
        Replaces the tfrom, tfrom_dates columns with the new values.

        Returns:
            DataFrame: The data with filled columns.
        """
        result = self.filtered
        #
        # PARTE 1 Cambiar la columna tfrom date a la que corresponda al day_of_week_1
        result = self.change_date_to_first_monday(df=result, date_column=self.tfrom_date_column, day_of_week_column="day_of_week")
        #
        # PARTE 2 Nuevo tfrom desde el ultimo dia del mes.
        result = result.drop(self.tfrom_date_column)
        result = result.withColumnRenamed(existing="first_monday", new=self.tfrom_date_column)
        #
        # Create one column one value dataframe
        result = weeks_difference(
            df=result,
            start_date_col=self.tfrom_date_column,
            start_week_of_year_col="week_of_year",
            end_date_col="last_day_vintage_date",
            end_week_of_year_col="week_of_year_vintage_date",
            weeks_per_year=self.weeks_per_year
        )
        result = result.drop("tfrom")
        result = result.withColumnRenamed(existing="weeks_diff", new="tfrom")
        result = result.withColumn("tfrom", col("tfrom").cast("double"))
        result = result.drop("last_day_vintage_date", "week_of_year", "day_of_week", "week_of_year_vintage_date",
        "day_of_week_vintage_date")
        if len(self.columns_to_drop_at_the_end) > 0:
            result = result.drop(*self.columns_to_drop_at_the_end)
        return result
    #
    # PROPERTIES
    @cached_property
    def min_max_dates(self) -> Tuple[date, date]:
        """
        Get the minimum and maximum dates from the extracted data.

        Returns:
            Tuple[date, date]: The minimum and maximum dates.
        """
        df = self.extracted.select(spark_max(self.tfrom_date_column).alias("max_date"), spark_min(self.tfrom_date_column).alias
        ("min_date"))
        collection = df.collect()
        max_date = collection[0]["max_date"]
        min_date = collection[0]["min_date"]
        return min_date, max_date
    #
    @cached_property
    def week_interval(self) -> Tuple[YearWeek, YearWeek]:
        """
        Get the minimum and maximum dates from the extracted data.

        Returns:
            Tuple[date, date]: The minimum and maximum dates.
        """
        _, max_date = self.min_max_dates
        #
        # Get max date of the month
        max_date: date = max_date.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
        max_yearweek = date_to_yearweek(max_date)
        # Check if the week is complete
        max_yearweek = add_weeks_to_yearweek(max_yearweek, -1) if not is_yearweek_complete_in_month(max_yearweek) else max_yearweek
        return add_weeks_to_yearweek(max_yearweek, - round(self.mean_weeks_per_month * (self.n_mis-(self.extra_months_per_run +
        1)))), add_weeks_to_yearweek(max_yearweek, 1)
    #
    @property
    def month_interval(self) -> Tuple[date, date]:
        """
        Get the minimum and maximum dates from the extracted data.

        Returns:
            Tuple[date, date]: The minimum and maximum dates.
        """
        _, max_date = self.min_max_dates
        # Get max date of the month
        max_date: date = max_date.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
        min_date = max_date - relativedelta(months=self.n_mis-(self.extra_months_per_run + 1))
        min_date = min_date.replace(day=1) # First day of the month
        return min_date, max_date + relativedelta(days=1)
    #
    @property
    def date_interval(self) -> Tuple[date, date]:
        """Calcula el intervalo de fechas basado en el tipo de columna de fecha.

        Raises:
            ValueError: Si el tipo de columna de fecha no es válido.

        Returns:
            Tuple[date, date]: Una tupla con el intervalo de fechas.
        """
        if self.type_of_date_column in ["monthly"] or self.tfrom_method in ["force_4_weeks", "payroll"]:
            # Aqui se utiliza esta opcion para force_4_weeks porque en este metodo los meses si están completos
            return self.month_interval
        elif (self.type_of_date_column == "daily") or (self.type_of_date_column == "weekly"):
            # TODO, el week interval está fallando.
            week_interval = self.week_interval
            return yearweek_to_date(week_interval[0]), yearweek_to_date(week_interval[1])
        #
        else:
            raise ValueError("type_of_date_column must be either 'monthly' or 'daily' or 'weekly'")
    #
    @property
    def weeks_per_year(self) -> int:
        """Calcula el número de semanas por año basado en el método de transformación.

        Raises:
            ValueError: Si el método de transformación no es válido.

        Returns:
            int: El número de semanas por año.
        """
        weeks_per_year = {
            "standard": self.standard_weeks_per_year,
            "force_4_weeks": self.forced_weeks_per_year,
            "payroll": self.forced_weeks_per_year,
        }
        try:
            return weeks_per_year[self.tfrom_method]
        except KeyError:
            raise ValueError("tfrom_method must be 'force_4_weeks', 'standard' or 'payroll'")
    #
    # METHODS WITH DECISIONS
    def _get_new_week_of_year_day_of_month(self, df: DataFrame, date_column:str, week_of_year_column:str, day_of_week_column:str) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y día de la semana basado en el método de transformación.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            week_of_year_column (str): El nombre de la nueva columna de semana del año.
            day_of_week_column (str): El nombre de la nueva columna de día de la semana.

        Raises:
            ValueError: Si el método de transformación no es válido.

        Returns:
            DataFrame: El DataFrame con las nuevas columnas.
        """
        functions = {
            "standard": self._get_new_week_of_year_day_of_month_standard,
            "force_4_weeks": self._get_new_week_of_year_day_of_month_force_4_weeks,
            "payroll": self._get_new_week_of_year_day_of_month_payroll,
        }
        try:
            function_ = functions[self.tfrom_method]
        except KeyError:
            raise ValueError("tfrom_method must be 'force_4_weeks', 'standard' or 'payroll'")
        return function_(df, date_column, week_of_year_column, day_of_week_column)
    #
    def change_date_to_first_monday(self, df: DataFrame, date_column: str, day_of_week_column:str) -> DataFrame:
        """Cambia la fecha a el primer lunes del mes o semana basado en el tipo de columna de fecha.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            day_of_week_column (str): El nombre de la columna de día de la semana.

        Raises:
            ValueError: Si el tipo de columna de fecha no es válido.

        Returns:
            DataFrame: El DataFrame con las fechas cambiadas.
        """
        functions = {
            "monthly": change_date_to_first_monday_of_the_month_column,
            "daily": change_date_first_monday_of_the_week_column,
            "weekly": change_date_first_monday_of_the_week_column,
        }
        try:
            change_date_to_first_monday = functions[self.type_of_date_column]
        except KeyError:
            raise ValueError("type_of_date_column must be either 'monthly' or 'daily' or 'weekly'")
        #
        return change_date_to_first_monday(df=df, date_column=date_column, day_of_week_column=day_of_week_column)
    #
    # METHODS
    @staticmethod
    def _get_new_week_of_year_day_of_month_week_of_month_object(df: DataFrame, date_column:str, week_of_year_column:str,
    day_of_week_column:str, Object:Type[WeekOfMonth]) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y día de la semana usando un objeto especifico.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            week_of_year_column (str): El nombre de la nueva columna de semana del año.
            day_of_week_column (str): El nombre de la nueva columna de día de la semana.
            Object (Type[WeekOfMonth]): El objeto a usar para el cálculo.

        Returns:
            DataFrame: El DataFrame con las nuevas columnas.
        """
        # Forced 4 weeks months
        date_df = df
        columns_before = date_df.columns
        test_object = Object(
            df=date_df,
            date_column=date_column
        )
        tagged_columns = [Object.new_week_of_year_column, Object.new_day_of_week_column]
        result = test_object.new_week_of_year_day_of_month
        result = result.select(columns_before + tagged_columns)
        result = result.withColumnRenamed(existing=Object.new_week_of_year_column, new=week_of_year_column)
        result = result.withColumnRenamed(existing=Object.new_day_of_week_column, new=day_of_week_column)
        return result
    #
    def _get_new_week_of_year_day_of_month_force_4_weeks(self, df: DataFrame, date_column:str, week_of_year_column:str,
    day_of_week_column:str) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y día de la semana para meses forzados a 4 semanas.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            week_of_year_column (str): El nombre de la nueva columna de semana del año.
            day_of_week_column (str): El nombre de la nueva columna de día de la semana.

        Returns:
            DataFrame: El DataFrame con las nuevas columnas.
        """
        return self._get_new_week_of_year_day_of_month_week_of_month_object(
            df=df,
            date_column=date_column,
            week_of_year_column=week_of_year_column,
            day_of_week_column=day_of_week_column,
            Object=WeekOfMonthFourWeeksForced
        )
    #
    def _get_new_week_of_year_day_of_month_payroll(self, df: DataFrame, date_column:str, week_of_year_column:str,
    day_of_week_column:str) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y día de la semana para payroll.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            week_of_year_column (str): El nombre de la nueva columna de semana del año.
            day_of_week_column (str): El nombre de la nueva columna de día de la semana.

        Returns:
            DataFrame: El DataFrame con las nuevas columnas.
        """
        return self._get_new_week_of_year_day_of_month_week_of_month_object(
            df=df,
            date_column=date_column,
            week_of_year_column=week_of_year_column,
            day_of_week_column=day_of_week_column,
            Object=WeekOfMonthPayroll
        )
    #
    def _get_new_week_of_year_day_of_month_standard(self, df: DataFrame, date_column:str, week_of_year_column:str,
    day_of_week_column:str) -> DataFrame:
        """Obtiene nuevas columnas de semana del año y día de la semana usando el método estándar.

        Args:
            df (DataFrame): El DataFrame de entrada.
            date_column (str): El nombre de la columna de fecha.
            week_of_year_column (str): El nombre de la nueva columna de semana del año.
            day_of_week_column (str): El nombre de la nueva columna de día de la semana.

        Returns:
            DataFrame: El DataFrame con las nuevas columnas.
        """
        result = df.withColumn(week_of_year_column, weekofyear(date_column))
        result = WeekOfMonth.day_of_week_starting_in_monday(output=day_of_week_column, df=result, column=date_column)
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
        save_parquet(df_save, path, self.mode)

    @staticmethod
    def _delete_intermediate(path:str) -> None:
        """Borra ``path`` si existe (ver :func:`libs.steps.path_delete`)."""
        path_delete(path)

    def _clean(self) -> None:
        """
        Limpia los datos intermedios y libera recursos.

        Si `is_dynamic` es True, no realiza ninguna acción. De lo contrario, elimina las rutas intermedias
        especificadas y libera los DataFrames persistidos.

        Returns:
            None
        """
        if self.is_dynamic:
            return None
        intermediate_hdfs = ["filtered"]
        for path in intermediate_hdfs:
            try:
                self._delete_intermediate(self.hdfs[path])
            except:
                pass
        #
        self.extracted.unpersist()
        self.extracted_with_weeks.unpersist()
        self.filtered.unpersist()
        self.filled_columns.unpersist()
