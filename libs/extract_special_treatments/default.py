"""
Project: Special Treatment for the Extract
Developer: @rg49392
Update Date: 2025-02-19
Version: 0.1

Class and Functions required to aggregate (payroll streaks)
"""

# libraries
from pyspark.sql import SparkSession, DataFrame
from typing import Optional


class ExtractSpecialTreatment:
    """
    Initialize the ExtractSpecialTreatment class.

    Parameters
    ----------
    parent : Extract
        The parent Extract object.
    input_df : DataFrame
        DataFrame containing the input data.
    table_name : str
        Name of the source table.
    date : str
        Date for which the information is needed.
    query : str
        Query string to be used for extraction.
    sqlContext : Optional[SparkSession], optional
        Spark session context, defaults to the parent's sqlContext if not provided.
    """

    def __init__(
        self,
        parent:"Extract",
        input_df:DataFrame,
        table_name:str,
        date:str,
        query:str,
        sqlContext:Optional[SparkSession] = None
    ) -> None:
        """_summary_

        Args:
            parent (Extract): _description_
            input_df (DataFrame): _description_
            table_name (str): _description_
            date (str): _description_
            query (str): _description_
            sqlContext (Optional[SparkSession], optional): _description_. Defaults to None.
        """
        self.parent = parent
        self.input_df = input_df
        self.table_name = table_name
        self.date = date
        self.query = query
        if sqlContext is None:
            self.sqlContext = parent.sqlContext
        else:
            self.sqlContext = sqlContext

    def _special_treatment(self, df:DataFrame) -> DataFrame:
        """
        Apply special treatment to the DataFrame.

        Parameters
        ----------
        df : DataFrame
            DataFrame to which special treatment will be applied.

        Returns
        -------
        DataFrame
            DataFrame after applying special treatment.
        """
        # NOTE: last part of this method was blurry in photo IMG_20261012_124726.jpg
        return df

    def run(self) -> DataFrame:
        """
        Applies the special treatment to the input DataFrame.

        Returns
        -------
        DataFrame
            The processed DataFrame.
        """
        # NOTE: reconstructed — inferred from usage in Extract._create_month_cohort
        return self._special_treatment(self.input_df)
