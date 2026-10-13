"""
Project: Special Treatment for Payroll Streaks
Developer: @rg49392
Update Date: 2025-02-19
Version: 0.1

Class and Functions required to aggregate (payroll streaks)
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from typing import Optional

from .default import ExtractSpecialTreatment


class PayrollStreaksExtractSpecialTreatment(ExtractSpecialTreatment):
    """
    Class to handle special treatment for payroll streaks extraction.

    This class extends ExtractSpecialTreatment to provide specific logic
    for handling payroll streaks data.

    Attributes
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
    sqlContext : SparkSession, optional
        Spark session context, defaults to the parent's sqlContext if not provided.
    columns_to_stack : list
        List of columns to be stacked.
    _numcliente : str
        Internal column name for client number.
    """
    _numcliente = "numcliente"
    def __init__(
        self,
        parent:"Extract",
        input_df:DataFrame,
        table_name:str,
        date:str,
        query:str,
        sqlContext:Optional[SparkSession] = None
    ) -> None:
        """
        Initialize the PayrollStreaksExtractSpecialTreatment class.

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
        self.columns_to_stack = ["num_cliente", "num_cliente_2", "num_cliente_3"]
        for column in self.columns_to_stack:
            assert column in input_df.columns, f"Column {column} not found in input_df"
        super().__init__(parent, input_df, table_name, date, query, sqlContext)

    def _special_treatment(self, df:DataFrame) -> DataFrame:
        """
        Apply special treatment to the DataFrame for payroll streaks.

        This method stacks specified columns and processes them to handle
        payroll streaks data.

        Parameters
        ----------
        df : DataFrame
            DataFrame to which special treatment will be applied.

        Returns
        -------
        DataFrame
            DataFrame after applying special treatment.
        """
        not_id_columns = [column for column in df.columns if column not in self.columns_to_stack]
        #
        for k, id_column in enumerate(self.columns_to_stack):
            id_df = df.select(*([id_column] + not_id_columns))
            #
            id_df = id_df.dropna(subset=[id_column])
            id_df = id_df.withColumnRenamed(existing=id_column, new=self._numcliente)
            if k==0:
                result = id_df
            else:
                id_df = id_df.select(result.columns)
                result = result.union(id_df)
        #
        return result
