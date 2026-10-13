# -*- coding: utf-8 -*-
"""
Project: Data Pull Framework (PEP 8 & Python3.6)
Developer: @rm36973
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to transpose table data A.K.A go from
a long data form to wide

"""

from pyspark.sql.functions import array, col, explode, struct, lit, concat_ws
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
import os

class ColumnsToVector:
    """
    Class

    Parameters
    ----------
    previous_path: string
        Path where data is read.
    pivot_dict: Dictionary
        Parameters needed by the class(key, group_by, etc).
    sqlContext: spark context
        Your sqlContext
    mode: string, optional
        Parquet write mode, defaults to 'overwrite'.
    """

    def __init__(self, write_path, v2c_dict, sqlContext, mode="overwrite") -> None:
        self.write_path = write_path
        self.v2c_dict = v2c_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _save_parquet(self, df_save, path) -> None:
        """
        Save data as parquet on desired location.

        Parameters
        ----------
        df_save: PySpark Dataframe
            Data to be saved
        path: string
            Path to store the data

        Returns
        -------
        None

        """

        df_save.write.mode(self.mode).parquet(path)

    def run(self) -> None:
        """
        Process to go from long format data to wide format.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        source = self.v2c_dict["source"]
        print("V2C: "+source)

        if source in ("bureau_cohort"):
            read_path = os.path.join(self.write_path, self.v2c_dict["prev_path"], self.v2c_dict["table_name"])
        else:
            read_path = os.path.join(self.write_path, self.v2c_dict["prev_path"])

        df_in = self.sqlContext.read.parquet(read_path)
        df_in = df_in.na.fill(0)
        feature_columns = [c for c in df_in.columns if c not in ["numcliente","vintage_date", "id_msf_tools_v2_12315"]]

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_vector")
        final_features_df = assembler.transform(df_in)
        final_features_df = final_features_df.select("numcliente","vintage_date", "features_vector")
        final_features_df = final_features_df.withColumnRenamed("vintage_date","vintage")
        final_features_df.write.mode(self.mode).parquet(os.path.join(self.write_path,"vector_assembler"))
