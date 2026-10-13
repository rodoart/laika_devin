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
import os
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

class GeneratePivot:
    """
    Class for transposing a data table.

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

    def __init__(self, previous_path, pivot_dict, sqlContext, mode="overwrite") -> None:
        self.previous_path = previous_path
        self.pivot_dict = pivot_dict
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

    def pivot_data(self) -> None:
        """
        Process to go from long format data to wide format.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        source = self.pivot_dict["source"]
        if source in ("corporate", "clocks", "bureau_pull", "bureau_cohort", "payroll_income"):
            read_path = os.path.join(
                self.previous_path, self.pivot_dict["table_name"])
        else:
            read_path = self.previous_path
        #P
        df_in = self.sqlContext.read.parquet(read_path)
        group_by = self.pivot_dict["pivot_key"]
        cols, dtypes = zip(*[(c, t)
            for (c, t) in df_in.dtypes if c not in group_by])
        kvs = explode(array(
            struct(lit(c).alias("key"), col(c).alias("value")) for c in cols)).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        head, _ = os.path.split(self.previous_path)

        if source in ("bureau_cohort"):
            self._save_parquet(df_wide, os.path.join(head, "pivot",self.pivot_dict["table_name"]))
            df_wide.unpersist()
        else:
            self._save_parquet(df_wide, os.path.join(head, "pivot"))
            df_wide.unpersist()

    def pivot_data_aux(self, path, group_by) -> None:
        """
        Auxiliar function to go from long format data to wide format.

        Parameters
        ----------
        path: string
            Path to read the data from.
        group_by: List
            String list of columns to exclude from the transposing process.

        Returns
        -------
        None

        """
        df_in = self.sqlContext.read.parquet(path)
        cols, dtypes = zip(*[(c, t)
            for (c, t) in df_in.dtypes if c not in group_by])
        kvs = explode(array(
            struct(lit(c).alias("key"), col(c).alias("value")) for c in cols)).alias("kvs")
        df_long = df_in.select(group_by + [kvs]).select(
            group_by + ["kvs.key", "kvs.value"])
        df_long = df_long.withColumn("tfrom", col("tfrom").cast(IntegerType()))
        df_long = df_long.withColumn("fecha_", concat_ws(
            "_", df_long["key"], df_long["tfrom"]))
        df_wide = df_long.groupBy(
            "numcliente", "vintage_date").pivot("fecha_").avg("value")
        head, _ = os.path.split(path)
        df_wide.write.mode("overwrite").parquet(os.path.join(head, "pivot"))
        df_wide.unpersist()
