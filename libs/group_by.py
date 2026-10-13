"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType
import logging

from .aggregattes.bureau_cohort import BureauCohortAggregate
from .aggregattes.bureau_pull import BureauPullAggregate
from .aggregattes.channels import ChannelsAggregate
from .aggregattes.corporate import CorporateAggregate
from .aggregattes.financiera import FinancieraAggregate
from .aggregattes.liabilities import LiabilitiesPullAggregate
from .aggregattes.macrogiros import MacrogirosAggregate
from .aggregattes.payroll_income import PayrollIncomeAggregate
from .aggregattes.pnl import PNLAggregate
from .aggregattes.pnl_new import PNLNewAggregate
from .aggregattes.pnl_cohort import PNLCohortAggregate
from .aggregattes.product_data import ProductDataAggregate
from .aggregattes.profundidad import ProfundidadAggregate
from .aggregattes.rev_balance import RevBalanceAggregate
from .aggregattes.payroll_streaks import PayrollStreaksAggregate
from .aggregattes.laika import LaikaAggregate
from .aggregattes.suguru import SuguruAggregate
from .aggregattes.s15_nf import S15NFAggregate
from .aggregattes.atms import AtmsAggregate
from .aggregattes.s15 import S15Aggregate
from .aggregattes.mokitara import MorkitaraAggregate

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)

class GeneratePivot:
    """
    Class Generate Pivot for transposing a data table

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
        """
        Class constructor.
        """
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
        if source in ("corporate", "clocks", "bureau_pull", "bureau_cohort"):
            read_path = os.path.join(
                self.previous_path, self.pivot_dict["table_name"])
        else:
            read_path = self.previous_path

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

class GroupBy:
    """
    Class Group By for creating the grouped data in datapull.

    Parameters
    ----------
    source: string
        Data Source name
    previous_path: string
        Path to save the groupby
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    n_mis : int
        Number of missing values allowed
    period : int
        Time period for the groupby
    pivot_path : str, optional
        Path for pivot table, defaults to an empty string
    tables : str, optional
        List of tables involved in the groupby, defaults to an empty string
    mode : str, optional
        Parquet write mode, defaults to 'overwrite'
    """

    def __init__(self,
        source:str,
        previous_path: str,
        agg_dict:dict,
        sqlContext:SparkSession,
        n_mis:int,
        period:int,
        pivot_path:str="",
        tables:str="",
        mode:str="overwrite"
    ) -> None:
        """
        Class constructor
        """
        self.source = source
        self.previous_path = previous_path
        self.pivot_path = pivot_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.n_mis = n_mis
        self.period = period
        self.tables = tables
        self.mode = mode

    def run(self) -> None:
        """
        Executes groupby process.

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        group_class = self._obtain_object()
        if self.source in ["product_data", "payroll_income"]:
            group_obj = group_class(
                previous_path=self.previous_path,
                agg_dict=self.agg_dict,
                n_mis=self.n_mis,
                period=self.period,
                sqlContext=self.sqlContext,
                mode=self.mode)
        elif self.source in ["payroll_streaks"]:
            group_obj = group_class(
                previous_path=self.previous_path,
                pivot_path=self.pivot_path,
                tables=self.tables,
                agg_dict=self.agg_dict,
                n_mis=self.n_mis,
                period=self.period,
                sqlContext=self.sqlContext,
                mode=self.mode)
        else:
            group_obj = group_class(
                previous_path=self.previous_path,
                agg_dict=self.agg_dict,
                sqlContext=self.sqlContext,
                mode=self.mode)
        group_obj.aggregate()
        #
    def _obtain_object(self) -> type[FinancieraAggregate] | type[ChannelsAggregate] | type[ProductDataAggregate] | type[CorporateAggregate] | type[ProfundidadAggregate] | type[PNLAggregate] | type[PNLNewAggregate] | type[PNLCohortAggregate] | type[LaikaAggregate] | type[SuguruAggregate] | type[S15NFAggregate] | type[S15Aggregate] | type[MorkitaraAggregate] | type[AtmsAggregate] | type[MacrogirosAggregate] | type[BureauCohortAggregate] | type[BureauPullAggregate] | type[LiabilitiesPullAggregate] | type[RevBalanceAggregate] | type[PayrollIncomeAggregate] | type[PayrollStreaksAggregate]:
        """
        Returns the corresponding groupby object of the source

        Parameters
        ----------
        None

        Returns
        -------
        object
            Source GroupBy Object
        """
        if self.source == "financiera":
            group_class = FinancieraAggregate
        elif self.source == "channels":
            group_class = ChannelsAggregate
        elif self.source == "product_data":
            group_class = ProductDataAggregate
        elif self.source == "corporate":
            group_class = CorporateAggregate
        #elif self.source == "clocks":
        #   pass
        elif self.source == "profundidad":
            group_class = ProfundidadAggregate
        elif self.source == "pnl":
            group_class = PNLAggregate
        elif self.source == "pnl_new":
            group_class = PNLNewAggregate
        elif self.source == "pnl_cohort":
            group_class = PNLCohortAggregate
        elif self.source == "laika":
            group_class = LaikaAggregate
        elif self.source == "suguru":
            group_class = SuguruAggregate
        elif self.source == "s15_nf":
            group_class = S15NFAggregate
        elif self.source == "s15":
            group_class = S15Aggregate
        elif self.source == "morkitara":
            group_class = MorkitaraAggregate
        elif self.source == "atms":
            group_class = AtmsAggregate
        elif self.source == "giros_credito":
            group_class = MacrogirosAggregate
        elif self.source == "bureau_cohort":
            group_class = BureauCohortAggregate
        elif self.source == "bureau_pull":
            group_class = BureauPullAggregate
        elif self.source == "liabilities":
            group_class = LiabilitiesPullAggregate
        elif self.source == "rev_balance":
            group_class = RevBalanceAggregate
        elif self.source == "payroll_income":
            group_class = PayrollIncomeAggregate
        elif self.source == "payroll_streaks":
            group_class = PayrollStreaksAggregate
        return group_class
