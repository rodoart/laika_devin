"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Clase y funciones para agregación (general): dispatcher ``GroupBy`` que
instancia el agregador correcto según la fuente, más una copia de
``GeneratePivot`` (transposición long -> wide).
"""

import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType
import logging
from typing import Any, Dict, List, Optional, Type

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
from .aggregattes.rev_balnce import RevBalanceAggregate
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
    """Transpone una tabla de datos (long -> wide).

    Parameters
    ----------
    previous_path : str
        Path donde se leen los datos.
    pivot_dict : Dict[str, Any]
        Parámetros que necesita la clase (``source``, ``pivot_key``,
        ``table_name``).
    sqlContext : SparkSession
        Tu sqlContext.
    mode : str, optional
        Modo de escritura del parquet, por defecto ``'overwrite'``.
    """

    def __init__(self, previous_path: str, pivot_dict: Dict[str, Any],
                 sqlContext: SparkSession, mode: str = "overwrite") -> None:
        """Constructor de la clase."""
        self.previous_path: str = previous_path
        self.pivot_dict: Dict[str, Any] = pivot_dict
        self.sqlContext: SparkSession = sqlContext
        self.mode: str = mode

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """Guarda datos como parquet en la ubicación indicada.

        Parameters
        ----------
        df_save : DataFrame
            Datos a guardar.
        path : str
            Path donde almacenar los datos.
        """
        df_save.write.mode(self.mode).parquet(path)

    def pivot_data(self) -> None:
        """Pasa de formato largo a ancho y guarda en ``pivot``."""
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
            *[struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
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

    def pivot_data_aux(self, path: str, group_by: List[str]) -> None:
        """Versión auxiliar de long -> wide sobre un path arbitrario.

        Parameters
        ----------
        path : str
            Path del que leer los datos.
        group_by : List[str]
            Columnas excluidas del proceso de transposición.
        """
        df_in = self.sqlContext.read.parquet(path)
        cols, dtypes = zip(*[(c, t)
            for (c, t) in df_in.dtypes if c not in group_by])
        kvs = explode(array(
            *[struct(lit(c).alias("key"), col(c).alias("value")) for c in cols])).alias("kvs")
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
    """Group By para crear los datos agrupados del datapull.

    Dispatcher: según ``source`` instancia el ``*Aggregate``
    correspondiente (ver :meth:`_obtain_object`) y llama a su
    ``aggregate()``.

    Parameters
    ----------
    source : str
        Nombre de la fuente.
    previous_path : str
        Path del paso anterior (entrada del groupby).
    agg_dict : Dict[str, Any]
        Todos los parámetros que necesita la clase (key, group_by,
        queries).
    sqlContext : SparkSession
        Tu sqlContext.
    n_mis : int
        Número de meses de información.
    period : int
        Periodo temporal del groupby.
    pivot_path : str, optional
        Path de la tabla pivot; por defecto cadena vacía.
    tables : str, optional
        Lista de tablas involucradas en el groupby; por defecto cadena
        vacía.
    mode : str, optional
        Modo de escritura del parquet; por defecto ``'overwrite'``.
    """

    def __init__(self,
        source: str,
        previous_path: str,
        agg_dict: Dict[str, Any],
        sqlContext: SparkSession,
        n_mis: int,
        period: int,
        pivot_path: str = "",
        tables: Any = "",
        mode: str = "overwrite"
    ) -> None:
        """Constructor de la clase."""
        self.source: str = source
        self.previous_path: str = previous_path
        self.pivot_path: str = pivot_path
        self.agg_dict: Dict[str, Any] = agg_dict
        self.sqlContext: SparkSession = sqlContext
        self.n_mis: int = n_mis
        self.period: int = period
        self.tables: Any = tables
        self.mode: str = mode

    def run(self) -> None:
        """Ejecuta el proceso de groupby: instancia el agregador de la
        fuente (con el set de argumentos que cada uno espera) y llama a su
        ``aggregate()``."""
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
        """Devuelve la clase ``*Aggregate`` correspondiente a ``self.source``.

        Returns
        -------
        type
            Clase agregadora de la fuente.
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
