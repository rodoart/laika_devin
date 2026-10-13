"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Clase y funciones para agregación (general): dispatcher ``GroupBy`` que
instancia el agregador correcto según la fuente. La transposición
long -> wide vive en ``libs/generate_pivot.py``.
"""

from pyspark.sql import SparkSession
import logging
from typing import Any, Dict, List, Type

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

# Registro source -> clase agregadora. Para añadir un source nuevo basta
# con una entrada aquí (más su bloque en configuration.json).
AGGREGATE_CLASSES: Dict[str, Type] = {
    "financiera": FinancieraAggregate,
    "channels": ChannelsAggregate,
    "product_data": ProductDataAggregate,
    "corporate": CorporateAggregate,
    "profundidad": ProfundidadAggregate,
    "pnl": PNLAggregate,
    "pnl_new": PNLNewAggregate,
    "pnl_cohort": PNLCohortAggregate,
    "laika": LaikaAggregate,
    "suguru": SuguruAggregate,
    "s15_nf": S15NFAggregate,
    "s15": S15Aggregate,
    "morkitara": MorkitaraAggregate,
    "atms": AtmsAggregate,
    "giros_credito": MacrogirosAggregate,
    "bureau_cohort": BureauCohortAggregate,
    "bureau_pull": BureauPullAggregate,
    "liabilities": LiabilitiesPullAggregate,
    "rev_balance": RevBalanceAggregate,
    "payroll_income": PayrollIncomeAggregate,
    "payroll_streaks": PayrollStreaksAggregate,
}

# Argumentos extra que cada agregador acepta además de
# (previous_path, agg_dict, sqlContext, mode).
AGGREGATE_EXTRA_ARGS: Dict[str, List[str]] = {
    "product_data": ["n_mis", "period"],
    "payroll_income": ["n_mis", "period"],
    "payroll_streaks": ["pivot_path", "tables", "n_mis", "period"],
}

class GroupBy:
    """Group By para crear los datos agrupados del datapull.

    Dispatcher: busca la clase ``*Aggregate`` en
    :data:`AGGREGATE_CLASSES` según ``source`` y llama a su
    ``aggregate()``. Los argumentos extra que acepta cada agregador se
    declaran en :data:`AGGREGATE_EXTRA_ARGS`.

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
        """Instancia el agregador de la fuente con los argumentos que
        espera (:data:`AGGREGATE_EXTRA_ARGS`) y llama a ``aggregate()``."""
        group_class = self._obtain_object()
        kwargs: Dict[str, Any] = {
            "previous_path": self.previous_path,
            "agg_dict": self.agg_dict,
            "sqlContext": self.sqlContext,
            "mode": self.mode,
        }
        for arg in AGGREGATE_EXTRA_ARGS.get(self.source, []):
            kwargs[arg] = getattr(self, arg)
        group_class(**kwargs).aggregate()

    def _obtain_object(self) -> Type:
        """Devuelve la clase ``*Aggregate`` correspondiente a ``self.source``.

        Returns
        -------
        Type
            Clase agregadora de la fuente.

        Raises
        ------
        KeyError
            Si ``self.source`` no está registrada en
            :data:`AGGREGATE_CLASSES`.
        """
        try:
            return AGGREGATE_CLASSES[self.source]
        except KeyError:
            raise KeyError(
                f"Source '{self.source}' has no aggregate registered in "
                f"AGGREGATE_CLASSES (libs/group_by.py).") from None
