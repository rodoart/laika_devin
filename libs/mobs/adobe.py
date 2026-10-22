"""
Project: Adobe Clickstream Mobs
Developer: Devin
Update Date: 2026
Version: 0.1

Imputador de features por MOBs de las fuentes Adobe
(``adobe_bancanet`` / ``adobe_mobile``).

Las features adobe llevan sufijo ``_<N>w`` (ventanas de ``N`` tfroms del
dominio de la variable) mientras que ``mobs_adobe`` está en **meses**. La
regla replica la del ``previous_fixer`` genérico —una ventana que exige
más historia que la observada queda a ``-99999``— con la conversión
``meses * 4`` semanas/mes de ``force_4_weeks``.

Clientes sin actividad digital (ausentes de ``calculate_mobs``) se tratan
como ``mobs=0``: todas sus ventanas quedan a ``-99999`` — es la base de
la imputación por inactividad.

Nota sobre unidades: para los dominios ``_q``/``_d``/``_ld`` el índice
``N`` del sufijo no son semanas literales (quincenas / días / buckets de
last_days); la comparación lo trata como semanas, es decir es
**conservadora** (puede imputar ``-99999`` a una ventana que el cliente
cubriría en días, nunca al revés).
"""
import re
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from .payroll_income import PreviousFixerPayroll


class PreviousFixerAdobe(PreviousFixerPayroll):
    """Imputador de MOBs para ``adobe_bancanet`` / ``adobe_mobile``.

    Solo difiere de la base en:

    - :attr:`DEFAULT_MB_TY` — ``mobs_adobe`` del parquet
      ``calculate_mobs``.
    - :attr:`month_suffix_pattern` — las features son ``_<N>w``.
    - :meth:`payroll_mob_imputation` — ``-99999`` cuando la ventana
      ``N`` excede ``mobs_adobe * 4`` semanas (mobs ausente = 0).
    """

    #: Columnas de mobs del parquet ``calculate_mobs``.
    DEFAULT_MB_TY = ["mobs_adobe"]
    #: Columnas de mobs que se eliminan del DataFrame de salida.
    MOBS_DROP_COLUMNS = DEFAULT_MB_TY
    #: Las ventanas adobe son ``_<N>w``, no ``_<N>m``.
    month_suffix_pattern = r"_\d+w$"
    #: Meses -> semanas del dominio semanal (``force_4_weeks``).
    WEEKS_PER_MONTH = 4

    @staticmethod
    def payroll_mob_imputation(df: DataFrame, features: List[str],
                               lista_meses: List[int],
                               reverse: Optional[bool] = False) -> DataFrame:
        """Imputa ``-99999`` a cada ``_<N>w`` con ``N > mobs_adobe * 4``.

        Itera las columnas ``_<N>w`` reales del dataframe (los dominios
        adobe producen índices dispersos: semanas, quincenas, last_days y
        dseq mezclados). Un mobs nulo (cliente sin actividad, fuera de
        ``calculate_mobs``) cuenta como 0 -> toda ventana a ``-99999``.

        Args:
            df (DataFrame): features ya cruzadas con ``mobs_adobe``.
            features (list): variables base (no usado: se derivan las
                columnas ``_<N>w`` presentes).
            lista_meses (list): no usado (ver docstring de clase).
            reverse (bool): no usado.

        Returns:
            DataFrame: features imputadas, sin la columna ``mobs_adobe``.
        """
        window_col = re.compile(r"_(\d+)w$")
        for name in df.columns:
            match = window_col.search(name)
            if not match:
                continue
            n = int(match.group(1))
            covered = col("mobs_adobe") * \
                PreviousFixerAdobe.WEEKS_PER_MONTH >= n
            df = df.withColumn(
                name, when(covered, col(name)).otherwise(lit(-99999)))
        return df.drop("mobs_adobe")
