"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
from pyspark.sql.functions import col
from .base import BaseAggregate
import logging

class CorporateAggregate(BaseAggregate):
    """
    Class to aggregate Corporate Data

    Parameters
    ----------
    previous_path: string
        Path to read data from
    agg_dict: Dictionary
        All parameters needed by the class(key, group_by, querys)
    sqlContext: spark context
        Your sqlContext
    mode: string
        Optional, parquet write mode, defaults to overwrite
    """

    #: Columna indicadora y número de niveles por variable categórica:
    #: la categoría resultante es ``grupo * n_niveles + nivel + 1``
    #: (grupo: 0=PENSIONADOS, 1=GOBIERNO, 2=resto).
    CAT_SPECS = {
        "cat_ind_i2_seg": ("pct_ind_i2", 2),
        "cat_iv2_seg": ("pct_iv2", 2),
        "cat_ind_i5_seg": ("pct_ind_i5", 5),
        "cat_iv5_seg": ("pct_iv5", 5),
    }

    def aggregate(self) -> None:
        """
        Process to aggregate corporate data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        corporate = self._read_table()
        conditions = self.agg_dict["conditions"]
        logging.debug("Variables en corporate[%d]: %s",len(corporate.columns),corporate.columns)
        groups = [col('seg').isin('PENSIONADOS'),
                  col('seg').isin('GOBIERNO'),
                  ~col('seg').isin('PENSIONADOS', 'GOBIERNO')]
        for column in self.agg_dict["new_columns"]:
            spec = self.CAT_SPECS.get(column)
            if spec is None:
                continue
            ind_col, n_levels = spec
            pairs = [(seg & (col(ind_col) == level),
                      gi * n_levels + level + 1)
                     for gi, seg in enumerate(groups)
                     for level in range(n_levels)]
            corporate = corporate.withColumn(
                column, self._nested_when(pairs, default=-99999))
        for column in self.agg_dict["drop_columns"]:
            corporate = corporate.drop(column)
        path_save = os.path.join(self.previous_path.replace(
            "extract", "groupby"), self.agg_dict["table_name"])
        self._save_parquet(corporate, path_save)
