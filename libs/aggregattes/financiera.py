"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import pandas as pd
from pyspark.sql.functions import col, lit, create_map, lower
from itertools import chain
from typing import Any, Dict, List
from .base import BaseAggregate

class FinancieraAggregate(BaseAggregate):
    """
    Class to aggregate Financiera Data

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

    def _create_groupby_query(self, query_dict: Dict[str, Any], group_by: List[str]) -> str:
        """
        Creates the SQL aggregation sentence to group the Financiera data

        Parameters
        ----------
        query_dict: Dictionary
            All parameters needed to create SQL sentence
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict)
        list_aux = []

        for _, item in agg_df.iterrows():
            for sth in item['variable']:
                if isinstance(sth, str):
                    str_aux = item['agg']+"(case when b1 in ('"+sth + \
                    "') then saldo else 0 end ) as "+item['feature']
                else:
                    str_aux = item['agg']+"(case when b1 in ("+",".join(
                    '"'+iii+'"' for iii in sth)+") then saldo else 0 end ) as "+item['feature']
            list_aux.append(str_aux)
        query_string = ",".join(list_aux)
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+query_string + \
            " FROM financiera GROUP BY "+group_str
        return query_str

    def aggregate(self) -> None:
        """
        Process to aggregate Financiera data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        cves_dict = {
            2030: 'adela', 2130: 'adela', 1011: 'cheques', 1010: 'cheques', 5000: 'bancanet',
            2100: 'comercial', 2170: 'comercial', 9002: 'credichq', 1020: 'ctamtra', 1040: 'debito',
            2050: 'hipotecario', 2060: 'hipotecario', 2070: 'hipotecario', 2140: 'hipotecario', 2150: 'hipotecario',
            1060: 'inversion', 1090: 'inversion', 1091: 'inversion', 1100: 'inversion', 1110: 'inversion', 1130: 'inversion',
            1150: 'inversion', 1160: 'inversion', 4000: 'inversion', 9001: 'inversion', 1070: 'miahorro',
            9004: 'otros', 9005: 'otros', 9006: 'otros', 9007: 'otros', 9008: 'otros', 9009: 'otros', 9010: 'otros',
            9011: 'otros', 9012: 'otros', 9013: 'otros', 1050: 'pagomatico', 1030: 'perfiles', 2020: 'personales',
            2040: 'personales', 2080: 'personales', 2090: 'personales', 2120: 'personales', 2160: 'personales',
            1080: 'plazo', 1120: 'plazo', 2105: 'pyme', 2173: 'pyme', 3001: 'seguros', 3002: 'seguros', 3003: 'seguros',
            3004: 'seguros', 1140: 'servicios', 1141: 'servicios', 9003: 'servicios',
            1055: 'tarjetas', 2010: 'tarjetas', 2011: 'tarjetas', 2075: 'tarjetas', 2110: 'tarjetas', 2175: 'tarjetas',
            8000: 'na', 1: 'na', 2: 'na', 0: 'na'
        }

        reading_path = self._table_path()
        financiera = self.sqlContext.read.parquet(reading_path)
        mapping_expr = create_map([lit(x) for x in chain(*cves_dict.items())])
        financiera = financiera.withColumn("b1", mapping_expr[col("cve_ayp")])
        financiera_aux = financiera.select(
            "numcliente", "saldo", "saldoaj", "vintage_date", "tfrom", "b1")
        financiera_aux = financiera_aux.withColumn('b1', lower(col('b1')))
        financiera_aux.createOrReplaceTempView("financiera")
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'])
        df_out = self.sqlContext.sql(query_str)
        self._save_parquet(df_out, self._sibling_path("groupby"))
