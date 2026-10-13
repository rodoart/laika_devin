"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from typing import Any, Dict, List
from ..generate_pivot import GeneratePivot
from .base import BaseAggregate

class ProductDataAggregate(BaseAggregate):
    """
    Class to aggregate Product Data Data

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

    def __init__(self, previous_path: str, agg_dict: Dict[str, Any], n_mis: int,
                 period: int, sqlContext: SparkSession, mode: str = "overwrite") -> None:
        """
        Class constructor.
        """
        super().__init__(previous_path, agg_dict, sqlContext, mode)
        self.n_mis = n_mis
        self.period = period

    def _create_groupby_query(self, query_dict: Dict[str, Any], group_by: List[str], tempview: str) -> str:
        """
        Creates the SQL aggregation query string use to group the bureau pull data

        Parameters
        ----------
        query_dict: Dictionary
            Variables to consider product tenure
        group_by: List
            String list containing group by column names

        Returns
        -------
        string
            SQL Sentence
        """
        agg_df = pd.DataFrame(query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}(case when {1}> 0 then 1 else 0 end ) as {2}'.
                    format(item['agg'], item['variable'],
                    item['feature'])
                    for _, item in agg_df.iterrows()]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM "+tempview+" GROUP BY "+group_str
        return query_str

    def aggregate(self) -> None:
        """
        Process to aggregate bureau pull data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        reading_path_baseline = self._table_path('table_names', 'baseline')
        baseline_df = self.sqlContext.read.parquet(reading_path_baseline)
        reading_path_ccp = self._table_path('table_names', 'ccp')
        ccp_df = self.sqlContext.read.parquet(reading_path_ccp)

        query_baseline = self._create_groupby_query(
            self.agg_dict['querys_dict']["baseline_dict"], self.agg_dict['group_by_key'], "baseline")
        query_ccp = self._create_groupby_query(
            self.agg_dict['querys_dict']["ccp_dict"], self.agg_dict['group_by_key'], "ccp")

        baseline_df.createOrReplaceTempView("baseline")
        ccp_df.createOrReplaceTempView("ccp")

        flags_base = self.sqlContext.sql(query_baseline)
        flags_xec_perf = self.sqlContext.sql(query_ccp)

        df_product = flags_base.join(
            flags_xec_perf, on=self.agg_dict['group_by_key'], how="full")

        df_product = df_product.withColumn(
            "tfrom", df_product["tfrom"].cast(IntegerType()))
        df_product = df_product.na.fill(0)
        self._save_parquet(df_product, self._sibling_path("groupby"))

        pivot_product_data = GeneratePivot(None, None, self.sqlContext)
        pivot_product_data.pivot_data_aux(self._sibling_path("groupby"), [
            "numcliente", "vintage_date", "tfrom"])

        product_data = self.sqlContext.read.parquet(
            self._sibling_path("pivot"))
        product_data.createOrReplaceTempView("data_flags1")

        features = ["ind_cnb", "ind_cpb", "ind_mor", "ind_disp", "ind_effec"]
        meses = range(self.period, self.n_mis +1, self.period)

        lista_aux = [[feature+"_"+str(idx) for idx in range(12) if feature+"_"+str(
            idx) in product_data.columns] for feature in features]
        lista_aux2 = [prod[:mes] for prod in lista_aux for mes in meses]

        query_Str = "select numcliente, vintage_date,"+",".join("greatest("+",".join(l1)+") as "+l1[0][:-1] + str(
            len(l1)) + "m " for _, l1 in enumerate(lista_aux2)) + " from data_flags1"

        df = self.sqlContext.sql(query_Str)
        df.createOrReplaceTempView("data_flags2")

        totl_ = [prod+"_"+str(mes)+"m" for mes in meses for prod in features if prod +
            "_"+str(mes)+"m" in df.columns]
        totl_1 = [totl_[len(features)*i:len(features)*(i+1)]
            for i in range(len(meses))]

        testStr2 = "select * ,"+",".join("("+"+".join(ind) + " ) as "+"total_prod_"+str(
            meses[i]) + "m " for i, ind in enumerate(totl_1))+" from data_flags2"

        df2 = self.sqlContext.sql(testStr2)
        self._save_parquet(df2, self._sibling_path("features"))
