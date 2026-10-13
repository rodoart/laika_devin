"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

import os
import pandas as pd
from pyspark.sql.functions import greatest, expr, col, broadcast, month, when, expr, lit, create_map, lower, array, explode, struct, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from itertools import chain
import logging

logging.basicConfig(format='%(asctime)s | %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class BureauCohortAggregate:
    """
    Class to aggregate Bureau Cohort Data

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

    def __init__(self, previous_path, agg_dict, sqlContext, mode="overwrite") -> None:
        """
        Class constructor.
        """
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _create_groupby_query(self, query_dict, group_by, filter_columns) -> str:
        """
        Creates the SQL aggregation sentence to group the Bureau Cohort data

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
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = (','.join(['{0}(case when {1}> 0 then {1} else 0 end ) as {2}'.
                    format(item['agg'], item['variable'], item['feature'])
                    for _, item in agg_df.iterrows() if item['variable'] in filter_columns]))
        group_str = ",".join(group_by)
        query_str = "SELECT "+group_str+","+features + \
            " FROM tempBureau GROUP BY "+group_str
        return query_str

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

    def getCostumerBureau(self, df, aggs, sqlContext) -> Any:
        """
        Calculate new bureau variables.

        Parameters
        ----------
        df: Spark Dataframe
            Data to work with
        aggs: Dictionary
            All parameters needed by the class(key, group_by, querys)
        sqlContext: Spark context
            Context working with

        Returns
        -------
        Spark Dataframe
            Dataframe with new calculated columns
        """
        group_by = ["numcliente", "vintage_date"]
        tfrom = "tfrom"
        timeWindow = 6
        groupBy = ",".join(group_by)

        aux_features = [item for item in df.columns if item not in ["numcliente", "vintage_date","tfrom"]]
        # Get Months on Bureau and Hit bureau
        df.createOrReplaceTempView("dfTempFlag")
        mob = "max(a5+{0}) over (partition by {1}) as mob".format(tfrom, groupBy)
        hit_adv = "case when sum((case when a5 is null then 0 else 1 end)) over (partition by {0})>0 then 1 else 0 end as hit_adv ".format(
        groupBy)
        nmiss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0}) as nmiss".format(
        groupBy)
        flags = ",".join([mob, hit_adv, nmiss])
        query = "select *,{0} from dfTempFlag order by {1} ".format(
            flags, groupBy)
        dfBureau = sqlContext.sql(query)
        # Get flags missings
        dfBureau.createOrReplaceTempView("dfTempMiss")
        fmiss = "case when (hit_adv=1) and (a5 is null) and mob>tfrom then 1 else 0 end as fmiss"
        flagNews = "case when (hit_adv=1) and mob<=12 then 1 else 0 end as flag_new"
        cumulative_miss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0} order by tfrom rows between 12 preceding and current row ) as cumulative".format(
        groupBy)
        flag = ",".join([fmiss, flagNews, cumulative_miss])
        query = "select *,{0} from dfTempMiss order by {1}".format(
            flag, groupBy)
        dfBureauMiss = sqlContext.sql(query)
        # get flag missing
        dfBureauMiss.createOrReplaceTempView("dFlags")
        flag_6m = "case when max(cumulative*(case when tfrom<={0} then 1 else 0 end)) over (partition by {1})>={2} then 1 else 0 end as flagMissing".format(
        timeWindow-1, groupBy, timeWindow)
        flag_7m = "case when max(cumulative) over (partition by {0}) >=7 then 1 else 0 end as flag_cum".format(
        groupBy)
        cumulativef = ",".join(["case when sum(case when cumulative<={0} then 1 else 0 end) over (partition by {1})>0 then 1 else 0 end as f{0}".format(
        item, groupBy) for item in range(12)])
        fillNew = "case when (hit_adv=0) or ((flag_new=1) and (fmiss=0) and (a5 is null)) then 1 else 0 end as fill0"
        flag = ",".join([flag_6m, flag_7m, fillNew])
        query = "select *,{0} from dFlags order by {1}".format(flag, groupBy)
        dfBureauMiss2 = sqlContext.sql(query)
        # Fill sistem
        dfBureauMiss2.createOrReplaceTempView("dfFill")
        hitBureau = "case when (hit_adv=1) and (flagMissing=0) and (flag_cum=0) and (flag_new=0) then 1 else 0 end as hitBureau"
        fill = ",".join(["case when fill0=1 then 0 else {0} end as {0}".format(
        item) for item in aux_features])
        flag = ",".join([groupBy, tfrom, hitBureau, fill])
        query = "select {0} from dfFill order by {1}".format(flag, groupBy)
        dfBureauMiss3 = sqlContext.sql(query)
        return dfBureauMiss3

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

        reading_path = os.path.join(
            self.previous_path, self.agg_dict['table_name'])
        df_in = self.sqlContext.read.parquet(reading_path)
        df_in.createOrReplaceTempView("tempBureau")
        filter_columns = df_in.columns
        query_str = self._create_groupby_query(
            self.agg_dict['query_dict'], self.agg_dict['group_by_key'], filter_columns)
        df_out = self.sqlContext.sql(query_str)
        df_gcb = self.getCostumerBureau(
            df_out, self.agg_dict['query_dict'], self.sqlContext)
        df_gcb_fin = df_gcb.where("tfrom<=2")
        head, _ = os.path.split(self.previous_path)
        self._save_parquet(df_gcb_fin, os.path.join(
            head, "groupby", self.agg_dict['table_name']))
