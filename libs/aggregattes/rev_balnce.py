"""
Project: Data Aggregation (PEP 8 & Python3.6)
Developer: @rm36973 @bb25173
Update Date: 2023-01-04
Version: 0.1

Class and Functions required to aggregate (general)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, col, broadcast, month, when, lit, round
from pyspark.sql.types import DoubleType
from typing import Any, Dict
from .base import BaseAggregate

class RevBalanceAggregate(BaseAggregate):
    """
    Class to aggregate Revolving Balance Data

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

    def epp_aggregate(self, sqlContext: SparkSession, epp_path: str, agg_dict: Dict[str, Any], epp_dict: Dict[str, Any]) -> DataFrame:
        """
        Agregates EPP Data.

        Parameters
        ----------
        sqlContext: Spark context
            Context working in
        epp_path: string
            Path to read EPP Data From
        aggs: Dictionary
            All parameters needed in the aggregation process

        Returns
        -------
        Spark Dataframe
            Dataframe with aggregated data.
        """
        import pyspark.sql.functions as fn
        promoID = sqlContext.sql(
            "select * from {0}".format(agg_dict['table_names']['catPromosID']))
        promoID = promoID.select(
            fn.col("promo_id").alias("promoId"),
            fn.col("grupo").alias("oferta"))

        epp_df = sqlContext.read.parquet(
            epp_path).withColumnRenamed("proc_dt", "mis_dt")
        epp_df = epp_df.filter("estatus = 1")
        epp_df = epp_df.join(broadcast(promoID), on=[
            epp_df.promo_id == promoID.promoId], how="left_outer")
        epp_df = epp_df.withColumn('plazo', epp_df['plazo'].cast(DoubleType()))
        epp_df = epp_df.withColumn(
            'month', month(epp_df['mis_dt']).alias('month'))

        epp_df = epp_df.withColumn('oferta',
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:1] == '7'), 'T0').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:2] == '03'), 'T0').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:2] == 'IT'), 'T0').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:5] == '4DBAL'), 'Balcon').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:3] == '4DB'), 'Disponible').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:2] == '5E'), 'Efectivo').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:2] == '2P'), 'PPP').
            when((epp_df['oferta'].isNull()) & (epp_df['promo_id'][0:2] == '6R'), 'RES').
            otherwise(epp_df['oferta']))

        tipo_ofertas = ['Balcon', 'Disponible', 'Efectivo', 'PPP', 'T0', 'RES']
        sel_cols = ['original_balance', 'current_balance',
            'tasa', 'plazo', 'plazo_actual']
        list_new_cols = []

        for oferta in tipo_ofertas:
            for col in sel_cols:
                new_col_name = col + '_' + oferta
                list_new_cols.append(new_col_name)
                epp_df = epp_df.withColumn(new_col_name, when(
                    epp_df['oferta'] == oferta, epp_df[col]).otherwise(None))

        # filter aggs
        agg_cols_sum = [c for c in list_new_cols if (
            'original_balance' in c) | ('current_balance' in c)]
        agg_cols_avg = [c for c in list_new_cols if (
            'tasa' in c) | ('plazo' in c) | ('plazo_actual' in c)]
        # Create aggs
        expr_sum = [fn.sum(epp_df[c]).alias("EPP_"+c) for c in agg_cols_sum]
        expr_avg = [fn.avg(epp_df[c]).alias("EPP_"+c) for c in agg_cols_avg]
        others = [fn.max('Fecha_Aceptacion').alias("EPP_MAX_of_Fecha_Aceptacion"), fn.min('Fecha_Aceptacion').alias(
            "EPP_MIN_of_Fecha_Aceptacion"), fn.avg('Tasa').alias("EPP_AVG_of_Tasa_Total"), fn.avg('plazo').alias(
            "EPP_AVG_of_Plazo_Total")]
        expr_agg = expr_sum + expr_avg + others
        epp_agg = epp_df.groupBy(
            ["crd_acct_nbr", "vintage_date", "tfrom"]).agg(*expr_agg)

        return epp_agg

    def plan_aggregate(self, sqlContext: SparkSession, plan_path: str) -> DataFrame:
        """
        Aggregates plan data.

        Parameters
        ----------
        sqlContext: Spark context
            Context working in
        plan_path: string
            Path to read Plan Data From

        Returns
        -------
        Spark Dataframe
            Dataframe with aggregated data.
        """
        import pyspark.sql.functions as fn
        plan_df = sqlContext.read.parquet(plan_path)
        product = {'Compras': col('plan_nbr')[0:2] == '10',
            'TMAX': col('plan_nbr')[0:2].isin(['11', '21']),
            'Cash': col('plan_nbr')[0:2] == '20',
            'T0': col('plan_nbr') == '90400',
            'Efec': col('plan_nbr').isin(['91001', '91000']),
            'DB': col('plan_nbr').isin(['90000', '90001']),
            'PPP': col('plan_nbr') == '90100',
            'Rewrites': col('plan_nbr') == '91200'
        }

        var = ['current_balance', 'int_grav_exent', 'tasa']
        for itemProduct in product:
            for itemVar in var:
                plan_df = plan_df.withColumn(
                    itemVar+'_'+itemProduct, when(product[itemProduct], col(itemVar)).otherwise(lit(0)))

        agg_cols_sum = [item+'_'+itemProduct for item in var[0:2]
            for itemProduct in product]
        agg_cols_avg = [var[2]+'_'+itemProduct for itemProduct in product]
        # Create aggs
        expr_sum = [fn.sum(col(c)).alias("Plan_"+c) for c in agg_cols_sum]
        expr_avg = [fn.avg(col(c)).alias("Plan_"+c) for c in agg_cols_avg]
        others = [fn.max('tasa_compras').alias("plan_max_tasa_compras"), fn.max(
            'tasa_cash').alias("plan_max_tasa_cash")]
        expr = expr_sum + expr_avg + others

        groupBy = ["crd_acct_nbr", "vintage_date", "tfrom"]
        dfAgg = plan_df.groupBy(groupBy).agg(*expr)
        newVars = {
            "Total_Balance": "current_balance",
            "Total_Intereses": "int_grav_exent"
        }
        total = ["({1}) as {0}".format(itemNewVar, "+".join(["Plan_{0}_{1}".format(
            newVars[itemNewVar], itemProduct) for itemProduct in product])) for itemNewVar in newVars]
        query = "select *,{0} from dfPlanesTemp".format(",".join(total))
        dfAgg.createOrReplaceTempView("dfPlanesTemp")
        plan_agg = sqlContext.sql(query)

        return plan_agg

    def revbalance_aggregate(self, ccp_path: str, agg_dict: Dict[str, Any], epp_agg: DataFrame, plan_agg: DataFrame) -> DataFrame:
        """
        Joins together the epp and plan data. Credit Card level is aggregated
        to Customer Level.

        Parameters
        ----------
        ccp_path: string
            Path to read CPP Data From
        agg_dict: Dictionary
            All parameters needed by the class(key, group_by, querys)
        epp_agg: Spark Dataframe
            EPP aggregated data
        plan_agg: Spark Dataframe
            Plan aggregated data

        Returns
        -------
        Spark Dataframe
            Dataframe with aggregated data.
        """
        cpp_df = self.sqlContext.read.parquet(ccp_path)
        ccpAgg = cpp_df.select(
            agg_dict['primaryKey']+["eir", "monthsonbooks", "numcliente"])

        pivotHist1 = ccpAgg.join(epp_agg, agg_dict['primaryKey'], how="left")
        pivotHist1 = pivotHist1.join(
            plan_agg, agg_dict['primaryKey'], how="left")

        pivotHist1.createOrReplaceTempView("tempHis")
        groupVariables = agg_dict['group_by_key']
        dropVar = ["crd_acct_nbr"]
        dropVar.extend(groupVariables)
        listFeatures = [
            item for item in pivotHist1.columns if item not in dropVar]
        listFeaturesSum = [item for item in listFeatures if "tasa" not in item.lower(
        ) and "fecha" not in item.lower()]
        listFeaturesMax = [
            item for item in listFeatures if "tasa" in item.lower()]
        querySum = ["sum({0}) as {0}".format(item) for item in listFeaturesSum]
        queryMax = ["max({0}) as {0}".format(item) for item in listFeaturesMax]
        query = ",".join(querySum+queryMax)
        groupBy = ",".join(groupVariables)
        queryAgg = "select {0},{1} from tempHis GROUP BY {0}".format(
            groupBy, query)

        histCliente = self.sqlContext.sql(queryAgg)
        histCliente1 = histCliente.withColumn("total_interes_fin", when(round(col("Total_Intereses"), 6) == round(
            col("eir"), 6), round(col("eir"), 6)).otherwise(col("Total_Intereses")+col("Plan_int_grav_exent_Compras")-round(col("eir"), 6)))
        histCliente1 = histCliente1.withColumn("total_bal_rev", when(
            col("total_interes_fin") > 0, col("Total_Balance")).otherwise(0))
        histCliente2 = histCliente1.withColumn("rev_flag", when(
            col("total_interes_fin") > 0, 1).otherwise(0))
        histCliente2.createOrReplaceTempView("dfCast")
        cast = ",".join(["cast({0} as double)".format(item)
            for item in histCliente2.columns if item not in dropVar])
        query = "select {0},{1} from dfCast".format(groupBy, cast)

        rev_plan_agg = self.sqlContext.sql(query)

        return rev_plan_agg

    def aggregate(self) -> None:
        """
        Process to aggregate Revolving Balance data.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        reading_path_epp = self._table_path('table_names', 'epp')
        epp_dict = self.agg_dict['querys_dict']["epp_dict"]
        epp_agg = self.epp_aggregate(
            self.sqlContext, reading_path_epp, self.agg_dict, epp_dict)

        reading_path_plan = self._table_path('table_names', 'plan')
        plan_agg = self.plan_aggregate(self.sqlContext, reading_path_plan)

        reading_path_cpp = self._table_path('table_names', 'cpp')
        revbal_agg = self.revbalance_aggregate(
            reading_path_cpp, self.agg_dict, epp_agg, plan_agg)
        revbal_agg = revbal_agg.withColumn("has_ccp", when(col("numcliente").isNotNull(),1).otherwise(0))
        # revbal_agg.withColumn("has_ccp", when(col("numcliente").isNotNull(),1).otherwise(0))
        self._save_parquet(revbal_agg, self._sibling_path("groupby"))
