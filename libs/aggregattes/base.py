"""
Clase base compartida por los agregadores del ``groupby``.

Concentra el constructor estándar y los helpers que antes se repetían
verbatim en cada ``*Aggregate``: lectura de la tabla de entrada,
escritura de parquets y la ruta "hermana" ``groupby``/``pivot``/
``features`` del ``previous_path``.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when

from ..steps import save_parquet


def parse_config_bool(value: Any, name: str = "value",
                      none_is_false: bool = False) -> bool:
    """Parsea un booleano de config que puede llegar como ``True``,
    ``"True"``, ``"true"``, ``False`` o ``"none"``.

    Parameters
    ----------
    value:
        Valor crudo del JSON (``bool`` o ``str``).
    name:
        Nombre de la clave, para el ``ValueError``.
    none_is_false:
        ``True`` si ``"none"`` también cuenta como ``False``
        (usado por ``_RenameStep`` de payroll_streaks).
    """
    v = str(value).lower()
    if v == "true":
        return True
    if v == "false" or (none_is_false and v == "none"):
        return False
    raise ValueError(f"The {name} value should be a boolean.")


def columns_with_prefix(columns: List[str], all_prefixes: List[str],
                        protected: List[str],
                        prefix: Optional[str] = None) -> List[str]:
    """Columnas de ``columns`` que empiezan por ``prefix`` (o las que no
    empiezan por ningún prefijo conocido si ``prefix`` es None/"none"/"").

    Excluye siempre las columnas ``protected`` (p. ej. claves que no deben
    renombrarse). Lógica compartida por los steps de payroll_income y
    payroll_streaks.
    """
    if prefix is None or str(prefix).lower() == "none" or prefix == "":
        return [c for c in columns
                if c not in protected
                and not any(c.startswith(p) for p in all_prefixes)]
    return [c for c in columns
            if c.startswith(prefix) and c not in protected]


def columns_with_prefixes(df: DataFrame, all_prefixes: List[str],
                          protected: List[str],
                          prefixes: List[str]) -> List[str]:
    """Unión (sin duplicados) de :func:`columns_with_prefix` para cada
    prefijo de ``prefixes``."""
    result: List[str] = []
    for prefix in prefixes:
        result += columns_with_prefix(
            df.columns, all_prefixes, protected, prefix)
    return list(set(result))


def sort_df_columns(df: DataFrame, protected: List[str]) -> DataFrame:
    """Reordena el DataFrame: primero ``protected`` y luego el resto de
    columnas ordenadas alfabéticamente."""
    rest = sorted(c for c in df.columns if c not in protected)
    return df.select(protected + rest)


class BaseAggregate:
    """Constructor y utilidades comunes de los ``*Aggregate``.

    Parameters
    ----------
    previous_path : str
        Path del paso anterior (entrada del agregado).
    agg_dict : Dict[str, Any]
        Parámetros de la fuente (``table_name``, ``group_by_key``,
        ``query_dict``…).
    sqlContext : SparkSession
        Sesión de Spark.
    mode : str, optional
        Modo de escritura del parquet, por defecto ``'overwrite'``.
    """

    def __init__(self, previous_path: str, agg_dict: Dict[str, Any],
                 sqlContext: SparkSession, mode: str = "overwrite") -> None:
        self.previous_path = previous_path
        self.agg_dict = agg_dict
        self.sqlContext = sqlContext
        self.mode = mode

    def _table_path(self, *keys: str) -> str:
        """``previous_path/agg_dict[k1][k2]...`` — ruta de la tabla de
        entrada. Soporta claves anidadas (p. ej. ``'table_names',
        'baseline'``)."""
        table: Any = self.agg_dict
        for key in keys or ("table_name",):
            table = table[key]
        return os.path.join(self.previous_path, table)

    def _read_table(self, *keys: str) -> DataFrame:
        """Lee ``previous_path/agg_dict[keys...]`` como DataFrame."""
        return self.sqlContext.read.parquet(self._table_path(*keys))

    def _sibling_path(self, *parts: str) -> str:
        """Ruta dentro del directorio padre de ``previous_path``
        (equivalente al viejo ``os.path.join(head, ...)``)."""
        head, _ = os.path.split(self.previous_path)
        return os.path.join(head, *parts)

    @staticmethod
    def _nested_when(pairs: Any, default: Any = -99999) -> "Any":
        """Construye ``when(c1,v1).when(c2,v2)....otherwise(default)``
        a partir de pares ``(condicion, valor)``."""
        pairs = list(pairs)
        expr = when(pairs[0][0], pairs[0][1])
        for cond, val in pairs[1:]:
            expr = expr.when(cond, val)
        return expr.otherwise(default)

    def _save_parquet(self, df_save: DataFrame, path: str) -> None:
        """Guarda el DataFrame como parquet con ``self.mode``,
        particionado por vintage/tfrom si aplica."""
        save_parquet(df_save, path, self.mode)

    def _create_groupby_query(self, query_dict: Dict[str, Any],
                              group_by: List[str],
                              filter_columns: Optional[List[str]] = None,
                              tempview: str = "tempPnl") -> str:
        """Query ``SELECT <keys>,<agg>(<var>) as <var> ... GROUP BY``.

        Estilo "estándar" usado por los agregadores PNL: cada fila de
        ``query_dict`` produce ``{agg}({variable}) as {variable}``. Si
        ``filter_columns`` se pasa, solo se agregan las variables
        presentes en el DataFrame de entrada.
        """
        agg_df = pd.DataFrame(data=query_dict)
        agg_df = agg_df[agg_df["variable"] != ""]
        features = ",".join(
            "{0}({1}) as {1}".format(item["agg"], item["variable"])
            for _, item in agg_df.iterrows()
            if filter_columns is None or item["variable"] in filter_columns)
        group_str = ",".join(group_by)
        return ("SELECT " + group_str + "," + features +
                " FROM " + tempview + " GROUP BY " + group_str)

    def getCostumerBureau(self, df: DataFrame, aggs: Dict[str, Any],
                          sqlContext: SparkSession) -> DataFrame:
        """Calcula las variables nuevas de bureau (hit, mob, flags de
        missing y relleno) encadenando vistas temporales SQL.

        Compartido por ``BureauPullAggregate`` y ``BureauCohortAggregate``.
        """
        group_by = ["numcliente", "vintage_date"]
        tfrom = "tfrom"
        timeWindow = 6
        groupBy = ",".join(group_by)

        aux_features = [item for item in df.columns
                        if item not in ["numcliente", "vintage_date", "tfrom"]]
        logging.debug("Customer Bureau Aux Columns: %s", aux_features)
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
        dfBureau.createOrReplaceTempView("dfTempMiss")
        fmiss = "case when (hit_adv=1) and (a5 is null) and mob>tfrom then 1 else 0 end as fmiss"
        flagNews = "case when (hit_adv=1) and mob<=12 then 1 else 0 end as flag_new"
        cumulative_miss = "sum((case when a5 is null then 1 else 0 end)) over (partition by {0} order by tfrom rows between 12 preceding and current row ) as cumulative".format(
        groupBy)
        flag = ",".join([fmiss, flagNews, cumulative_miss])
        query = "select *,{0} from dfTempMiss order by {1}".format(
            flag, groupBy)
        dfBureauMiss = sqlContext.sql(query)
        dfBureauMiss.createOrReplaceTempView("dFlags")
        flag_6m = "case when max(cumulative*(case when tfrom<={0} then 1 else 0 end)) over (partition by {1})>={2} then 1 else 0 end as flagMissing".format(
        timeWindow-1, groupBy, timeWindow)
        flag_7m = "case when max(cumulative) over (partition by {0}) >=7 then 1 else 0 end as flag_cum".format(
        groupBy)
        fillNew = "case when (hit_adv=0) or ((flag_new=1) and (fmiss=0) and (a5 is null)) then 1 else 0 end as fill0"
        flag = ",".join([flag_6m, flag_7m, fillNew])
        query = "select *,{0} from dFlags order by {1}".format(flag, groupBy)
        dfBureauMiss2 = sqlContext.sql(query)
        dfBureauMiss2.createOrReplaceTempView("dfFill")
        hitBureau = "case when (hit_adv=1) and (flagMissing=0) and (flag_cum=0) and (flag_new=0) then 1 else 0 end as hitBureau"
        fill = ",".join(["case when fill0=1 then 0 else {0} end as {0}".format(
        item) for item in aux_features])
        flag = ",".join([groupBy, tfrom, hitBureau, fill])
        query = "select {0} from dfFill order by {1}".format(flag, groupBy)
        dfBureauMiss3 = sqlContext.sql(query)
        return dfBureauMiss3
