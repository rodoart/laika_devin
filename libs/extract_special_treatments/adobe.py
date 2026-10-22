"""
Project: Special Treatment for Adobe Clickstream
Developer: Devin
Update Date: 2026
Version: 0.1

Tratamiento especial del extract para las fuentes Adobe
(``adobe_bancanet`` / ``adobe_mobile``).

Resuelve la identidad del cliente — la tabla cruda de Adobe Analytics no
trae ``numcliente``, solo ``post_evar5`` (hash) — y aplica los filtros de
página que no caben en el WHERE de fecha del extract:

1. Filtro de páginas: para cada grupo ``clickstream_filters`` del config
   (``extract.adobe``) se construye ``TRIM(<key>) LIKE '<prefix>%' OR ...``
   y los grupos se unen con OR (misma semántica que el datapull original
   de ``adobe_source``).
2. ``session_id = post_visid_high-post_visid_low-visit_num``.
3. Backfill de ``post_evar5``: ``first(ignorenulls)`` sobre la ventana de
   sesión ordenada por ``date_time`` — propaga el hash a los hits de la
   sesión que lo traen vacío (login a mitad de sesión).
4. Resolución ``post_evar5`` -> ``numcliente`` contra la tabla hash
   (``hash_id_db``) **filtrada por fecha**: solo snapshots
   ``proc_dt < <primer día del mes del lote>`` para no usar información
   del futuro. Doble join (``numcliente_hash`` y ``numcliente_salti``) y
   ``coalesce`` como en el pipeline original.

El cálculo de flags de producto y tiempos de sesión (``T_*``) NO va aquí —
va en ``AdobeAggregate`` (etapa groupby), como manda la convención del
datapull: el special treatment se usa solo cuando no hay otra opción (el
join con el pivot necesita ``numcliente``).

Config esperada (``configuration.json`` -> ``<source>.extract.adobe``)::

    "adobe": {
        "hash_id_db": "db.tabla_hash",
        "clickstream_filters": [
            {"key": "pagename",     "page_names": ["MX|BNP3|..."]},
            {"key": "post_evar25",  "page_names": ["MX|BNP|..."]},
            {"key": "post_clickmaplinkbyregion", "page_names": ["Iniciar sesi"]}
        ],
        "event_filter": "",
        "use_event_filter": false,
        "keep_columns": ["numcliente", "session_id", "file_date", "date_time", ...]
    }
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from typing import List, Optional

from .default import ExtractSpecialTreatment


class AdobeExtractSpecialTreatment(ExtractSpecialTreatment):
    """Tratamiento de extract para fuentes Adobe Clickstream.

    Attributes
    ----------
    _adobe_cfg : dict
        ``sources_cat["adobe"]`` — filtros, tabla hash y columnas de salida.
    """

    _evar5 = "post_evar5"
    _evar5_filled = "first_non_empty_post_evar5"

    def __init__(
        self,
        parent: "Extract",
        input_df: DataFrame,
        table_name: str,
        date: str,
        query: str,
        sqlContext: Optional[SparkSession] = None
    ) -> None:
        super().__init__(parent, input_df, table_name, date, query, sqlContext)
        self._adobe_cfg = self.parent.sources_cat.get("adobe", {})
        assert self._adobe_cfg.get("hash_id_db"), \
            "adobe special treatment requires extract.adobe.hash_id_db"

    # ------------------------------------------------------------------
    # Filtros de página
    # ------------------------------------------------------------------
    def _pagename_filters(self) -> List[str]:
        """Cláusulas OR por grupo de ``clickstream_filters``.

        Cada grupo es ``TRIM(<key>) LIKE '<p1>%' OR TRIM(<key>) LIKE '<p2>%' ...``;
        un grupo vacío (``page_names`` vacío) equivale a ``1=1`` como en el
        pipeline original.
        """
        clauses = []
        for group in self._adobe_cfg.get("clickstream_filters", []):
            key = group["key"]
            page_names = group.get("page_names", [])
            if page_names:
                # UPPER en ambos lados: el select puede normalizar la
                # columna (mobile hace UPPER(TRIM(pagename))) y los
                # patrones del config conservan el case del código original
                conds = " OR ".join(
                    f"UPPER(TRIM({key})) LIKE '{prefix.upper()}%'"
                    for prefix in page_names)
                clause = f"({conds})"
            else:
                clause = "(1=1)"
            event_filter = self._adobe_cfg.get("event_filter", "")
            if self._adobe_cfg.get("use_event_filter") and event_filter:
                clause = f"{clause} AND ({event_filter})"
            clauses.append(clause)
        return clauses

    # ------------------------------------------------------------------
    # Identidad
    # ------------------------------------------------------------------
    def _hash_db(self) -> DataFrame:
        """Tabla hash filtrada a snapshots **anteriores** al mes del lote.

        ``self.date`` es el primer día del mes extraído; usar
        ``proc_dt < date`` garantiza que ningún vintage reciba identidades
        creadas después del mes del evento (sin fuga de información).
        """
        hash_id_db = self._adobe_cfg["hash_id_db"]
        max_proc_dt = self.sqlContext.sql(
            f"SELECT MAX(proc_dt) AS p FROM {hash_id_db} "
            f"WHERE proc_dt < '{self.date}'").first()["p"]
        return self.sqlContext.sql(
            f"SELECT numcliente, numcliente_hash, numcliente_salti "
            f"FROM {hash_id_db} WHERE proc_dt = '{max_proc_dt}'")\
            .select(
                F.col("numcliente").cast("string").alias("numcliente"),
                F.trim(F.upper(F.col("numcliente_hash"))).alias("numcliente_hash"),
                F.trim(F.upper(F.col("numcliente_salti"))).alias("numcliente_salti"))\
            .dropDuplicates(["numcliente_hash", "numcliente_salti"])

    def _resolve_numcliente(self, df: DataFrame, hash_df: DataFrame) -> DataFrame:
        """Doble join hash/salti + coalesce -> ``numcliente`` (fiel al
        pipeline original de adobe_source)."""
        return df\
            .join(hash_df.drop("numcliente_salti"), on="numcliente_hash",
                  how="left")\
            .withColumnRenamed("numcliente", "numcliente_by_hash")\
            .join(hash_df.drop("numcliente_hash"), on="numcliente_salti",
                  how="left")\
            .withColumnRenamed("numcliente", "numcliente_by_salti")\
            .withColumn("numcliente", F.when(
                F.col("numcliente_by_salti").isNotNull(),
                F.col("numcliente_by_salti")).otherwise(F.col("numcliente_by_hash")))

    # ------------------------------------------------------------------
    def _special_treatment(self, df: DataFrame) -> DataFrame:
        """Filtros de página + session_id + backfill evar5 + numcliente."""
        # 1. filtro de páginas (grupos OR)
        clauses = self._pagename_filters()
        if clauses:
            df = df.filter("(" + ") OR (".join(clauses) + ")")

        # 2. sesión y backfill de post_evar5
        df = df.withColumn(
            "session_id",
            F.concat(F.col("post_visid_high"), F.lit("-"),
                     F.col("post_visid_low"), F.lit("-"), F.col("visit_num")))
        # frame = toda la sesión: el hit con evar5 puede venir DESPUÉS
        # (login a mitad de sesión) -> first() sobre [preceding..following]
        session_window = Window.partitionBy("session_id")\
            .orderBy("date_time").rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing)
        # "" no es null: hay que excluirlo explícitamente del first() o el
        # backfill propaga el propio vacío
        df = df.withColumn(
            self._evar5_filled,
            F.first(F.when(F.col(self._evar5) != "",
                           F.col(self._evar5)),
                    ignorenulls=True).over(session_window))
        df = df.withColumn(
            self._evar5,
            F.when((F.col(self._evar5) == "") | F.col(self._evar5).isNull(),
                   F.col(self._evar5_filled))
             .otherwise(F.col(self._evar5)))

        # 3. resolución hash -> numcliente (sin fuga: snapshot previo al mes)
        df = df.withColumn("numcliente_hash",
                           F.trim(F.upper(F.col(self._evar5))))
        df = df.withColumn("numcliente_salti", F.col("numcliente_hash"))
        df = df.filter(F.col("numcliente_hash").isNotNull()
                       & (F.col("numcliente_hash") != ""))
        df = self._resolve_numcliente(df, self._hash_db())
        df = df.filter(F.col("numcliente").isNotNull())

        # 4. columnas de salida (numcliente + insumos del aggregate)
        keep = self._adobe_cfg.get("keep_columns")
        if keep:
            df = df.select(*keep)
        return df
