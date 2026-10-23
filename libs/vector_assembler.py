"""
Project: Data Pull Framework
Developer: @rm36973

``FeaturesVectorAssembler`` — crea el vector final de features del
datapull: ensambla cada transformación de ``features/`` en su propio
vector, las une al pivot de raw features y reescribe los metadatos ML
para que cada slot del vector conserve el nombre de su feature.
"""
from pyspark.sql.functions import col
from pyspark.sql import DataFrame, SparkSession
from pyspark.ml.feature import VectorAssembler
from typing import Any, Dict, List, Optional, Tuple
import os
import re
import logging

from .steps import path_listdir, path_delete, save_parquet
from .hive_path import is_successful

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


class FeaturesVectorAssembler:
    """Feature Vector Assembler para crear el vector final de features.

    Parameters
    ----------
    path_features : str
        Path donde están las features, incluida la raw.
    save_path : str
        Path donde guardar el vector assembler.
    sqlContext : SparkSession
        Tu sqlContext.
    raw_name : str
        Opcional, nombre de la carpeta de raw features; por defecto
        ``raw_features``.
    mode : str
        Opcional, modo de escritura del parquet; por defecto
        ``overwrite``.
    id_column : str
        Opcional, nombre de la columna id en las features.
    """
    def __init__(self, path_features: str, save_path: str,
                 sqlContext: SparkSession, raw_name: str = "raw_features",
                 mode: str = "overwrite",
                 id_column: str = "id_msf_tools_v2_12315",
                 fallback_path: Optional[str] = None) -> None:
        self.path_features: str = path_features
        # ``imputate_mobs/_passthrough/<feat>`` declara features que la
        # etapa mobs dejó sin copiar: el VA las lee de ``fallback_path``
        # (el dir de features). Si no hay marcadores, el comportamiento
        # es idéntico al de siempre.
        self.fallback_path: Optional[str] = fallback_path
        self._passthrough: List[str] = sorted(self._check_files(
            os.path.join(path_features, "_passthrough")))
        self.save_path: str = save_path
        self.sqlContext: SparkSession = sqlContext
        self.raw_name: str = raw_name
        # orden determinista: el orden de listdir (HDFS/FS local) no lo es,
        # y de él depende el orden de las columnas del vector ensamblado
        self.transformations: List[str] = sorted(
            self._check_files(path_features) + self._passthrough)
        self.transformations.remove(raw_name)
        for extra in ("assembled_features", "_passthrough"):
            if extra in self.transformations:
                self.transformations.remove(extra)
        self.mode: str = mode
        self.id_column: str = id_column

    def _check_files(self, path: str) -> List[str]:
        """Lista los ficheros/directorios de ``path``.

        Delega en :func:`libs.steps.path_listdir` (CLI ``hdfs`` en el
        clúster, FS local si la ruta no parece HDFS).
        """
        return path_listdir(path)

    def _evaluate_path(self, path: str) -> bool:
        """Evalúa si ``path`` es un parquet terminado (dir + ``_SUCCESS``).

        Una escritura matada a medias no cuenta: un dir corrupto no se
        lee — se resuelve al ``fallback_path`` si existe.
        """
        return is_successful(path)

    def _delete_intermediate(self, path: str) -> None:
        """Borra ``path`` si existe (ver :func:`libs.steps.path_delete`)."""
        path_delete(path)

    def _features_vec_assemb(self) -> Tuple[()]:
        """Crea el vector final con ``VectorAssembler`` de Spark.

        Para cada transformación de ``self.transformations``:

        1. Lee ``<path_features>/<trans>`` y la ensambla en un vector.
        2. Hace join de todos los vectores sobre el pivot de raw features.
        3. Ensambla todo en ``features_vector`` y reescribe los metadatos
           ``ml_attr`` con los nombres reales de las features.
        4. Guarda ``<save_path>`` uniendo las columnas raw (no features)
           con el vector final.
        """
        features_to_process = []
        colz = []
        id_column = self.id_column
        for feature in self.transformations:
            features_path = self._resolve(feature)
            if features_path is not None:
                df_fts = self.sqlContext.read.parquet(features_path)
                skip = id_column
                colz_aux = [x for x in df_fts.columns if x not in skip]
                colz.append(colz_aux)
                assembler = VectorAssembler(inputCols = colz_aux, outputCol=feature)
                df_assemb = assembler.transform(df_fts).selectExpr(id_column + " as id_msf_" + feature, feature)
                features_to_process.append((features_path, feature, df_assemb))

        new_meta_dict = self._create_new_meta(colz)
        raw_path = self._resolve(self.raw_name)
        if raw_path is not None:
            all_features_df = self.sqlContext.read.parquet(raw_path).select(id_column)
            for _, feature_name, df_join in features_to_process:
                all_features_df = all_features_df.alias('a')\
                    .join(df_join.alias('b'),
                          col("a." + id_column) == col("b.id_msf_" + feature_name),
                          how='left')\
                    .drop("id_msf_" + feature_name)
        colz_all_assemblers = [x for x in all_features_df.columns if x not in id_column]
        all_assemblers = VectorAssembler(inputCols=colz_all_assemblers, outputCol='features_vector')
        final_features_df = all_assemblers.transform(all_features_df)
        final_features_df = final_features_df.select(id_column, 'features_vector')
        ## Change metadata
        old_metadata = final_features_df.schema["features_vector"].metadata
        new_metadata = old_metadata
        for meta in new_metadata["ml_attr"]["attrs"]["numeric"]:
            id_column = meta['idx']
            meta['name'] = new_meta_dict[id_column]
        final_features_df = final_features_df.withColumn("features_vector", col("features_vector").alias("", metadata = new_metadata))
        save_parquet(final_features_df,
                     os.path.join(self.save_path + "_aux"), self.mode)
        final_features_df = self.sqlContext.read.parquet(os.path.join(self.save_path + "_aux"))
        raw_features = self.sqlContext.read.parquet(raw_path)
        raw_columns = [column for column in raw_features.columns if not column[-1].isdigit()] + [self.id_column]
        #print("RAW: ", raw_columns)
        logging.debug("Raw Columns for Vector Asembler: %s", raw_columns)
        raw_features = raw_features.select(*raw_columns)
        va_final = raw_features.join(final_features_df, on = [self.id_column]).drop(self.id_column)
        va_final = va_final.withColumnRenamed("vintage_date","vintage")
        save_parquet(va_final, os.path.join(self.save_path), self.mode)
        self._delete_intermediate(os.path.join(self.save_path + "_aux"))
        return()

    def _resolve(self, name: str) -> Optional[str]:
        """Resuelve el parquet de una transformación.

        Devuelve ``<path_features>/<name>`` si existe; si no y hay
        ``fallback_path``, ``<fallback_path>/<name>``; si tampoco existe
        allí, ``None``.
        """
        candidate = os.path.join(self.path_features, name)
        if self._evaluate_path(candidate):
            return candidate
        if self.fallback_path is not None:
            candidate = os.path.join(self.fallback_path, name)
            if self._evaluate_path(candidate):
                return candidate
        return None

    def _create_new_meta(self, colz: List[List[str]]) -> Dict[int, str]:
        """Construye el dict ``{idx: nombre_columna}`` para los metadatos ML.

        Parameters
        ----------
        colz : List[List[str]]
            Lista de listas de columnas (una por transformación).

        Returns
        -------
        Dict[int, str]
            Diccionario con la nueva metadata ``idx -> nombre``.
        """
        colz_list = []
        cont = 0
        cont2 = 0
        while cont < len(colz):
            while cont2 < len(colz[cont]):
                colz_list.append(colz[cont][cont2])
                cont2 += 1
            cont += 1
            cont2 = 0
        cont = 0
        dict_colz = {}
        for x in colz_list:
            dict_colz[cont] = x
            cont += 1
        return(dict_colz)

    def run(self) -> None:
        """Ejecuta el vector assembler."""
        self._features_vec_assemb()
