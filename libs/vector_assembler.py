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
import subprocess
from pyspark.ml.feature import VectorAssembler
from typing import Any, Dict, List, Tuple
import os
import re
import logging

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
                 id_column: str = "id_msf_tools_v2_12315") -> None:
        self.path_features: str = path_features
        self.save_path: str = save_path
        self.sqlContext: SparkSession = sqlContext
        self.raw_name: str = raw_name
        self.transformations: List[str] = self._check_files(path_features)
        self.transformations.remove(raw_name)
        if "assembled_features" in self.transformations:
            self.transformations.remove("assembled_features")
        self.mode: str = mode
        self.id_column: str = id_column

    def _check_files(self, path: str) -> List[str]:
        """Lista los ficheros/directorios de ``path`` en Hadoop.

        En ejecuciones locales se sustituye por
        ``libs.steps.path_listdir`` (ver ``patch_filesystem``).

        Parameters
        ----------
        path : str
            Path a listar.

        Returns
        -------
        List[str]
            Lista con todos los ficheros del path.
        """
        args_list=['hdfs', 'dfs', '-ls', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        proc.wait()
        return [elem[re.search(path, elem).end():].replace("/","") for elem in stdout.decode('ascii','ignore').splitlines() if path
        in elem]

    def _evaluate_path(self, path: str) -> bool:
        """Evalúa si ``path`` existe en Hadoop.

        Parameters
        ----------
        path : str
            Path a comprobar.

        Returns
        -------
        bool
            ``True`` si el path existe; ``False`` si no.
        """
        args_list=['hdfs', 'dfs', '-test', '-d', path]
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        proc.communicate()
        value=proc.returncode==0 and True or False
        return value

    def _delete_intermediate(self, path: str) -> None:
        """Borra ``path`` con ``hdfs dfs -rm`` si existe.

        Parameters
        ----------
        path : str
            Path a borrar.
        """
        if (self._evaluate_path(path)):
            args_list = ['hdfs', 'dfs', '-rm', '-f', '-r', '-skipTrash', path]
            proc = subprocess.Popen(
                args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.communicate()

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
            features_path = os.path.join(self.path_features,feature)
            if self._evaluate_path(features_path):
                df_fts = self.sqlContext.read.parquet(features_path)
                skip = id_column
                colz_aux = [x for x in df_fts.columns if x not in skip]
                colz.append(colz_aux)
                assembler = VectorAssembler(inputCols = colz_aux, outputCol=feature)
                df_assemb = assembler.transform(df_fts).selectExpr(id_column + " as id_msf_" + feature, feature)
                features_to_process.append((features_path, feature, df_assemb))

        new_meta_dict = self._create_new_meta(colz)
        raw_path = os.path.join(self.path_features, self.raw_name)
        if self._evaluate_path(raw_path):
            pivot = self.sqlContext.read.parquet(raw_path).select(id_column)
            cont = 0
            for feature in features_to_process:
                df_join = feature[2]
                if cont == 0:
                    all_features_df = pivot.alias('a').join(df_join.alias('b'), col("a." + id_column) == col("b.id_msf_" + feature[1]), how='left')
                    all_features_df = all_features_df.drop("id_msf_" + feature[1])
                else:
                    all_features_df = all_features_df.alias('a').join(df_join.alias('b'), col("a." + id_column) == col("b.id_msf_" + feature[1]), how='left')
                    all_features_df = all_features_df.drop("id_msf_" + feature[1])
            cont += 1
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
        final_features_df.write.mode(self.mode).parquet(os.path.join(self.save_path + "_aux"))
        final_features_df = self.sqlContext.read.parquet(os.path.join(self.save_path + "_aux"))
        raw_features = self.sqlContext.read.parquet(raw_path)
        raw_columns = [column for column in raw_features.columns if not column[-1].isdigit()] + [self.id_column]
        #print("RAW: ", raw_columns)
        logging.debug("Raw Columns for Vector Asembler: %s", raw_columns)
        raw_features = raw_features.select(*raw_columns)
        va_final = raw_features.join(final_features_df, on = [self.id_column]).drop(self.id_column)
        va_final = va_final.withColumnRenamed("vintage_date","vintage")
        va_final.write.mode(self.mode).parquet(os.path.join(self.save_path))
        self._delete_intermediate(os.path.join(self.save_path + "_aux"))
        return()

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
