"""
``datapull.py`` — clase principal :class:`DataPull` del proceso datapull
del equipo COE.

Orquesta el pipeline completo de una fuente: ``extract`` -> ``weeks`` ->
``mt`` -> ``groupby`` -> ``pivoting`` -> ``ratios`` -> ``fe`` -> ``mobs``
-> ``va`` -> ``cols2vec``, según el ``steps_list`` de
``configuration.json``. Cada etapa queda guardada como parquet bajo
``<working_path>/<year_id>/<source>/<stage>`` y las comprobaciones
``_check_*`` hacen el pipeline idempotente.

Notas
-----
- ``reset_spark_session`` decora los ``_perform_*`` pesados: para y
  recrea la sesión Spark entre etapas (evita que un driver saturado
  arrastre fallos a la siguiente etapa).
- Los helpers de filesystem (``_check_files``, ``_evaluate_path``)
  shell-out a ``hdfs dfs``; en ejecuciones locales se sustituyen por los
  helpers duales de :mod:`libs.steps` (ver ``patch_datapull_fs`` en
  ``main_dynamic.py``).
"""
from pyspark import conf
from libs.email_sender import EmailSender
from libs.extract import Extract
from libs.weeks import WeeklyTFrom
from libs.feature_engineering import FeatureEngineering
from libs.vector_assembler import FeaturesVectorAssembler
from libs.missing_treatment import MissingTreatment
from libs.group_by import GroupBy
from libs.generate_pivot import GeneratePivot
from libs.generate_ratios import GenerateRatios
from libs.colums_2_vector import ColumnsToVector
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame, SparkSession

import os
import subprocess
import re
import pandas as pd
import traceback
from datetime import datetime
from dateutil.relativedelta import relativedelta
import unidecode
import time
import logging

from typing import Any, Callable, Dict, List, Optional, Tuple

logging.basicConfig(format='%(asctime)s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S%p', level=logging.INFO)


def reset_spark_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorador que resetea la sesión de Spark antes de ejecutar la función.

    Para la sesión activa (si la hay), crea una nueva con
    ``self.spark_session_builder()`` y re-registra la UDF
    ``quita_acentos``. Así cada ``_perform_*`` corre sobre una sesión
    limpia.
    """
    def wrapper(self, *args, **kwargs) -> Any:
        # Stop the existing session if active
        try:
            self._sqlContext.stop()
        except:
            pass
        finally:
            # Remove reference to the old session
            self._sqlContext = None
        #
        # Create a new session
        self._sqlContext = self.spark_session_builder()
        self._quita_acentos()
        # Execute the decorated function
        return func(self, *args, **kwargs)
    return wrapper

#

class DataPull():
    """Clase principal del proceso datapull del equipo COE.

    Parameters
    ----------
    config_rigid : Dict[str, Any]
        Configuraciones por defecto del equipo de ingeniería de datos de
        COE Advance Analytics (``configuration.json``).
    config_custom : Dict[str, Any]
        Configuraciones custom definidas por el usuario final
        (``config_custom.json``).
    spark_session_builder : Callable[[], SparkSession]
        Función que genera el ``sqlContext`` del script.
    """
    message_start_controls_stract = """
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<h1> Input controls: </h1>"""
    message_start_finals = """
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<h1> Times of execution: </h1>"""
    message_style = """
<style type="text/css" media="screen">
    #default {
        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
        font-size: 12px;
        border: 3px solid #FCCD00 ;
        }

    #default th {
        padding-top: 4px;
        padding-bottom: 4px;
        text-align: center;
        background-color: #002072;
        color: white;
        }

    #default tbody td {
            border: 1px solid #FFFFFF;
        }

    #default th {
            border: 1px solid #FFFFFF;
        }

    #default td, #default th {
        border: 1px solid #ddd;
        padding: 4px;
        }

    #default tr {background-color: #eaeaea;
        text-align: center;
        }
</style>
</head>
<body>
"""
    message_end = """</body>"""
    subject_controls = "Input controls for source {}"
    subject_source_completed = "Datapull process for source {}"

    def __init__(self, config_rigid: Dict[str, Any],
                 config_custom: Dict[str, Any],
                 spark_session_builder: Callable[[], SparkSession]) -> None:
        self.config_rigid: Dict[str, Any] = config_rigid
        self.config_custom: Dict[str, Any] = config_custom
        self._change_types_config()
        self.spark_session_builder: Callable[[], SparkSession] = spark_session_builder
        self.sources: List[str] = self._get_sources()
        self.email_sender: EmailSender = EmailSender(self.config_custom["email_sender"])
        self._quita_acentos()
    #
    @property
    def sqlContext(self) -> SparkSession:
        """Sesión de Spark perezosa: la crea si no está inicializada."""
        if not hasattr(self, '_sqlContext'):
            self._sqlContext = self.spark_session_builder()
            self._quita_acentos()  # Ensure UDF is registered whenever the session is created
        return self._sqlContext

    @property
    def pivot(self) -> DataFrame:
        """Tabla pivot (cliente x vintage), preparada si alguna fuente la
        necesita enriquecida."""
        try:
            self._pivot = self.sqlContext.read.parquet(self._path_new_pivot)
        except:
            self._pivot = self._pivot_preparation(self.sources)
        #
        return self._pivot

    def _get_sources(self) -> List[str]:
        """Normaliza ``config_custom["sources"]`` a lista.

        Returns
        -------
        List[str]
            Todas las fuentes a ejecutar definidas por el usuario final.
        """
        if type(self.config_custom["sources"]) is list:
            return(self.config_custom["sources"])
        else:
            return([self.config_custom["sources"]] )

    def run(self) -> None:
        """Ejecuta el proceso datapull para todas las fuentes configuradas."""
        for source in self.sources:
            try:
                self._pipeline(source)
            except Exception as e:
                traceback_output = traceback.format_exc()
                mess_error = f"An error was found in source {source}, contact data engineer team for details and solutions"
                print(mess_error)
                print(traceback_output)
                subject = "Error in execution of source {}".format(source)
                self.email_sender.send(
                    self.config_custom["email_receivers"],
                    '{}'.format(subject),
                    mess_error + f"\n{traceback_output}"
                    )

    def _pipeline(self, source: str) -> None:
        """Crea y ejecuta el pipeline de una fuente concreta.

        Recorre ``steps_list`` de la config rígida enhebrando
        ``previous_path`` entre etapas (extract -> weekly_extract ->
        missing_treatment -> groupby -> pivot -> ratios/features -> mobs
        -> vector_assembler) y manda el correo con los tiempos al final.

        Parameters
        ----------
        source : str
            Nombre de la fuente sobre la que correr el datapull.
        """
        logging.info("############# DATAPULL START FOR SOURCE: %s ###############", source.upper())
        ########### Define general constants ###########
        source_dict = self.config_rigid[source]
        write_path = os.path.join(self.config_custom["working_path"], self.config_custom["year_id"], source)
        pivot_path = self.config_custom["pivot_path"]
        number_threads = self.config_custom["n_threads"]
        write_mode = self.config_custom["write_mode"]
        ################ DESCOMENTAR DESPUES
        steps_list = self.config_rigid[source]["steps_list"]
        year_id = self.config_custom["year_id"]
        ######### Execution of different steps ###########
        previous_step = None
        previous_path = None
        #Defining missing treatment (required because contains missing treatment and mobs in the same class)
        #mt=missing_treatment(year_id,source_dict,source,self.config_custom["n_mis"])
        mt = MissingTreatment(year_id, source_dict, source,
                              self.config_custom["n_mis"], self.config_custom["n_threads"])
        time_execution = []
        for step in steps_list:
            time_start = time.time()
            if step == 'extract':
                # Special nmis cases
                if source == "payroll_income":
                    self.config_custom["n_mis"] = self.config_custom["n_mis"] + 2
                if "weeks" in source_dict["steps_list"]:
                    self.config_custom["n_mis"] = self.config_custom["n_mis"] + 1
                #
                # Start extract
                self._perform_extract(source_dict, write_path, number_threads, write_mode, source)
                previous_step = "extract"
                previous_path = os.path.join(write_path,"extract")
            elif step == "weeks":
                path_weeks = previous_path
                tables_weeks = [table + "_historic" for table in source_dict["extract"]["table_name"]]
                self._perform_weeks(path_weeks, tables_weeks, source)
                # restore n_mis
                self.config_custom["n_mis"] = self.config_custom["n_mis"] - 1
                previous_step = "weekly_extract"
                previous_path = os.path.join(write_path,"weekly_extract")
            elif step == "mt":
                path_mt = previous_path
                tables_mt = [table + "_historic" for table in source_dict["extract"]["table_name"]]
                self._perform_mt(mt, path_mt, tables_mt, source)
                previous_step = "missing_treatment"
                previous_path = os.path.join(write_path,"missing_treatment")
            elif step == "groupby":
                path_groupby = os.path.join(write_path,"groupby")

                agg_dict = source_dict["group_by"]
                self._perform_groupby(
                    path_groupby=path_groupby,
                    pivot_path=pivot_path,
                    source=source,
                    previous_path=previous_path,
                    agg_dict=agg_dict,
                    n_mis=self.config_custom["n_mis"],
                    period=self.config_custom["period"],
                    tables=[table + "_historic" for table in source_dict["extract"]["table_name"]]
                )
                previous_step = "groupby"
                previous_path = os.path.join(write_path,"groupby")
                # Restore n_mis
                if source == "payroll_income":
                    self.config_custom["n_mis"] = self.config_custom["n_mis"] - 2
            elif step == "pivoting":
                path_pivoting = os.path.join(write_path,"pivot")
                pivot_dict = source_dict["pivoting"]
                self._perform_pivoting(path_pivoting, previous_path, pivot_dict, source)
                previous_step = "pivot"
                previous_path = os.path.join(write_path,"pivot")
            elif step == "ratios":
                logging.info("%s - Performing Ratios", source.upper())
                #print("Performing ratios")
                ratios_dict = source_dict["ratios"]
                ratios_dict["time_window"] = self._get_time_window(self.config_custom["n_mis"],self.config_custom["period"])
                ratios_dict["n_time"] = self.config_custom["n_mis"]
                ratios = GenerateRatios(previous_path, ratios_dict, source, self.sqlContext, write_mode)
                ratios.run()
                previous_step = "ratios"
                if source == "corporate":
                    previous_path = os.path.join(write_path,"features")
                else:
                    previous_path = os.path.join(write_path,"ratios")
                logging.info("%s - Ending Ratios", source.upper())
                #print("Ending ratios")
            elif step == "fe":
                try:
                    fe_dict = source_dict["fe"]
                except:
                    fe_dict = None
                #
                self._perform_fe(previous_path, write_path, source, fe_dict)
                previous_step = "features"
                previous_path = os.path.join(write_path,"features")
            elif step == "mobs":
                if source not in ["payroll_income", "payroll_streaks"]:
                    self._create_mobs_extract()
                working_path = self.config_custom["working_path"]
                path_fe = previous_path
                previous_path = os.path.join(write_path,"mobs")
                self._perform_mobs(working_path, path_fe, mt, previous_path, source)
                previous_step = "mobs"
                previous_path = os.path.join(write_path,"mobs","imputate_mobs")
            elif step == "va":
                previous_step = "va"
                path_features = previous_path
                self._perform_va(path_features = path_features, write_path = write_path, source=source)
            elif step == "cols2vec":
                cols2vec_dict = source_dict["cols2vec"]
                self._perform_cols2vec(write_path, cols2vec_dict, source)
                previous_step = "cols2vec"

            time_end = time.time()
            time_execution.append(self._calculate_time(time_end-time_start))
        logging.info("############# DATAPULL END FOR SOURCE: %s #############", source.upper())
        execution_dataframe = pd.DataFrame({"steps" : steps_list, "time of execution": time_execution})
        message_start = self.message_start_finals
        message_style = self.message_style
        message_end = self.message_end
        html_mess = self._create_html_notification_times(execution_dataframe, message_start,message_style,message_end)
        subject = self.subject_source_completed.format(source)
        self.email_sender.send(
            self.config_custom["email_receivers"],
            '{} completed'.format(subject),
            html_mess
            )

    def _quita_acentos(self) -> None:
        """Registra la UDF ``quita_acentos`` (minúsculas sin caracteres
        especiales)."""
        def quita_acentos(unicodestring: Optional[str]) -> str:
            if unicodestring == None:
                return ""
            return unidecode.unidecode(unicodestring).lower()
        self.sqlContext.udf.register("quita_acentos",quita_acentos)

    def _calculate_time(self, time_passed: float) -> str:
        """Da formato (horas/minutos/segundos) a un tiempo en segundos.

        Parameters
        ----------
        time_passed : float
            Tiempo transcurrido, en segundos, a convertir y formatear.

        Returns
        -------
        str
            El tiempo formateado de esa transformación.
        """
        time_passed = round(time_passed)
        if time_passed / 3600 > 1:
            hours = time_passed // 3600
            minutes = (time_passed - hours * 3600) // 60
            return f"{hours} hours, {minutes} minutes"
        elif time_passed / 60 > 1:
            minutes = time_passed // 60
            seconds = time_passed - minutes * 60
            return f"{minutes} minutes, {seconds} seconds"
        else:
            return f"{time_passed} seconds"

    ###################################################
    ############# Perform of steps#####################
    ###################################################

    def _perform_extract(self, source_dict: Dict[str, Any], write_path: str,
                         number_threads: int, write_mode: str,
                         source: str) -> None:
        """Ejecuta la extracción.

        Parameters
        ----------
        source_dict : Dict[str, Any]
            Diccionario de configuración de la fuente.
        write_path : str
            Path donde escribir el extract.
        number_threads : int
            Número de hilos a usar en la ejecución.
        write_mode : str
            Modo de escritura de los parquets.
        source : str
            Fuente sobre la que hacer la extracción.
        """
        logging.info("%s - Starting Extract", source.upper())
        #print("Starting extract")
        sources_cat = source_dict["extract"]
        path_extract = os.path.join(write_path, "extract")
        if self._check_extract(path_extract = path_extract, extract_dictionary = sources_cat):
            n_mis = self.config_custom["n_mis"]
            extract = Extract(pivot_table = self.pivot, working_path = write_path, n_mis = n_mis,\
                        sources_cat = sources_cat, source=source, sqlContext = self.sqlContext,\
                        number_threads = number_threads, mode = write_mode)
            extract.collect_history()
            html_mess = extract.create_html_controls(self.message_start_controls_stract, self.message_style, self.message_end)
            subject = self.subject_controls.format(source)
            self.email_sender.send(
                self.config_custom["email_receivers"],
                '{} completed'.format(subject),
                html_mess
            )
            #print("Ending extract")
            logging.info("%s - Ending Extract", source.upper())
        else:
            logging.info("%s - Extract was already performed", source.upper())
            #print(f"Extract was already performed for source {source}!")
    #
    @reset_spark_session
    def _perform_weeks(self, path_weeks: str, tables_weeks: List[str],
                       source: str) -> None:
        """Ejecuta el reetiquetado semanal (``WeeklyTFrom``).

        Parameters
        ----------
        path_weeks : str
            Path donde están los dataframes del paso extract.
        tables_weeks : List[str]
            Lista con todas las tablas a las que aplicar el reetiquetado.
        source : str
            Fuente en ejecución.
        """
        logging.info("%s - Starting Weeks Re TFrom", source.upper())
        #print("Starting missing treatment")
        path_write = self.config_custom["working_path"]
        path_missing = os.path.join(path_weeks[:path_weeks.rindex("/")],"weekly_extract")
        if self._check_missing(path_missing = path_missing, tables_missing = tables_weeks):
            weekly_tfrom = WeeklyTFrom(
                extracted_tables = tables_weeks,
                extracted_path = path_weeks,
                weeks_dict=self.config_rigid[source]["weeks"],
                n_mis=self.config_custom["n_mis"],
                sqlContext=self.sqlContext
            )
            weekly_tfrom.run()
            #print("Ending missing treatment")
            logging.info("%s - Ending Weeks Re TFrom", source.upper())
        else:
            #print("Missing treatment was already performed!")
            logging.info("%s - Weeks Re TFrom was already performed", source.upper())
    #
    @reset_spark_session
    def _perform_mt(self, mt: MissingTreatment, path_mt: str,
                    tables_mt: List[str], source: str) -> None:
        """Ejecuta el tratamiento de missings.

        Parameters
        ----------
        mt : MissingTreatment
            Objeto de tratamiento de la fuente.
        path_mt : str
            Path donde se guardan los dataframes con missings tratados.
        tables_mt : List[str]
            Lista con todas las tablas a tratar.
        source : str
            Fuente en ejecución.
        """
        logging.info("%s - Starting Missing Treatment", source.upper())
        #print("Starting missing treatment")
        path_write = self.config_custom["working_path"]
        path_missing = os.path.join(path_mt[:path_mt.rindex("/")],"missing_treatment")
        if self._check_missing(path_missing = path_missing, tables_missing = tables_mt):
            mt.run_mt(path_mt,tables_mt,path_write,self.sqlContext)
            #print("Ending missing treatment")
            logging.info("%s - Ending Missing Treatment", source.upper())
        else:
            #print("Missing treatment was already performed!")
            logging.info("%s - Missing Treatment was already performed", source.upper())
    #
    @reset_spark_session
    def _perform_groupby(self, path_groupby: str, source: str,
                         previous_path: Optional[str], pivot_path: str,
                         agg_dict: Dict[str, Any], n_mis: int, period: int,
                         tables: List[str]) -> None:
        """Ejecuta el groupby/agregaciones.

        Parameters
        ----------
        path_groupby : str
            Path donde se guarda el groupby.
        source : str
            Nombre de la fuente.
        previous_path : str
            Path del paso anterior.
        pivot_path : str
            Path del pivot de clientes.
        agg_dict : Dict[str, Any]
            Diccionario de agregaciones del groupby.
        n_mis : int
            Meses de información.
        period : int
            Periodo de agrupación.
        tables : List[str]
            Lista de tablas involucradas en el groupby.
        """
        if self._check_groupby(path_groupby):
            logging.info("%s - Starting GroupBy", source.upper())
            group_by = GroupBy(
                source=source,
                previous_path=previous_path,
                pivot_path=pivot_path,
                agg_dict=agg_dict,
                sqlContext=self.sqlContext,
                n_mis=n_mis,
                period=period,
                tables=tables
            )
            group_by.run()
        else:
            logging.info("%s - GroupBy was already performed", source.upper())
            #print("Groupby was already performed!")
    #
    @reset_spark_session
    def _perform_pivoting(self, path_pivoting: str, previous_path: str,
                          pivot_dict: Dict[str, Any], source: str) -> None:
        """Ejecuta el pivoting.

        Parameters
        ----------
        path_pivoting : str
            Path donde se guarda el pivoting.
        previous_path : str
            Path del paso anterior (groupby).
        pivot_dict : Dict[str, Any]
            Config de pivoting de la fuente.
        source : str
            Fuente en ejecución.
        """
        #print("Starting pivoting")
        logging.info("%s - Starting Pivoting", source.upper())
        if self._check_pivoting(path_pivoting):
            pivoting = GeneratePivot(previous_path, pivot_dict, self.sqlContext)
            pivoting.pivot_data()
        else:
            #print("pivoting was already performed!")
            logging.info("%s - Pivoting was already performed", source.upper())
        #print("Ending pivoting")
        logging.info("%s - Ending Pivoting", source.upper())
    #
    @reset_spark_session
    def _perform_fe(self, path_transpose: str, write_path: str, source: str,
                    fe_dict: Optional[Dict[str, Any]] = None) -> None:
        """Ejecuta el feature engineering.

        Si ``features/`` ya existe, solo corre las transformaciones que
        falten según :meth:`_fe_performed` (reanudación a mitad de etapa).

        Parameters
        ----------
        path_transpose : str
            Path del dataframe de entrada a transponer.
        write_path : str
            Path donde escribir la salida del feature engineering.
        source : str
            Fuente en ejecución.
        fe_dict : Optional[Dict[str, Any]]
            Diccionario de feature engineering; es opcional.
        """
        ########## Constants of feature engineering ##########
        #trans_list = self.config_custom["lista_trans"]
        logging.info("%s - Starting Feature Enginnering", source.upper())
        """
        Aqui regresar si dynamic no funciona
        """
        #
        df_transpose = self.sqlContext.read.parquet(path_transpose)
        #
        if self.config_custom.get("lista_trans_per_source") is not None:
            trans_list = self.config_custom["lista_trans_per_source"][source]
        else:
            trans_list = self.config_custom["lista_trans"]
        logging.debug("Feature Transformations: %s",trans_list)
        #print(source,":",trans_list)
        path_fe = os.path.join(write_path, "features")
        period = self.config_custom["period"]
        number_threads = self.config_custom["n_threads"]
        write_mode = self.config_custom["write_mode"]
        ########## Check if Feature Engineering was already performed ##########
        fe = FeatureEngineering(df = df_transpose, working_path = write_path, period = period,\
                        sqlContext = self.sqlContext, number_threads = number_threads,\
                        fe_dict=fe_dict, mode = write_mode)
        if not self._evaluate_path(path_fe):
            fe.run(trans_list)
        else:
            list_remaining = self._fe_performed(path_fe, trans_list)
            if list_remaining:
                fe.run(list_remaining)
            else:
                logging.info("%s - Feature Engineering was already performed", source.upper())
    #
    @reset_spark_session
    def _perform_mobs(self, working_path: str, path_fe: str,
                      mt: MissingTreatment, path_mobs: str,
                      source: str) -> None:
        """Ejecuta los MOBs (months on books / imputación por antigüedad).

        Parameters
        ----------
        working_path : str
            Working path raíz de la corrida.
        path_fe : str
            Path de las transformaciones de feature engineering.
        mt : MissingTreatment
            Objeto de tratamiento de la fuente (también corre los mobs).
        path_mobs : str
            Path de salida de los mobs.
        source : str
            Fuente en ejecución.
        """
        #print("Performing mobs!")
        logging.info("%s - Starting MOBs", source.upper())
        files_fe = self._check_files(path_fe)
        periodo = self.config_custom["period"]
        path_mobs_aux = os.path.join(path_mobs,"imputate_mobs")
        if self._check_mobs(path_mobs_aux, files_fe):
            feat_list = self._get_mobs_list(path_fe)
            mt.run_mobs(working_path, path_mobs, path_fe, feat_list, periodo, self.sqlContext)
        else:
            #print("Mobs already performed!")
            logging.info("%s - MOBs was already performed", source.upper())
        #print("Ending mobs!")
        logging.info("%s - Ending MOBs", source.upper())
    #
    @reset_spark_session
    def _perform_va(self, path_features: str, write_path: str,
                    source: str) -> None:
        """Ejecuta el vector assembler.

        Parameters
        ----------
        path_features : str
            Path donde están las features (salida de mobs).
        write_path : str
            Path donde escribir el vector assembler.
        source : str
            Fuente en ejecución.
        """
        #print("Starting vector assembler")
        logging.info("%s - Starting Vector Assembler", source.upper())
        save_path = os.path.join(write_path,"vector_assembler")
        if self._evaluate_path(save_path):
            logging.info("%s - Vector Assembler was already performed", source.upper())
            #print(f"Vector assembler was already performed")
        else:
            va = FeaturesVectorAssembler(path_features = path_features, save_path = save_path, sqlContext = self.sqlContext)
            va.run()

            logging.info("%s - Ending Vector Assembler", source.upper())
            #print("Ending vector assembler")
    #
    @reset_spark_session
    def _perform_cols2vec(self, write_path: str,
                          cols2vec_dict: Dict[str, Any],
                          source: str) -> None:
        """Ejecuta el cols2vec (empaquetado final del producto).

        Parameters
        ----------
        write_path : str
            Path de trabajo de la fuente.
        cols2vec_dict : Dict[str, Any]
            Config de cols2vec de la fuente.
        source : str
            Fuente en ejecución.
        """
        #print("Starting cols2vec")
        logging.info("%s - Starting Cols2Vec(VectorAssembler)", source.upper())
        save_path = os.path.join(write_path,"vector_assembler")
        if self._evaluate_path(save_path):
            #print(f"Cols2Vec(VectorAssembler) was already performed")
            logging.info("%s - Cols2Vec(VectorAssembler) was already performed", source.upper())
        else:
            c2v = ColumnsToVector(write_path, cols2vec_dict, sqlContext = self.sqlContext)
            c2v.run()
        #print("Ending Cols2Vec(VectorAssembler)")
        logging.info("%s - Ending Cols2Vec(VectorAssembler)", source.upper())
    #
    ###################################################
    ########### Check the current step#################
    ###################################################
    def _check_extract(self, path_extract: str,
                       extract_dictionary: Dict[str, Any]) -> bool:
        """Comprueba si el extract ya se ejecutó en el workpath.

        Parameters
        ----------
        path_extract : str
            Path del extract.
        extract_dictionary : Dict[str, Any]
            Diccionario de extract de la fuente.

        Returns
        -------
        bool
            ``True`` si el extract hay que ejecutarlo y ``False`` si ya
            existe (para todas las tablas).
        """
        tables_historic = [table + "_historic" for table in extract_dictionary["table_name"]]
        files = self._check_files(path_extract)
        if files:
            for file_aux in files:
                if file_aux in tables_historic:
                    tables_historic.remove(file_aux)
            if tables_historic:
                return True
            else:
                return False
        else:
            return True

    def _check_missing(self, path_missing: str,
                       tables_missing: List[str]) -> bool:
        """Comprueba si el missing treatment ya se ejecutó en el workpath.

        Parameters
        ----------
        path_missing : str
            Path del missing treatment.
        tables_missing : List[str]
            Lista de tablas de salida esperadas.

        Returns
        -------
        bool
            ``True`` si hay que ejecutarlo; ``False`` si ya existe (para
            todas las tablas).
        """
        tables_aux = tables_missing.copy()
        files = self._check_files(path_missing)
        if files:
            for file_aux in files:
                if file_aux in tables_aux:
                    tables_aux.remove(file_aux)
            if tables_aux:
                return True
            else:
                return False
        else:
            return True

    def _check_groupby(self, path_groupby: str) -> bool:
        """Comprueba si el groupby ya se ejecutó en el workpath.

        Parameters
        ----------
        path_groupby : str
            Path del groupby.

        Returns
        -------
        bool
            ``True`` si hay que ejecutarlo; ``False`` si ya existe.
        """
        if self._evaluate_path(path_groupby):
            return False
        else:
            return True

    def _check_pivoting(self, path_pivoting: str) -> bool:
        """Comprueba si el pivoting ya se ejecutó en el workpath.

        Parameters
        ----------
        path_pivoting : str
            Path del pivoting.

        Returns
        -------
        bool
            ``True`` si hay que ejecutarlo; ``False`` si ya existe.
        """
        if self._evaluate_path(path_pivoting):
            return False
        else:
            return True

    def _fe_performed(self, path_fe: str, trans_list: List[str]) -> List[str]:
        """Devuelve las transformaciones de feature engineering que quedan
        por ejecutar.

        Parameters
        ----------
        path_fe : str
            Path de ``features/``.
        trans_list : List[str]
            Lista de transformaciones configuradas.

        Returns
        -------
        List[str]
            Transformaciones pendientes (``[]`` si no queda ninguna).
        """
        trans_list_aux = trans_list.copy()
        files = self._check_files(path_fe)
        for fil in files:
            if fil[:fil.index("_")] in trans_list_aux:
                trans_list_aux.remove(fil[:fil.index("_")])
        #The condition below is to check if another transformation excluding ratios mod was not performed
        if len(trans_list_aux) > 1:
            return trans_list_aux
        else:
            if len(files)-len(trans_list) <= 0:
                return []
            else:
                return ["ratiosmod"]

    def _check_mobs(self, path_mobs: str, files_fe: List[str]) -> bool:
        """Comprueba si los mobs ya se ejecutaron.

        Parameters
        ----------
        path_mobs : str
            Path de ``mobs/imputate_mobs``.
        files_fe : List[str]
            Directorios presentes en feature engineering.

        Returns
        -------
        bool
            ``True`` si hay que ejecutarlo; ``False`` si ya existe (para
            todas las transformaciones).
        """
        files = self._check_files(path_mobs)
        if files:
            for file_aux in files:
                if file_aux in files_fe:
                    files_fe.remove(file_aux)
            if files_fe:
                return True
            else:
                return False
        else:
            return True
    ###################################################
    ########### Preparation of steps#####################
    ###################################################
    @reset_spark_session
    def _pivot_preparation(self, sources: List[str]) -> DataFrame:
        """Prepara el pivot: lo enriquece con tarjetas de crédito y contratos
        de liabilities si alguna fuente lo requiere.

        Parameters
        ----------
        sources : List[str]
            Lista de fuentes para ver si hace falta preparación.

        Returns
        -------
        DataFrame
            El pivot (leído de ``pivot_path`` o el preparado y persistido
            en ``pivot_prepared``).
        """
        #print("Preparing pivot for the sources")
        logging.info(f"Preparing Pivot for sources: {sources}")
        path_new_pivot = os.path.join(self.config_custom["working_path"], self.config_custom["year_id"], "pivot_prepared")
        self._path_new_pivot = path_new_pivot
        if self._evaluate_path(path_new_pivot):
            logging.info("Pivot was already prepared")
            #print("Pivot already prepared!")
            return self.sqlContext.read.parquet(path_new_pivot)
        pivot = self.sqlContext.read.parquet(self.config_custom["pivot_path"])
        vintages = self._get_vintages_dates(pivot)
        n_mis = self.config_custom["n_mis"]
        if self._check_crd_acct_nbr(sources):
            lag = self.config_rigid["lag_ecm_pivot"]
            pivots_aux_list = []
            for vintage in vintages:
                max_date, min_date = self._get_range_dates([vintage], lag, n_mis)
                df_ecm = self.sqlContext.sql(self.config_rigid["query_ecm"])
                df_ecm = df_ecm.where(df_ecm.proc_dt < max_date).where(df_ecm.proc_dt >= min_date)[["numcliente","crd_acct_nbr"]].\
                distinct()
                pivot_aux = pivot.where(F.datediff(F.col("vintage"), F.lit(vintage)) == 0)
                pivot_aux = pivot_aux.join(df_ecm,pivot_aux.numcliente == df_ecm.numcliente,how="left").drop(df_ecm.numcliente)
                pivots_aux_list.append(pivot_aux)
            # Unimos todos los pivots de vintages
            pivot = reduce(DataFrame.unionAll, pivots_aux_list)
        if self._check_contratos(sources):
            lag = self.config_rigid["lag_liabilities_pivot"]
            pivots_aux_list = []
            mis_col = self.config_rigid["mis_col_liabilities_pivot"]
            for vintage in vintages:
                max_date, min_date = self._get_range_dates([vintage], lag, n_mis)
                pivot_aux = pivot.where(F.datediff(F.col("vintage"), F.lit(vintage)) == 0)
                for query in self.config_rigid["queries_liabilities"]:
                    df_liab = self.sqlContext.sql(query)
                    if "id_cuenta16" in df_liab.columns:
                        df_liab = df_liab.where(df_liab[mis_col] < max_date).where(df_liab[mis_col] >= min_date)[["numcliente",
                        "id_cuenta16"]].distinct()
                    else:
                        columnas = df_liab.columns
                        if "mis_date" in columnas:
                            columnas.remove("mis_date")
                        if "inf_dt" in columnas:
                            columnas.remove("inf_dt")
                        df_liab = df_liab.where(df_liab[mis_col] < max_date).where(df_liab[mis_col] >= min_date)[columnas].distinct()
                    pivot_aux = pivot_aux.join(df_liab, pivot_aux.numcliente == df_liab.numcliente,how="left").drop(df_liab.numcliente)
                pivots_aux_list.append(pivot_aux)
            # Unimos todos los pivots de vintages
            pivot = reduce(DataFrame.unionAll, pivots_aux_list)
        if self._check_crd_acct_nbr(sources) and self._check_contratos(sources):
            cols_pivot = ['numcliente', 'vintage', 'crd_acct_nbr', 'num_contrato_maestra', 'num_contrato_perfiles',
            'num_contrato_perf_ejec', 'num_contrato_cheques', 'num_contrato_otros']
            pivot = pivot[cols_pivot].distinct()
        elif self._check_crd_acct_nbr(sources):
            cols_pivot = ['numcliente', 'vintage', 'crd_acct_nbr']
            pivot = pivot[cols_pivot].distinct()
        elif self._check_contratos(sources):
            cols_pivot = ['numcliente', 'vintage','num_contrato_maestra', 'num_contrato_perfiles', 'num_contrato_perf_ejec',
            'num_contrato_cheques', 'num_contrato_otros']
            pivot = pivot[cols_pivot].distinct()
        else:
            return pivot
        pivot.write.mode(self.config_custom["write_mode"]).parquet(path_new_pivot)
        pivot = self.sqlContext.read.parquet(path_new_pivot)
        #print("Ending pivot preparation")
        logging.info("Ending Pivot Preparation")
        return pivot
    #
    def _create_mobs_extract(self) -> None:
        """Crea la extracción previa que necesita el módulo de mobs.

        Escribe ``<working_path>/<year_id>/mobs_preparation`` con un
        ``Extract`` sobre la config ``mobs_preparation``.
        """
        logging.info("Starting MOBs Preparation")
        sources_cat = self.config_rigid["mobs_preparation"]
        write_path = os.path.join(self.config_custom["working_path"], self.config_custom["year_id"])
        n_mis = self.config_custom["n_mis"]
        path_premobs = os.path.join(write_path,"mobs_preparation")
        if self._check_mobs_extract(path_premobs):
            extract = Extract(pivot_table = self.pivot, working_path = write_path, n_mis = n_mis,\
                        sources_cat = sources_cat, source="mobs_preparation", sqlContext = self.sqlContext, name =
                        "mobs_preparation")
            extract.collect_history()
        else:
            #print("Premobs already performed!")
            logging.info("PreMOBs was already performed")
        #print("Ending mobs preparation")
        logging.info("Ending MOBs Preparation")

    def _check_mobs_extract(self, path_premobs: str) -> bool:
        """Comprueba si el premobs ya se ejecutó.

        Parameters
        ----------
        path_premobs : str
            Path donde se ubica el premobs.

        Returns
        -------
        bool
            ``True`` si hay que ejecutarlo; ``False`` si no.
        """
        if len(self._check_files(path_premobs)) == 3:
            return False
        else:
            return True

    def _check_crd_acct_nbr(self, sources: List[str]) -> bool:
        """Comprueba si alguna fuente necesita el número de tarjeta de crédito.

        Parameters
        ----------
        sources : List[str]
            Lista de fuentes a comprobar.

        Returns
        -------
        bool
            ``True`` si alguna la necesita.
        """
        for source in sources:
            if source in self.config_rigid["need_crd_acct_nbr"]:
                return True
        return False

    def _check_contratos(self, sources: List[str]) -> bool:
        """Comprueba si alguna fuente necesita el número de "contrato".

        Parameters
        ----------
        sources : List[str]
            Lista de fuentes a comprobar.

        Returns
        -------
        bool
            ``True`` si alguna lo necesita.
        """
        for source in sources:
            if source in self.config_rigid["need_contratos"]:
                return True
        return False

    def _get_mobs_list(self, path_fe: str) -> Tuple[List[str], List[str]]:
        """Clasifica las transformaciones de fe en (con imputación de mobs,
        solo copia).

        Parameters
        ----------
        path_fe : str
            Path donde se guardaron las transformaciones.

        Returns
        -------
        Tuple[List[str], List[str]]
            Tupla ``(transformaciones que necesitan imputación,
            transformaciones que solo se copian)``.
        """
        files_fe = self._check_files(path_fe)
        list_no_mobs = self.config_rigid["list_no_mobs"]
        list_copy = []
        list_imputate = []
        for feature_transformation in files_fe:
            if feature_transformation in list_no_mobs:
                list_copy.append(feature_transformation)
            else:
                list_imputate.append(feature_transformation)
        return list_imputate, list_copy
    ###################################################
    ########### Auxiliary/general functions#############
    ###################################################
    def _get_vintages_dates(self, pivot: DataFrame) -> List[Any]:
        """Obtiene las fechas de vintage distintas del pivot.

        Parameters
        ----------
        pivot : DataFrame
            Dataframe con los clientes y su fecha de vintage.

        Returns
        -------
        List[Any]
            Todas las fechas de vintage del pivot (orden descendente).
        """
        pivot.createOrReplaceTempView("pivot_temporal")
        vintages = self.sqlContext.sql("select distinct vintage from pivot_temporal").collect()
        vintages = [vintages[p][0] for p in range(len(vintages))]
        vintages.sort(reverse = True)
        return vintages

    def _get_range_dates(self, vintages: List[Any], lag: int, n_mis: int,
                         strftime: str = "%Y-%m-%d") -> Tuple[str, str]:
        """Obtiene el rango de fechas para la lista de clientes de una tabla.

        Parameters
        ----------
        vintages : list
            Lista con todas las fechas de vintage del pivot.
        lag : int
            Meses de lag de la fuente.
        n_mis : int
            Meses de información deseados.
        strftime : str
            Formato de fecha a string; por defecto ``"%Y-%m-%d"``.

        Returns
        -------
        Tuple[str, str]
            Tupla ``(max_date, min_date)``.
        """
        max_date = max(vintages) + relativedelta(days = 1) - relativedelta(months = -1, days = 1)
        min_date = min(vintages) + relativedelta(days = 1) - relativedelta(months = n_mis + lag - 1, days = 1)
        return (max_date.strftime(strftime),min_date.strftime(strftime))

    def _get_time_window(self, n_mis: int, period: int) -> List[int]:
        """Lista con los meses agrupados de información del feature
        engineering.

        Parameters
        ----------
        n_mis : int
            Número de meses de información.
        period : int
            Número de meses agrupados en fe.

        Returns
        -------
        List[int]
            Lista con la cantidad de meses agrupados, p. ej. ``[3,6,9,12]``.
        """
        time_window = []
        for k in range(1,n_mis+1):
            if k % period == 0:
                time_window.append(period * (k // period))
        return time_window

    def _obtain_path(self, path: str, substring: str) -> Optional[str]:
        """Obtiene el path dentro de ``path`` que contiene ``substring``.

        Parameters
        ----------
        path : str
            Directorio a listar.
        substring : str
            Substring a buscar en los nombres.

        Returns
        -------
        Optional[str]
            El path completo que contiene el substring, o ``None``.
        """
        files = self._check_files(path)
        for string in files:
            if substring in string:
                return os.path.join(path,string)
        return None

    def _check_substring(self, string_list: List[str], substring: str) -> bool:
        """Comprueba si ``substring`` está contenido en alguna string de la
        lista.

        Parameters
        ----------
        string_list : List[str]
            Lista de strings donde buscar.
        substring : str
            Substring a buscar.

        Returns
        -------
        bool
            ``True`` si el substring está en alguna string de la lista.
        """
        for string in string_list:
            if substring in string:
                return True
        return False

    def _check_files(self, path: str) -> List[str]:
        """Lista los ficheros/directorios de ``path`` en Hadoop.

        En ejecuciones locales se sustituye por
        ``libs.steps.path_listdir`` (ver ``patch_datapull_fs``).

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

        En ejecuciones locales se sustituye por
        ``libs.steps.path_exists``.

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

    def _change_types_config(self) -> None:
        """Convierte a ``int`` las claves ``n_mis``/``period``/``n_threads``
        del config custom."""
        for key in ["n_mis","period","n_threads"]:
            self.config_custom[key] = int(self.config_custom[key])

    def _create_html_notification_times(self, table_times: pd.DataFrame,
                                        message_start: str,
                                        message_style: str, message_end: str,
                                        table_id: str = "default") -> str:
        """Construye el HTML del correo de tiempos de ejecución.

        Parameters
        ----------
        table_times : pd.DataFrame
            Dataframe con el tiempo de cada step.
        message_start : str
            Comentario inicial para el título del correo.
        message_style : str
            Debe contener el estilo a usar para las tablas.
        message_end : str
            Contiene el comentario final y el ``</body>``.
        table_id : str
            Estilo de ``message_style`` a usar; tiene valor por defecto.

        Returns
        -------
        str
            El código HTML completo para enviar un correo.
        """
        mess_final = message_start + message_style + f"<br> Below are showed the time of execution of every step: <br> {table_times.to_html(index=False,table_id = table_id)} <br>" + message_end
        return mess_final
