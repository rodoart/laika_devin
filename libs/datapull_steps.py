"""
Wrappers :class:`~libs.steps.Step` para las etapas del pipeline datapull.

Cada etapa de ``DataPull._pipeline`` (``extract``, ``weeks``, ``mt``,
``groupby``, ``pivoting``, ``ratios``, ``fe``, ``mobs``, ``va``,
``cols2vec``) se convierte en un :class:`DatapullStageStep`: un
:class:`libs.steps.Step` cuyo ``step_action`` delega en el método
``DataPull._perform_*`` correspondiente.

Comportamiento dinámico (escritura a disco / recarga) idéntico al
framework:

- ``is_dynamic=True`` + salida completa -> la etapa se **recarga** (se
  salta); las comprobaciones de completitud reutilizan
  ``DataPull._check_extract``, ``_check_missing``, ``_fe_performed``,
  ``_check_mobs``, ``_evaluate_path``, etc. (los mismos guardas que
  ``_perform_*`` ya aplica internamente).
- ``previous_path`` se enhebra por ``output_parameters`` exactamente como
  hace ``_pipeline`` (mobs -> ``mobs/imputate_mobs``, ratios de corporate
  -> ``features``, ``va``/``cols2vec`` lo pasan intacto).
- La config de salida de cada etapa (``config/paths.json`` vía
  :class:`PathsConfig`) se guarda en ``output_hive[stage]``; las entradas
  marcadas ``keep_or_delete: "delete"`` se suman a
  :meth:`Step.collect_tmp_paths` para ``--cleanup``.
"""
import logging
import os
import time
from typing import Any, Dict, List, Optional

from libs.steps import Step, collect_step_output, path_exists
from libs.hive_path import HivePath
from libs.paths import PathsConfig
from libs.missing_treatment import MissingTreatment
from libs.mobs.mobs import Mobs
from libs.generate_ratios import GenerateRatios

logger = logging.getLogger(__name__)


class DatapullStageStep(Step):
    """Clase base: una etapa del pipeline de una fuente, respaldada por una
    entrada de ``config/paths.json``.

    Parameters
    ----------
    datapull:
        Instancia de ``DataPull`` ya construida (configs cargadas).
    source:
        Fuente (clave de ``config_rigid``) a la que pertenece la etapa.
    paths:
        :class:`PathsConfig` que resuelve la ruta de salida.
    previous_step:
        Etapa anterior de la cadena (``None`` en la primera).
    is_dynamic:
        ``True`` para recargar salidas persistidas.
    mt:
        ``MissingTreatment`` de la etapa ``mt``.
    mobs:
        ``Mobs`` de la etapa ``mobs``.

    Attributes
    ----------
    output_path : str
        Path absoluto donde la etapa escribe.
    write_path : str
        ``<working_path>/<year_id>/<source>`` raíz de la fuente.
    source_dict : Dict[str, Any]
        ``config_rigid[source]`` (config rígida de la fuente).
    last_duration : float
        Segundos que tardó la última :meth:`execute`.
    """

    #: Nombre de la etapa en ``paths.json`` / ``steps_list``.
    stage: str = ""

    def __init__(self, datapull: Any, source: str, paths: PathsConfig,
                 previous_step: Optional["Step"] = None,
                 is_dynamic: bool = True,
                 mt: Optional[MissingTreatment] = None,
                 mobs: Optional[Mobs] = None) -> None:
        self.datapull = datapull
        self.source: str = source
        self.paths: PathsConfig = paths
        self.mt: Optional[MissingTreatment] = mt
        self.mobs: Optional[Mobs] = mobs
        cc = datapull.config_custom
        cfg = paths.stage_config(cc["working_path"], cc["year_id"], source,
                                 self.stage)
        super().__init__(
            previous_step=previous_step,
            output_hive={self.stage: cfg},
            is_dynamic=is_dynamic,
            step_name=f"{source}_{self.stage}",
        )
        self.output_path: str = cfg["table_or_hdfs"]
        self.write_path: str = paths.source_root(
            cc["working_path"], cc["year_id"], source)
        self.source_dict: Dict[str, Any] = datapull.config_rigid[source]
        self.last_duration: float = 0.0
        # para el reporte de email: estado de la última ejecución de ESTE
        # step (sin contar los upstream) y su duración propia
        self.last_status: str = "pending"
        self.action_duration: float = 0.0

    @property
    def _historic_tables(self) -> List[str]:
        """Nombres ``<table>_historic`` que produce el extract de esta fuente."""
        return [t + "_historic"
                for t in self.source_dict["extract"]["table_name"]]

    @property
    def sqlContext(self) -> Any:
        """Siempre la sesión *actual*.

        Los ``_perform_*`` decorados con ``reset_spark_session`` paran y
        recrean la sesión entre etapas, así que no puede capturarse en
        tiempo de construcción.
        """
        return self.datapull.sqlContext

    @sqlContext.setter
    def sqlContext(self, value: Any) -> None:  # Step.__init__ asigna; ignorar
        pass

    # -- enhebrado de previous_path ----------------------------------------

    def _read_previous_path(self) -> Optional[str]:
        """Último ``previous_path`` propagado por el step upstream."""
        for value in self.input_parameters.values():
            if isinstance(value, dict) and "previous_path" in value:
                return value["previous_path"]
        return None

    def _next_path(self, previous_path: Optional[str]) -> Optional[str]:
        """``previous_path`` que verá la siguiente etapa (default: el propio)."""
        return self.output_path

    # -- comprobación de completitud (recarga dinámica) --------------------

    def _already_done(self) -> bool:
        """``True`` si la salida ya existe y está completa."""
        return path_exists(self.output_path)

    # -- cuerpo de la etapa -------------------------------------------------

    def _run(self, previous_path: Optional[str]) -> None:
        """Ejecuta el ``_perform_*`` real. Subclases lo implementan."""
        raise NotImplementedError

    def execute(self) -> Any:
        """Ejecuta la cadena previa + esta etapa midiendo la duración total."""
        _start = time.time()
        try:
            return super().execute()
        finally:
            self.last_duration = time.time() - _start

    def step_action(self) -> Dict[str, Any]:
        """Recarga la salida si está completa; si no, la recompute.

        Devuelve los ``output_parameters`` del step con ``previous_path``
        listo para el step siguiente.
        """
        previous_path = self._read_previous_path()
        _start = time.time()
        try:
            if self.is_dynamic and self._already_done():
                self.last_status = "reloaded"
                logger.info("%s data reloaded from %s",
                            self.stage.capitalize(), self.output_path)
            else:
                self._run(previous_path)
                self.last_status = "computed"
        except Exception:
            self.last_status = "failed"
            raise
        finally:
            self.action_duration = time.time() - _start
        return collect_step_output(
            self, self._next_path(previous_path), "previous_path")


###############################################################################
# Steps de etapa
###############################################################################

class ExtractStep(DatapullStageStep):
    """Etapa ``extract``: ``DataPull._perform_extract``.

    Aplica los ajustes especiales de ``n_mis`` del ``_pipeline`` original:
    +2 para ``payroll_income`` y +1 si la fuente incluye ``weeks``.
    """

    stage: str = "extract"

    def _already_done(self) -> bool:
        """``True`` si no falta ninguna tabla del extract (``_check_extract``)."""
        return not self.datapull._check_extract(
            self.output_path, self.source_dict["extract"])

    def _run(self, previous_path: Optional[str]) -> None:
        cc = self.datapull.config_custom
        # Ajustes especiales de n_mis (como en _pipeline): +2 para
        # payroll_income, +1 si la fuente tiene weeks. El ajuste es local a
        # la llamada — si este step se recarga en vez de ejecutarse, los
        # steps posteriores siguen viendo el n_mis base (determinismo al
        # reanudar).
        n_mis_bump = (2 if self.source == "payroll_income" else 0) + \
                     (1 if "weeks" in self.source_dict["steps_list"] else 0)
        cc["n_mis"] += n_mis_bump
        try:
            self.datapull._perform_extract(
                self.source_dict, self.write_path, cc["n_threads"],
                cc["write_mode"], self.source,
                # el correo de controles se pliega en el reporte único
                send_controls=False,
                # reanudable: cada parquet mensual/_full/_historic ya
                # escrito se recarga; solo falta lo que falte
                is_dynamic=self.is_dynamic)
        finally:
            cc["n_mis"] -= n_mis_bump


class WeeksStep(DatapullStageStep):
    """Etapa ``weeks``: ``DataPull._perform_weeks``; restaura ``n_mis`` (-1)."""

    stage: str = "weeks"

    def _already_done(self) -> bool:
        """``True`` si no falta ninguna ``_historic`` (``_check_missing``)."""
        return not self.datapull._check_missing(
            self.output_path, self._historic_tables)

    def _run(self, previous_path: Optional[str]) -> None:
        # weeks consume el mismo n_mis ampliado que extract (+1 por el mes
        # extra de historia, +2 si payroll_income); ajuste local a la llamada
        cc = self.datapull.config_custom
        n_mis_bump = (2 if self.source == "payroll_income" else 0) + 1
        cc["n_mis"] += n_mis_bump
        try:
            self.datapull._perform_weeks(
                previous_path, self._historic_tables, self.source,
                is_dynamic=self.is_dynamic)
        finally:
            cc["n_mis"] -= n_mis_bump


class MtStep(DatapullStageStep):
    """Etapa ``mt`` (missing treatment): ``DataPull._perform_mt``."""

    stage: str = "mt"

    def _already_done(self) -> bool:
        """``True`` si no falta ninguna ``_historic`` (``_check_missing``)."""
        return not self.datapull._check_missing(
            self.output_path, self._historic_tables)

    def _run(self, previous_path: Optional[str]) -> None:
        self.datapull._perform_mt(
            self.mt, previous_path, self._historic_tables, self.source)


class GroupByStep(DatapullStageStep):
    """Etapa ``groupby``: ``DataPull._perform_groupby``; restaura ``n_mis``
    (-2) para ``payroll_income``."""

    stage: str = "groupby"

    def _already_done(self) -> bool:
        # comprobación a nivel parquet: dir + _SUCCESS (una escritura
        # matada a medias se recomputa)
        return HivePath(self.output_path).is_successful()

    def _run(self, previous_path: Optional[str]) -> None:
        cc = self.datapull.config_custom
        self.datapull._perform_groupby(
            path_groupby=self.output_path,
            source=self.source,
            previous_path=previous_path,
            pivot_path=cc["pivot_path"],
            agg_dict=self.source_dict["group_by"],
            # payroll_income agrega con el n_mis ampliado (+2)
            n_mis=cc["n_mis"] + (2 if self.source == "payroll_income" else 0),
            period=cc["period"],
            tables=self._historic_tables)


class PivotingStep(DatapullStageStep):
    """Etapa ``pivoting``: ``DataPull._perform_pivoting``."""

    stage: str = "pivoting"

    def _already_done(self) -> bool:
        return HivePath(self.output_path).is_successful()

    def _run(self, previous_path: Optional[str]) -> None:
        self.datapull._perform_pivoting(
            self.output_path, previous_path,
            self.source_dict["pivoting"], self.source)


class RatiosStep(DatapullStageStep):
    """Etapa ``ratios``: instancia y corre ``GenerateRatios``.

    Para ``corporate`` los ratios escriben dentro de ``features`` (el
    ``previous_path`` propagado es el de la etapa ``fe``).
    """

    stage: str = "ratios"

    def _next_path(self, previous_path: Optional[str]) -> str:
        # los ratios de corporate escriben en "features"
        if self.source == "corporate":
            return self.paths.stage_path(
                self.datapull.config_custom["working_path"],
                self.datapull.config_custom["year_id"], self.source, "fe")
        return self.output_path

    def _already_done(self) -> bool:
        return path_exists(self._next_path(None))

    def _run(self, previous_path: Optional[str]) -> None:
        cc = self.datapull.config_custom
        logger.info("%s - Performing Ratios", self.source.upper())
        ratios_dict = self.source_dict["ratios"]
        ratios_dict["time_window"] = self.datapull._get_time_window(
            cc["n_mis"], cc["period"])
        ratios_dict["n_time"] = cc["n_mis"]
        ratios = GenerateRatios(previous_path, ratios_dict, self.source,
                                self.datapull.sqlContext, cc["write_mode"])
        ratios.run()
        logger.info("%s - Ending Ratios", self.source.upper())


class FeStep(DatapullStageStep):
    """Etapa ``fe`` (feature engineering): ``DataPull._perform_fe``.

    La completitud se comprueba por transformación (``_fe_performed``), de
    modo que una corrida colgada a mitad solo rehace las transformaciones
    que falten.
    """

    stage: str = "fe"

    def _already_done(self) -> bool:
        if not path_exists(self.output_path):
            return False
        # fe es por transformación: verifica que no quede ninguna pendiente
        cc = self.datapull.config_custom
        trans_list = (cc["lista_trans_per_source"][self.source]
                      if cc.get("lista_trans_per_source") is not None
                      else cc["lista_trans"])
        return not self.datapull._fe_performed(self.output_path, trans_list)

    def _run(self, previous_path: Optional[str]) -> None:
        fe_dict = self.source_dict.get("fe")
        self.datapull._perform_fe(previous_path, self.write_path,
                                  self.source, fe_dict)


class MobsStep(DatapullStageStep):
    """Etapa ``mobs``: ``DataPull._perform_mobs``.

    Propaga ``<write_path>/mobs/imputate_mobs`` como ``previous_path``
    para ``va``.
    """

    stage: str = "mobs"

    def _already_done(self) -> bool:
        path_fe = os.path.join(self.write_path, "features")
        files_fe = self.datapull._check_files(path_fe)
        return not self.datapull._check_mobs(self.output_path, files_fe)

    def _run(self, previous_path: Optional[str]) -> None:
        if self.source not in ["payroll_income", "payroll_streaks",
                               "adobe_bancanet", "adobe_mobile"]:
            self.datapull._create_mobs_extract()
        cc = self.datapull.config_custom
        path_mobs = os.path.join(self.write_path, "mobs")
        self.datapull._perform_mobs(
            cc["working_path"], previous_path, self.mobs, path_mobs, self.source)


class VaStep(DatapullStageStep):
    """Etapa ``va`` (vector assembler): ``DataPull._perform_va``.

    Escribe en ``vector_assembler`` pero deja pasar ``previous_path``
    intacto al siguiente step.
    """

    stage: str = "va"

    def _next_path(self, previous_path: Optional[str]) -> Optional[str]:
        # va escribe en vector_assembler pero previous_path pasa a través
        return previous_path

    def _run(self, previous_path: Optional[str]) -> None:
        self.datapull._perform_va(
            path_features=previous_path, write_path=self.write_path,
            source=self.source)


class Cols2VecStep(DatapullStageStep):
    """Etapa ``cols2vec``: ``DataPull._perform_cols2vec`` (empaqueta el
    producto final; deja pasar ``previous_path``)."""

    stage: str = "cols2vec"

    def _next_path(self, previous_path: Optional[str]) -> Optional[str]:
        return previous_path

    def _run(self, previous_path: Optional[str]) -> None:
        self.datapull._perform_cols2vec(
            self.write_path, self.source_dict["cols2vec"], self.source)


###############################################################################
# Constructor de cadenas
###############################################################################

#: Nombre de etapa (``steps_list``) -> clase de step.
STAGE_STEP_CLASSES: Dict[str, type] = {
    "extract": ExtractStep,
    "weeks": WeeksStep,
    "mt": MtStep,
    "groupby": GroupByStep,
    "pivoting": PivotingStep,
    "ratios": RatiosStep,
    "fe": FeStep,
    "mobs": MobsStep,
    "va": VaStep,
    "cols2vec": Cols2VecStep,
}


def build_source_chain(datapull: Any, source: str,
                       paths: Optional[PathsConfig] = None,
                       is_dynamic: bool = True,
                       step_filter: Optional[List[str]] = None) -> Step:
    """Construye la cadena ``previous_step`` de ``source`` y devuelve su último
    step.

    Parameters
    ----------
    datapull:
        Instancia de ``DataPull`` con las configs cargadas.
    source:
        Fuente a encadenar (clave de ``config_rigid``).
    paths:
        :class:`PathsConfig`; por defecto ``config/paths.json``.
    is_dynamic:
        ``True`` para recargar salidas persistidas; ``False`` recomputa
        todo.
    step_filter:
        Restringe las etapas de ``steps_list`` que se encadenan; las
        filtradas se saltan por completo (sus parquets upstream deben
        existir ya para que la corrida tenga sentido).

    Returns
    -------
    Step
        Último step de la cadena; ``.execute()`` corre toda la cadena.
    """
    paths = paths or PathsConfig()
    steps_list = datapull.config_rigid[source]["steps_list"]
    if step_filter:
        steps_list = [s for s in steps_list if s in step_filter]
    assert steps_list, f"No stages selected for source {source}"

    # MissingTreatment (mt) y Mobs (mobs), como en _pipeline
    cc = datapull.config_custom
    mt = MissingTreatment(cc["year_id"], datapull.config_rigid[source],
                          source, cc["n_mis"], cc["n_threads"])
    mobs = Mobs(cc["year_id"], datapull.config_rigid[source],
                source, cc["n_mis"], cc["n_threads"],
                is_dynamic=is_dynamic)

    previous: Optional[Step] = None
    for stage in steps_list:
        cls = STAGE_STEP_CLASSES[stage]
        previous = cls(datapull=datapull, source=source, paths=paths,
                       previous_step=previous, is_dynamic=is_dynamic,
                       mt=mt, mobs=mobs)
    return previous
