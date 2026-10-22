#!/usr/bin/env python3
"""``main_dynamic.py`` — entry point del datapull sobre el framework Step.

Corre las mismas etapas que ``main.py``/``DataPull._pipeline``, pero cada
etapa es un :class:`libs.steps.Step` encadenado por ``previous_step``:

- la salida de cada etapa es un parquet bajo
  ``<working_path>/<year_id>/<source>/<stage>`` (el layout relativo vive en
  ``config/paths.json`` — archivo interno, no lo edita el usuario);
- ``is_dynamic`` (activado por defecto) hace la corrida reanudable: las
  etapas cuya salida ya está completa se recargan en vez de recomputarse;
- ``supervisor.py`` vigila el stdout (``Executing step:`` /
  ``Step finished:``) y reinicia este proceso si muere o se queda
  zombi — el proceso nuevo reanuda en la última etapa persistida.

Los archivos de config del usuario (``config/configuration.json``,
``config/config_custom.json``) no se tocan; ``config/paths.json`` solo
contiene paths relativos internos y puede reubicarse con
``LAIKA_PATHS_CONFIG``. ``LAIKA_CONFIG_DIR`` permite apuntar a un
directorio de configs alterno (tests).

Usage::

    python main_dynamic.py [--local] [--sources SRC [SRC ...]]
                           [--no-dynamic] [--steps S [S ...]]
                           [--cleanup | --cleanup-only] [--no-email]
"""
import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("main_dynamic")


def parse_args() -> argparse.Namespace:
    """Parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Datapull over the step framework (resumable).")
    parser.add_argument("--local", action="store_true",
        help="SparkSession local[*] instead of YARN")
    parser.add_argument("--sources", nargs="+", default=None,
        help="Override config_custom['sources']")
    parser.add_argument("--no-dynamic", action="store_true",
        help="Recompute every stage even if its output exists")
    parser.add_argument("--steps", nargs="+", default=None,
        help="Only chain these stages of steps_list")
    parser.add_argument("--paths-config", default=None,
        help="Override config/paths.json location")
    parser.add_argument("--cleanup", action="store_true",
        help="After executing, delete intermediate parquets")
    parser.add_argument("--cleanup-only", action="store_true",
        help="Only delete intermediate parquets")
    parser.add_argument("--no-email", action="store_true",
        help="Do not send notification emails")
    return parser.parse_args()


def install_local_shims() -> None:
    """Shims de ejecución local desde ``tests/local/bootstrap.py``.

    Instala el parche ``PurePosixPath`` y ``HADOOP_HOME`` en Windows.
    No-op en el clúster (si ``tests/local`` no existe o falla el import).
    """
    tests_local = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "tests", "local")
    if os.path.isdir(tests_local) and tests_local not in sys.path:
        sys.path.insert(0, tests_local)
    try:
        import bootstrap  # noqa: F401  (los shims viven a nivel de módulo)
    except ImportError:
        pass


def build_session_builder(local: bool) -> Callable[[], "Any"]:
    """Devuelve el builder de SparkSession apropiado.

    Se llama cada vez que hace falta una sesión nueva (``DataPull``
    resetea la sesión entre etapas con ``reset_spark_session``).

    Parameters
    ----------
    local:
        ``True`` -> ``local[*]`` (con ``java.library.path`` de
        ``HADOOP_HOME`` en Windows); ``False`` -> YARN.
    """
    from libs.session import SparkSessionBuilder
    if local:
        from libs.hive_path import os_mode
        extra_conf = {"spark.ui.enabled": "true"}
        if os_mode() == "windows" and os.environ.get("HADOOP_HOME"):
            hadoop_bin = (Path(os.environ["HADOOP_HOME"]) / "bin").as_posix()
            extra_conf["spark.driver.extraJavaOptions"] = \
                f'-Djava.library.path="{hadoop_bin}"'
        return SparkSessionBuilder(
            app_name=os.environ.get("APP_NAME", "DataPull_local"),
            master="local[*]",
            extra_conf=extra_conf).build
    return SparkSessionBuilder().build


def disable_email(datapull: Any) -> None:
    """Sustituye ``email_sender.send`` por un no-op."""
    datapull.email_sender.send = lambda *a, **k: None


def iter_chain(last_step: "Any") -> Iterator["Any"]:
    """Itera los steps de una cadena ``previous_step``, primero -> último."""
    chain: List[Any] = []
    step = last_step
    while step is not None:
        chain.append(step)
        step = step.previous_step if not isinstance(
            step.previous_step, list) else None
    return reversed(chain)


def run_source(datapull: Any, source: str, paths: Any, is_dynamic: bool,
               step_filter: Optional[List[str]], cleanup: bool,
               send_email: bool) -> bool:
    """Construye + ejecuta la cadena de steps de una fuente.

    Parameters
    ----------
    datapull:
        Instancia de ``DataPull`` (configs + email_sender).
    source:
        Fuente a procesar.
    paths:
        :class:`libs.paths.PathsConfig` del layout de salidas.
    is_dynamic:
        Recarga de parquets ya escritos.
    step_filter:
        Restringe las etapas encadenadas (``--steps``).
    cleanup:
        ``True``: solo borra parquets intermedios, sin ejecutar.
    send_email:
        Envía el correo de éxito/error.

    Returns
    -------
    bool
        ``True`` si la fuente terminó bien.
    """
    from libs.datapull_steps import build_source_chain
    from libs.notifications import (build_run_report, count_distinct_clients,
                                    feature_stats, parquet_num_columns)

    logging.info("############# DATAPULL START FOR SOURCE: %s ###############",
                 source.upper())
    last_step = build_source_chain(
        datapull, source, paths=paths, is_dynamic=is_dynamic,
        step_filter=step_filter)
    if cleanup:
        last_step.delete_tmp_paths()
        return True

    cc = datapull.config_custom
    started_at = datetime.now()
    status, error_tb = "success", None
    # los controles del extract se acumulan en el datapull (instancia
    # compartida entre fuentes) -> reset para no arrastrar la anterior
    datapull.extract_controls_html = None
    try:
        last_step.execute()
    except Exception:
        status = "failed"
        error_tb = traceback.format_exc()
        logger.error("Error in source %s:\n%s", source, error_tb)

    chain = list(iter_chain(last_step))
    # stats del reporte (defensivo: nunca debe impedir el envío del mail)
    stats: dict = {"pivot_clients": None, "final_clients": None,
                   "final_columns": None, "feature_detail": None,
                   "trans_list": None}
    try:
        sql_context = datapull.sqlContext
        stats["pivot_clients"] = count_distinct_clients(
            sql_context, cc["pivot_path"])
        out_path = last_step.output_path
        stats["final_clients"] = count_distinct_clients(
            sql_context, out_path)
        stats["final_columns"] = parquet_num_columns(sql_context, out_path)
        if any(s.stage == "fe" for s in chain):
            trans_list = (cc["lista_trans_per_source"][source]
                          if cc.get("lista_trans_per_source") is not None
                          else cc["lista_trans"])
            stats["trans_list"] = trans_list
            stats["feature_detail"] = feature_stats(
                sql_context,
                paths.stage_path(cc["working_path"], cc["year_id"],
                                 source, "fe"),
                trans_list)[1]
    except Exception:
        logger.warning("Run report stats failed", exc_info=True)

    if send_email:
        try:
            subject, html_mess = build_run_report(
                source=source, chain=chain, status=status,
                started_at=started_at, finished_at=datetime.now(),
                config_custom=cc, error_traceback=error_tb,
                controls_html=datapull.extract_controls_html, **stats)
            datapull.email_sender.send(cc["email_receivers"], subject,
                                       html_mess)
        except Exception:
            logger.warning("Run report email failed", exc_info=True)

    if status == "failed":
        return False
    logging.info("############# DATAPULL END FOR SOURCE: %s ###############",
                 source.upper())
    return True


def main() -> int:
    """Entry point: carga configs, construye ``DataPull`` y corre las fuentes.

    Returns
    -------
    int
        0 si todas las fuentes terminaron bien; 1 si alguna falló.
    """
    args = parse_args()
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S%p", level=logging.INFO)

    config_dir = os.environ.get("LAIKA_CONFIG_DIR", "config")
    with open(os.path.join(config_dir, "configuration.json")) as f:
        config_rigid = json.load(f)
    with open(os.path.join(config_dir, "config_custom.json")) as f:
        config_custom = json.load(f)

    if args.local:
        install_local_shims()
    from datapull import DataPull
    from libs.paths import PathsConfig
    paths = PathsConfig(args.paths_config)

    builder = build_session_builder(local=args.local)
    datapull = DataPull(config_rigid, config_custom, builder)

    send_email = not args.no_email and not os.environ.get("LAIKA_DISABLE_EMAIL")
    if not send_email:
        disable_email(datapull)

    sources = args.sources or config_custom["sources"]
    if isinstance(sources, str):
        sources = [sources]

    ok = True
    for source in sources:
        ok &= run_source(
            datapull, source, paths,
            is_dynamic=not args.no_dynamic,
            step_filter=args.steps,
            cleanup=args.cleanup or args.cleanup_only,
            send_email=send_email)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
