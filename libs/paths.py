"""
Cargador de ``config/paths.json`` — la config **interna** (no la toca el
usuario) que mapea cada etapa del pipeline a su path de salida relativo
bajo ``<working_path>/<year_id>/<source>/``.

La ubicación del JSON puede sobreescribirse con la variable de entorno
``LAIKA_PATHS_CONFIG`` para que los tests apunten a un archivo aislado;
``working_path``/``year_id`` siguen viniendo del ``config_custom.json``
del usuario.
"""
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional

#: Ubicación por defecto del JSON de paths internos (junto al repo).
DEFAULT_PATHS_CONFIG: str = str(
    Path(__file__).resolve().parents[1] / "config" / "paths.json")


class PathsConfig:
    """Resuelve los paths de salida por etapa a partir del config interno.

    Parameters
    ----------
    config_path:
        Ruta al ``paths.json``; si no se pasa se usa
        ``LAIKA_PATHS_CONFIG`` o :data:`DEFAULT_PATHS_CONFIG`.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_path: str = config_path or os.environ.get(
            "LAIKA_PATHS_CONFIG", DEFAULT_PATHS_CONFIG)
        with open(self.config_path) as f:
            self._config: Dict[str, Any] = json.load(f)
        #: ``{stage: {"path": rel, "keep_or_delete": "keep|delete"}}``.
        self.stages: Dict[str, Dict[str, Any]] = self._config["stages"]

    def source_root(self, working_path: str, year_id: str, source: str) -> str:
        """``<working_path>/<year_id>/<source>`` como path POSIX."""
        return str(PurePosixPath(working_path) / year_id / source)

    def stage_path(self, working_path: str, year_id: str, source: str,
                   stage: str) -> str:
        """Path de salida absoluto de ``stage`` para ``source``."""
        rel = self.stages[stage]["path"]
        return str(PurePosixPath(
            self.source_root(working_path, year_id, source)) / rel)

    def stage_config(self, working_path: str, year_id: str, source: str,
                     stage: str) -> Dict[str, Any]:
        """Config estilo ``output_hive`` para una etapa.

        Returns
        -------
        Dict[str, Any]
            ``{"table_or_hdfs": <path absoluto>, "keep_or_delete": ...}``
            tal como lo consumen los steps y los decoradores
            ``dynamic_parquet``.
        """
        cfg = dict(self.stages[stage])
        cfg["table_or_hdfs"] = cfg.pop("path")
        cfg["table_or_hdfs"] = self.stage_path(working_path, year_id, source, stage)
        return cfg
