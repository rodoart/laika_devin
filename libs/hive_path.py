"""
`HivePath` — objeto-path estilo ``pathlib`` para las salidas del datapull.

Portado desde ``framework/libs/data_engineering_toolbox/path/__init__.py``
(subclase de ``PurePosixPath`` respaldada por comandos ``hdfs dfs``) y
extendido con un *backend* de filesystem local para que la misma API
funcione tanto en el clúster (HDFS) como en ejecuciones ``--local``
(pruebas).

Selección de *backend* por ruta:

- ``hdfs://...``                -> CLI ``hdfs``
- ``file:...``, ``C:\\...``, ... -> FS local
- resto -> variables de entorno ``LAIKA_FS_MODE``/``APP_MODE``:
  ``"hdfs"``/``"cluster"`` -> HDFS; ``"windows"``/``"unix"``/``"wsl"``/
  ``"local"`` -> local; automático: CLI ``hdfs`` si existe y la ruta es
  POSIX absoluta, en otro caso local.
"""
from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime
from fnmatch import fnmatch
from operator import itemgetter
from pathlib import Path, PurePosixPath
from subprocess import PIPE, Popen
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Union

# Type alias: cualquier cosa que se pueda convertir a ruta.
PathLike = Union[str, Path, "HivePath"]
# Fila de un `ls` parseado (mismo formato que `hdfs dfs -ls`).
LsEntry = Dict[str, Any]

###############################################################################
# Modo de plataforma (unix / wsl / windows)
###############################################################################

#: Modos de plataforma reconocidos.
OS_MODES = ("unix", "wsl", "windows")
#: Modos que fuerzan el backend HDFS.
HDFS_MODES = ("hdfs", "cluster")
#: Modos que fuerzan el filesystem local (``local`` = alias legacy).
LOCAL_MODES = ("local", "windows", "unix", "wsl")


def os_mode() -> str:
    """Plataforma de ejecución: ``"unix"`` | ``"wsl"`` | ``"windows"``.

    1. ``APP_OS_MODE`` / ``APP_MODE`` / ``LAIKA_OS_MODE`` con valor válido.
    2. Autodetección: ``win32`` -> ``windows``; kernel
       ``microsoft``/``wsl`` -> ``wsl``.
    3. **Default: ``"unix"``** — cualquier POSIX funciona sin configuración.
    """
    mode = (os.environ.get("APP_OS_MODE")
            or os.environ.get("APP_MODE")
            or os.environ.get("LAIKA_OS_MODE") or "").lower()
    if mode in OS_MODES:
        return mode
    if os.name == "nt" or sys.platform == "win32":
        return "windows"
    try:
        release = (platform.release() + " " + platform.version()).lower()
    except Exception:
        release = ""
    if "microsoft" in release or "wsl" in release:
        return "wsl"
    return "unix"


def _configured_fs_mode() -> str:
    """Modo de backend forzado por env."""
    return (os.environ.get("LAIKA_FS_MODE")
            or os.environ.get("APP_MODE") or "").lower()


###############################################################################
# Selección de backend
###############################################################################

def is_hdfs_path(path: PathLike) -> bool:
    """Decide si ``path`` vive en HDFS.

    Parameters
    ----------
    path:
        Ruta como ``str``, ``Path`` o ``HivePath``.

    Returns
    -------
    bool
        ``True`` si debe tratarse con el CLI ``hdfs``; ``False`` para el
        filesystem local.
    """
    s = str(path)
    if s.startswith("hdfs://"):
        return True
    if s.startswith("file:") or re.match(r"^[A-Za-z]:[/\\]", s):
        return False
    mode = _configured_fs_mode()
    if mode in HDFS_MODES:
        return True
    if mode in LOCAL_MODES:
        return False
    return s.startswith("/") and shutil.which("hdfs") is not None


def _local(p: PathLike) -> str:
    """Vista del FS local de una ruta (quita el esquema ``file:`` si existe)."""
    return str(p).replace("file:", "")


###############################################################################
# Backend hdfs (port de path/hdfs.py)
###############################################################################

def _arg_expander(*args: Union[str, Iterable[str]]) -> List[str]:
    """Expande argumentos mixtos (listas, tuplas, strings) a una lista plana.

    El CLI ``hdfs dfs`` acepta flags opcionales (`-r`, `-p`, ...) que el
    llamador pasa como lista o como string suelto.
    """
    result: List[str] = []
    for arg in args:
        if isinstance(arg, str):
            result.append(arg)
        elif isinstance(arg, Iterable):
            result += [i for i in arg]
        else:
            raise TypeError(f'{arg} is type {type(arg)}, not admitted.')
    return result


def hdfs_command(*args: Union[str, Iterable[str]],
                 wait: bool = False) -> "tuple[Popen, str, str]":
    """Ejecuta ``hdfs dfs <args>`` y lanza :class:`SystemError` si rc != 0.

    Parameters
    ----------
    args:
        Flags y rutas del subcomando (`-ls`, `-test`, `-rm`, ...).
    wait:
        Si ``True``, espera explícitamente a que el proceso termine
        (``Popen.wait``), además del ``communicate`` implícito.

    Returns
    -------
    tuple
        ``(proceso, stdout, stderr)`` ya decodificados a ``str``.
    """
    cmd = ['hdfs', 'dfs'] + _arg_expander(*args)
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderror = proc.communicate()
    if wait:
        proc.wait()
    if proc.returncode != 0:
        raise SystemError(stderror.decode('utf-8'))
    return proc, stdout.decode('utf-8'), stderror.decode('utf-8')


# Columnas de la salida `hdfs dfs -ls`, en orden.
_LEGEND = ('file_type', 'permissions', 'copies', 'user', 'group', 'size',
           'date_and_time', 'path')
# Regex que parsea cada línea de `hdfs dfs -ls`.
_LS_PRINT_PATTERN = re.compile(
    '([d\\-]{1})'
    + '([rwx\\-\\+]{9,10})'
    + '\\s+'
    + '([\\-\\d]{1})'
    + '\\s+'
    + '([a-z0-9]+)'
    + '\\s+'
    + '([a-z]+)'
    + '\\s+'
    + '([0-9]+)'
    + '\\s+'
    + '(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})'
    + '\\s+'
    + '([\\w\\/\\d\\.\\-\\=\\:]+)'
)
# Formato `yyyy-MM-dd HH:mm` que imprime `hdfs dfs -ls`.
_DATETIME_FORMAT = '%Y-%m-%d %H:%M'


def _hdfs_ls(path: str, *args: Union[str, Iterable[str]]) -> List[LsEntry]:
    """``hdfs dfs -ls`` sobre ``path`` parseado a lista de dicts.

    Cada dict tiene las claves de :data:`_LEGEND`; ``date_and_time`` ya es
    un :class:`datetime`.
    """
    args_list = ['-ls', args, path] if args else ['-ls', path]
    _, stdout, _ = hdfs_command(*args_list, wait=True)
    result: List[LsEntry] = []
    for find_ in _LS_PRINT_PATTERN.findall(stdout):
        temp_dict: LsEntry = {}
        for key, value in zip(_LEGEND, find_):
            temp_dict[key] = (datetime.strptime(value, _DATETIME_FORMAT)
                              if key == 'date_and_time' else value)
        result.append(temp_dict)
    return result


def _local_ls(path: str, recursive: bool = False) -> List[LsEntry]:
    """``_ls`` local que devuelve el mismo formato de dict que el de HDFS.

    Parameters
    ----------
    path:
        Directorio a listar (se le quita el prefijo ``file:`` si lo tiene).
    recursive:
        ``True`` equivale al flag ``-R`` (desciende el árbol completo).
    """
    root = _local(path)
    result: List[LsEntry] = []
    if recursive:
        entries = [os.path.join(dirpath, name)
                   for dirpath, dirnames, filenames in os.walk(root)
                   for name in dirnames + filenames]
    else:
        entries = ([os.path.join(root, name) for name in os.listdir(root)]
                   if os.path.isdir(root) else [])
    for full in entries:
        try:
            st = os.stat(full)
        except OSError:
            continue
        result.append({
            'file_type': 'd' if os.path.isdir(full) else '-',
            'permissions': '',
            'copies': '1',
            'user': '',
            'group': '',
            'size': str(st.st_size),
            'date_and_time': datetime.fromtimestamp(st.st_mtime),
            'path': Path(full).as_posix(),
        })
    return result


def _ls(path: str, *args: Union[str, Iterable[str]]) -> List[LsEntry]:
    """Dispatch de ``_ls`` al backend HDFS o local según :func:`is_hdfs_path`."""
    if is_hdfs_path(path):
        return _hdfs_ls(path, *args)
    return _local_ls(path, recursive='-R' in args)


def test(path: str, *args: str) -> bool:
    """``hdfs dfs -test <flag> path``, o su equivalente local.

    Soporta los flags ``-e`` (existe), ``-d`` (directorio) y ``-f``
    (fichero regular). En HDFS devuelve ``True`` si el rc del comando es 0.
    """
    assert args, 'At least one arg is requiered.'
    if not is_hdfs_path(path):
        local = _local(path)
        flag = args[0]
        if flag == '-e':
            return os.path.exists(local)
        if flag == '-d':
            return os.path.isdir(local)
        if flag == '-f':
            return os.path.isfile(local)
        return os.path.exists(local)
    try:
        proc, _, _ = hdfs_command('-test', args, path)
        value = proc.returncode
    except Exception:
        value = 1
    return False if value != 0 else True


def exists(path: str, *args: str) -> bool:
    """``True`` si ``path`` existe."""
    return test(path, '-e')


def is_dir(path: str, *args: str) -> bool:
    """``True`` si ``path`` es un directorio."""
    return test(path, '-d')


def is_file(path: str, *args: str) -> bool:
    """``True`` si ``path`` es un fichero regular."""
    return test(path, '-f')


def list_files(path: str) -> List[str]:
    """Nombres (basename) de las entradas dentro de ``path``."""
    return [file_dict['path'].split('/')[-1] for file_dict in _ls(path)]


def is_successful(path: str) -> bool:
    """``path`` existe (dir) y contiene un fichero ``_SUCCESS``.

    Es la comprobación usada para decidir si un parquet de Spark se
    terminó de escribir correctamente.
    """
    return is_dir(path) and '_SUCCESS' in list_files(path)


def mkdir(path: str, *args: str) -> None:
    """Crea un directorio; ``-p`` crea padres intermedios (HDFS) /
    ``exist_ok`` en local."""
    if is_hdfs_path(path):
        args_list = ['-mkdir', args, path] if args else ['-mkdir', path]
        hdfs_command(*args_list)
    else:
        os.makedirs(_local(path), exist_ok='-p' in args)


def touch(path: str, *args: str) -> None:
    """Crea un fichero vacío (o actualiza su timestamp)."""
    if is_hdfs_path(path):
        args_list = ['-touch', args, path] if args else ['-touch', path]
        hdfs_command(*args_list)
    else:
        Path(_local(path)).touch(exist_ok=True)


def rmdir(path: str, *args: str) -> None:
    """Borra ``path``; con ``-r`` recursivo (o fichero suelto en local)."""
    if is_hdfs_path(path):
        args_list = ['-rm', args, path] if args else ['-rm', path]
        hdfs_command(*args_list)
    else:
        local = _local(path)
        if '-r' in args:
            if os.path.isdir(local):
                shutil.rmtree(local)
            elif os.path.exists(local):
                os.remove(local)
        elif os.path.isdir(local):
            os.rmdir(local)
        elif os.path.isfile(local):
            os.remove(local)


def mv(src: str, dst: str, *args: str) -> None:
    """Mueve ``src`` a ``dst``."""
    if is_hdfs_path(src):
        args_list = ['-mv', args, src, dst] if args else ['-mv', src, dst]
        hdfs_command(*args_list)
    else:
        shutil.move(_local(src), _local(dst))


###############################################################################
# HivePath
###############################################################################

class HivePath(type(PurePosixPath())):
    """``PurePosixPath`` + ``exists``/``ls``/``rm``/``mv`` sobre HDFS o FS local.

    Además del path, puede cargar metadatos del último ``ls`` en el que se
    resolvió (``file_type``, ``size``, ``date_and_time``, ...), guardados
    como atributos del objeto.
    """

    #: Claves de metadatos que un dict de ``ls`` puede poblar.
    DICT_NAMES = ('file_type', 'permissions', 'copies', 'user', 'group',
                  'size', 'date_and_time', 'path')

    def __new__(cls, *pathsegments: PathLike, **kwargs: Any) -> "HivePath":
        return super().__new__(cls, *pathsegments)

    def __init__(self, *pathsegments: PathLike, **kwargs: Any) -> None:
        try:
            super().__init__(*pathsegments)
        except TypeError:
            super().__init__()
        # Metadatos opcionales procedentes de un `ls` (p. ej. en iterdir).
        empty_dict = {key: None for key in self.DICT_NAMES
                      if key not in list(kwargs.keys())}
        empty_dict.update(kwargs)
        empty_dict.pop('path', None)
        self.__dict__.update(empty_dict)

    def exists(self) -> bool:
        """``True`` si la ruta existe en el backend correspondiente."""
        return exists(str(self))

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Crea el directorio; ``parents`` crea los intermedios."""
        if self.exists():
            if not exist_ok:
                raise FileExistsError
            return
        if not parents:
            if not self.parent.exists():
                raise FileNotFoundError('Parent folder does not exist.')
            mkdir(str(self))
        else:
            mkdir(str(self), '-p')

    def _retrieve_own_ls_dict(self) -> Optional[LsEntry]:
        """Popula ``self`` con los metadatos del ``ls`` de su directorio padre."""
        ls_list = _ls(str(self.parent))
        for file_dict in ls_list:
            if self.name == HivePath(file_dict['path']).name:
                file_dict.pop('path')
                self.__dict__.update(file_dict)
                return file_dict
        return None

    def is_dir(self) -> bool:
        """``True`` si la ruta es un directorio."""
        return is_dir(str(self))

    def is_file(self) -> bool:
        """``True`` si la ruta es un fichero regular."""
        return is_file(str(self))

    def is_parquet(self) -> bool:
        """El directorio existe y contiene salida parquet (``_SUCCESS``)."""
        return self.is_successful()

    def _iterdict(self, recursive: bool = False,
                  sorted_by_time: bool = False
                  ) -> Generator[LsEntry, None, None]:
        """Dicts ``ls`` del contenido de este directorio."""
        ls_dir = _ls(str(self), '-R') if recursive else _ls(str(self))
        if sorted_by_time:
            ls_dir = sorted(ls_dir,
                            key=itemgetter('date_and_time', 'path'))
        for file_dict in ls_dir:
            yield file_dict

    def iterdir(self, recursive: bool = False,
                sorted_by_time: bool = False) -> Iterator["HivePath"]:
        """Itera sobre ficheros y directorios contenidos en este directorio."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            yield HivePath(file_dict['path'], **file_dict)

    def listdirs(self, recursive: bool = False,
                 sorted_by_time: bool = False) -> Iterator["HivePath"]:
        """Itera solo sobre los directorios contenidos."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == 'd':
                yield HivePath(file_dict['path'], **file_dict)

    def listfiles(self, recursive: bool = False,
                  sorted_by_time: bool = False) -> Iterator["HivePath"]:
        """Itera solo sobre los ficheros contenidos."""
        for file_dict in self._iterdict(recursive, sorted_by_time):
            if file_dict['file_type'] == '-':
                yield HivePath(file_dict['path'])

    def listparquets(self, recursive: bool = True,
                     sorted_by_time: bool = False) -> Iterator["HivePath"]:
        """Itera sobre los directorios-parquet (los que contienen ``_SUCCESS``)."""
        for hive in self.listfiles(recursive, sorted_by_time):
            if hive.name == '_SUCCESS':
                yield hive.parent

    def glob(self, pattern: str) -> Iterator["HivePath"]:
        """Itera el subárbol y devuelve las rutas que matchean ``pattern``."""
        if not pattern:
            raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        for hive in self.iterdir(recursive=True):
            if fnmatch(hive.name, pattern):
                yield hive

    def touch(self, exist_ok: bool = True) -> None:
        """Crea un fichero vacío en esta ruta."""
        if not exist_ok and self.exists():
            raise FileExistsError
        touch(str(self))

    def rmdir(self, recursive: bool = False, skip_trash: bool = False) -> None:
        """Borra el directorio; ``recursive`` para árboles, ``skip_trash``
        para ``-skipTrash`` en HDFS."""
        if not self.is_dir():
            raise TypeError('It has to be a directory.')
        arguments: List[str] = []
        if recursive:
            arguments += ['-r']
        if skip_trash:
            arguments += ['-skipTrash']
        rmdir(str(self), *arguments)

    def rm(self) -> None:
        """Borra el fichero apuntado por esta ruta."""
        if not self.is_file():
            raise TypeError('It has to be a file.')
        rmdir(str(self))

    def mv(self, destiny: Union['HivePath', str],
           overwrite: bool = False) -> None:
        """Mueve este path dentro del directorio ``destiny``."""
        if isinstance(destiny, str):
            destiny = HivePath(destiny)
        if overwrite:
            try:
                destiny.joinpath(self.name).rmdir(recursive=True,
                                                  skip_trash=False)
            except Exception:
                pass
        destiny.mkdir(exist_ok=True, parents=True)
        mv(str(self), str(destiny))

    def rename(self, new_name: str) -> 'HivePath':
        """Renombra la última componente del path y devuelve el nuevo objeto."""
        mv(str(self), str(self.with_name(new_name)))
        return HivePath(self.with_name(new_name))

    def is_successful(self) -> bool:
        """La ruta existe como directorio y contiene un fichero ``_SUCCESS``.

        Es la comprobación de "parquet completamente escrito" usada por los
        steps para decidir recarga vs. recomputo.
        """
        return is_dir(str(self)) and '_SUCCESS' in [
            hive.name for hive in self.iterdir()]
