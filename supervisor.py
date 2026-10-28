#!/usr/bin/env python3
"""Supervisor autónomo del pipeline datapull basado en steps.

Lanza ``SUPERVISOR_COMMAND`` como subproceso y lo vigila:

- **Muerte del proceso** (OOM-kill, SIGKILL, excepción no manejada): lo
  reinicia. Como las salidas de los steps se recargan de parquet cuando
  ``is_dynamic`` está activo, el proceso nuevo de facto reanuda en la
  etapa en la que iba.
- **Etapa atascada**: si el pipeline pasa más de
  ``SUPERVISOR_STEP_TIMEOUT`` segundos en el mismo step, se consulta la
  Spark UI del driver: tareas activas/completadas creciendo -> sigue
  computando, se espera; sin trabajo vivo durante
  ``SUPERVISOR_STALL_TIMEOUT`` -> zombi: matar y reiniciar.

El progreso se infiere del stdout del hijo (mensajes ``Executing step:`` /
``Step finished:`` que loguea :class:`libs.steps.Step`); el pipeline no
necesita ningún cambio.

Variables de entorno:

=============================  ============================================
``SUPERVISOR_COMMAND``         comando a vigilar  (default ``python main_dynamic.py``)
``SUPERVISOR_STEP_TIMEOUT``    máx. segundos en el mismo step (default 7200)
``SUPERVISOR_STALL_TIMEOUT``   segundos sin progreso de tareas -> zombi (600)
``SUPERVISOR_WATCHDOG_INTERVAL``  período del watchdog en segundos (30)
``SUPERVISOR_MAX_RESTARTS``    reinicios antes de rendirse (10)
``PYSPARK_PORT``               puerto de la Spark UI del driver (4040)
``SUPERVISOR_QUEUES``          colas YARN candidatas (CSV); en cada
                               reinicio se elige la de más capacidad libre
                               según ``yarn queue -status`` (vacío = fija)
``SUPERVISOR_CLEANUP_SCRIPT``  script a ejecutar UNA vez cuando el
                               pipeline termina con éxito (vacío = ninguno)
=============================  ============================================

Usage::

    python supervisor.py
"""
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from typing import Any, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from libs.hive_path import os_mode

#: ``True`` en Windows nativo (sin killpg ni shlex POSIX); unix/wsl -> POSIX.
_IS_WINDOWS = os_mode() == "windows"

#: "Executing step: <name>" emitido por Step.execute().
STEP_START_RE = re.compile(r"Executing step:\s*(\S+)")
#: "Step finished: <name>" emitido por Step.execute().
STEP_DONE_RE = re.compile(r"Step finished:\s*(\S+)")

#: Comando supervisado.
COMMAND = os.environ.get("SUPERVISOR_COMMAND", "python main_dynamic.py")
#: Tiempo máximo permitido dentro de un mismo step antes de sospechar zombi.
STEP_TIMEOUT = float(os.environ.get("SUPERVISOR_STEP_TIMEOUT", 2 * 3600))
#: Tiempo sin progreso de tareas Spark que declara la sesión zombi.
STALL_TIMEOUT = float(os.environ.get("SUPERVISOR_STALL_TIMEOUT", 600))
#: Período del bucle del watchdog.
WATCHDOG_INTERVAL = float(os.environ.get("SUPERVISOR_WATCHDOG_INTERVAL", 30))
#: Reinicios permitidos antes de rendirse.
MAX_RESTARTS = int(os.environ.get("SUPERVISOR_MAX_RESTARTS", 10))
#: Puerto de la Spark UI del driver.
SPARK_UI_PORT = int(os.environ.get("PYSPARK_PORT", 4040))
#: Colas YARN candidatas (CSV o espacios): en cada (re)inicio se exporta al
#: hijo la de más capacidad libre vía ``PYSPARK_QUEUE``. Vacío -> fija.
QUEUES = [q.strip() for q in
          os.environ.get("SUPERVISOR_QUEUES", "").replace(",", " ").split()
          if q.strip()]
#: Script de limpieza a ejecutar una única vez tras éxito del pipeline
#: (p.ej. borrado de dirs temporales/staging; el script se auto-borra).
CLEANUP_SCRIPT = os.environ.get("SUPERVISOR_CLEANUP_SCRIPT", "")

#: (numActiveTasks, numCompleteTasks) agregados de los stages activos.
TaskCounts = Tuple[int, int]


def log(msg: str) -> None:
    """Imprime una línea de log del supervisor con timestamp."""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] [supervisor] {msg}", flush=True)


class PipelineMonitor:
    """Estado de progreso del proceso hijo, alimentado línea a línea desde su
    stdout.

    Attributes
    ----------
    current_step:
        Step en ejecución (o ``"(startup)"`` fuera de un step).
    step_started_at:
        Timestamp en que empezó el step actual.
    finished_steps:
        Nombres de steps ya terminados.
    last_tasks:
        Última lectura ``(activas, completadas)`` de la Spark UI.
    last_progress_at:
        Última vez que se observó progreso (nuevo step o más tareas
        completadas).
    """

    def __init__(self) -> None:
        self.current_step: str = "(startup)"
        self.step_started_at: float = time.time()
        self.finished_steps: Set[str] = set()
        self.last_tasks: Optional[TaskCounts] = None
        self.last_progress_at: float = time.time()

    def feed_line(self, line: str) -> None:
        """Procesa una línea del stdout del hijo buscando marcas de step."""
        match = STEP_START_RE.search(line)
        if match:
            self.current_step = match.group(1)
            self.step_started_at = time.time()
            self.last_progress_at = time.time()
            return
        match = STEP_DONE_RE.search(line)
        if match:
            self.finished_steps.add(match.group(1))
            self.current_step = "(startup)"
            self.step_started_at = time.time()
            self.last_progress_at = time.time()


def _http_json(url: str, timeout: float = 5.0) -> Any:
    """GET JSON con timeout (API REST de la Spark UI)."""
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def spark_task_counts(port: int) -> Optional[TaskCounts]:
    """``(numActiveTasks, numCompleteTasks)`` sobre los stages activos de la app.

    Returns
    -------
    Optional[TaskCounts]
        ``None`` si la UI del driver no responde; ``(0, 0)`` si no hay
        stages activos.
    """
    try:
        apps = _http_json(f"http://localhost:{port}/api/v1/applications")
        if not apps:
            return None
        app_id = apps[0]["id"]
        stages = _http_json(
            f"http://localhost:{port}/api/v1/applications/{app_id}/stages?status=active")
        active = sum(s.get("numActiveTasks", 0) for s in stages)
        complete = sum(s.get("numCompleteTasks", 0) for s in stages)
        return (active, complete)
    except Exception:
        return None


def is_zombie(monitor: PipelineMonitor) -> bool:
    """Decide si la sesión está zombi (sin progreso) una vez que el step actual
    excede ``STEP_TIMEOUT``.

    Prefiere evidencia de la Spark UI; si la UI no responde, cae a la
    heurística pura de tiempo por step.

    Returns
    -------
    bool
        ``True`` si hay que matar y reiniciar el proceso.
    """
    now = time.time()
    if now - monitor.step_started_at < STEP_TIMEOUT:
        return False
    counts = spark_task_counts(SPARK_UI_PORT)
    if counts is None:
        return True
    if monitor.last_tasks is None:
        monitor.last_tasks = counts
        return False
    if counts[1] > monitor.last_tasks[1]:
        monitor.last_progress_at = now
    monitor.last_tasks = counts
    return now - monitor.last_progress_at >= STALL_TIMEOUT


def _drain_stdout(proc: "subprocess.Popen[str]",
                  monitor: PipelineMonitor) -> None:
    """Hilo: copia el stdout del hijo al propio y alimenta el monitor."""
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        monitor.feed_line(line)


def _kill_tree(proc: "subprocess.Popen[str]") -> None:
    """Mata el proceso hijo y su árbol (``killpg`` en POSIX, terminate/kill en
    Windows)."""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, AttributeError, PermissionError):
        try:
            proc.terminate()
        except ProcessLookupError:
            return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, AttributeError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass


# ---------------------------------------------------------------------------
# Selección de cola YARN por capacidad disponible
# ---------------------------------------------------------------------------

#: Campos porcentuales de `yarn queue -status`: "Capacity : 20.0%".
_QUEUE_FIELD_RE = re.compile(
    r"(?im)^\s*([a-z][a-z ]*?)\s*:\s*(-?[0-9.]+)\s*%?\s*$")
#: Recursos del fair scheduler: "Used Resources : <memory:4096, vCores:4>".
_QUEUE_MEM_RE = re.compile(
    r"(?im)^\s*([a-z][a-z ]*?)\s*:\s*<memory:([0-9]+)[^>]*>")


def _yarn_queue_status(queue: str) -> Optional[str]:
    """Salida de ``yarn queue -status <queue>`` o ``None`` si no se obtuvo.

    Se intenta el nombre tal cual y después con prefijo ``root.`` —
    algunos clusters solo aceptan el path absoluto de la cola.
    """
    for name in (queue, f"root.{queue}"):
        try:
            proc = subprocess.run(
                ["yarn", "queue", "-status", name],
                capture_output=True, text=True, timeout=30)
        except Exception:
            return None            # sin CLI yarn (local/windows)
        if proc.returncode == 0 and "Queue Information" in proc.stdout:
            return proc.stdout
    return None


def _queue_available(status_out: str) -> Optional[float]:
    """Capacidad libre reportada por ``yarn queue -status`` (mayor = mejor).

    - Capacity scheduler (porcentajes): ``efectivo_max - usado`` en
      puntos porcentuales; max <= 0 (ilimitado) cae a ``capacity``.
    - Fair scheduler (``<memory:N>``): fracción libre del fair share,
      ``1 - used_mem / fair_mem`` en [0, 1].
    """
    fields = {m.group(1).strip().lower(): float(m.group(2))
              for m in _QUEUE_FIELD_RE.finditer(status_out)}
    used = next((fields[k] for k in
                 ("absolute used capacity", "used capacity",
                  "current capacity") if k in fields), None)
    cap = next((fields[k] for k in
                ("absolute maximum capacity", "absolute capacity",
                 "maximum capacity", "capacity")
                if fields.get(k, 0) > 0), None)
    if used is not None:
        return (cap if cap is not None else 100.0) - used
    mem = {m.group(1).strip().lower(): float(m.group(2))
           for m in _QUEUE_MEM_RE.finditer(status_out)}
    used_mem = mem.get("used resources")
    if used_mem is None:
        return None
    fair = next((mem[k] for k in
                 ("max resources", "maximum resources",
                  "steady fair share", "instantaneous fair share")
                 if mem.get(k, 0) > 0), None)
    if fair is None:
        return 1.0              # sin límite reportado -> libre
    return 1.0 - min(used_mem / fair, 1.0)


def pick_queue() -> Optional[str]:
    """Cola con más capacidad libre, o ``None`` si no se midió ninguna.

    Best-effort: un fallo de ``yarn queue -status`` nunca bloquea el
    reinicio — el hijo conserva el ``PYSPARK_QUEUE`` heredado.
    """
    best, best_free = None, float("-inf")
    for queue in QUEUES:
        status = _yarn_queue_status(queue)
        free = _queue_available(status) if status else None
        if free is None:
            log(f"queue {queue}: sin datos (`yarn queue -status` falló)")
            continue
        log(f"queue {queue}: capacidad libre {free:g}")
        if free > best_free:
            best, best_free = queue, free
    return best


def _run_cleanup_script() -> None:
    """Ejecuta ``SUPERVISOR_CLEANUP_SCRIPT`` si existe (best-effort)."""
    if not CLEANUP_SCRIPT or not os.path.isfile(CLEANUP_SCRIPT):
        return
    script = os.path.abspath(CLEANUP_SCRIPT)
    try:
        # cwd=dir del script + basename como argv: evita traducir rutas
        # absolutas entre python-Windows y bash-posix (msys/wsl)
        proc = subprocess.run(
            ["bash", os.path.basename(script)],
            cwd=os.path.dirname(script), timeout=600,
            capture_output=True, text=True)
        for line in (proc.stdout or "").strip().splitlines():
            log(f"[cleanup] {line}")
        if proc.returncode != 0:
            log(f"Cleanup script rc={proc.returncode}: "
                f"{(proc.stderr or '').strip()}")
    except Exception as exc:
        log(f"Cleanup script falló ({exc}); se omite la limpieza")


def run_child(monitor: PipelineMonitor) -> int:
    """Lanza el hijo y lo vigila.

    Returns
    -------
    int
        Exit code del proceso, o ``-9`` si se mató por zombi.
    """
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    queue = pick_queue()
    if queue:
        env["PYSPARK_QUEUE"] = queue
        log(f"Cola YARN seleccionada para este intento: {queue}")
    kwargs: dict = {}
    if not _IS_WINDOWS:
        kwargs["start_new_session"] = True   # grupo propio: killpg mata el árbol
    proc = subprocess.Popen(
        COMMAND if _IS_WINDOWS else shlex.split(COMMAND),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        shell=_IS_WINDOWS,
        **kwargs,
    )
    threading.Thread(target=_drain_stdout, args=(proc, monitor), daemon=True).start()
    while proc.poll() is None:
        if is_zombie(monitor):
            log(f"Zombie session detected in step '{monitor.current_step}' "
                f"(no task progress in {STALL_TIMEOUT:.0f}s). Killing and restarting...")
            _kill_tree(proc)
            return -9
        time.sleep(WATCHDOG_INTERVAL)
    return proc.returncode


def main() -> int:
    """Bucle del supervisor: corre el hijo y lo reinicia hasta
    ``MAX_RESTARTS``.

    Returns
    -------
    int
        0 si el pipeline terminó bien; 1 si se agotaron los reinicios.
    """
    log(f"Supervising '{COMMAND}' "
        f"(step_timeout={STEP_TIMEOUT:.0f}s, stall_timeout={STALL_TIMEOUT:.0f}s, "
        f"max_restarts={MAX_RESTARTS}, ui_port={SPARK_UI_PORT})")
    restarts = 0
    while True:
        monitor = PipelineMonitor()
        return_code = run_child(monitor)
        if return_code == 0:
            log("Pipeline finished successfully.")
            _run_cleanup_script()
            return 0
        restarts += 1
        step = monitor.current_step
        log(f"Process exited (rc={return_code}) during step '{step}'. "
            f"Restart {restarts}/{MAX_RESTARTS}; written parquets are reloaded.")
        if restarts > MAX_RESTARTS:
            log("Max restarts reached; giving up.")
            return 1
        time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
