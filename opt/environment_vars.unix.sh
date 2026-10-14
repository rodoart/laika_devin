#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.unix.sh — datapull local (modo "unix"/"wsl").
#
# Variables para correr el datapull y sus tests SIN clúster (Spark
# local[*] sobre el FS local). Es el modo por defecto fuera del clúster;
# el equivalente en clúster es run_fs.sh (spark-submit YARN).
#
# Uso:
#   source opt/environment_vars.unix.sh
#   python main_dynamic.py --local --no-email
#   python -m pytest tests/local/unit_tests -q
# =============================================================================

# Plataforma/backend: local, sin HDFS (unix por defecto; APP_MODE=wsl si
# WSL lo requiere — mismo comportamiento de backend).
export APP_OS_MODE="${APP_MODE:-unix}"
export LAIKA_FS_MODE="${LAIKA_FS_MODE:-unix}"

# Spark local: mismo intérprete para driver y workers (evita
# PYTHON_VERSION_MISMATCH cuando `python3` del PATH es otra versión).
CURRENT_SCRIPT_DIR=$(dirname "$(realpath "${BASH_SOURCE[0]}")")
export LAIKA_WORKSPACE_DIR=$(realpath "$CURRENT_SCRIPT_DIR/..")

# venv local (python3 -m venv): prioridad VENV_DIR_LINUX, luego .venv del
# workspace, luego el venv compartido de WSL ~/venvs/laika_framework.
if [ -n "${VENV_DIR_LINUX:-}" ] && [ -x "${VENV_DIR_LINUX}/bin/python" ]; then
    _VENV_PY="${VENV_DIR_LINUX}/bin/python"
elif [ -x "${LAIKA_WORKSPACE_DIR}/.venv/bin/python" ]; then
    _VENV_PY="${LAIKA_WORKSPACE_DIR}/.venv/bin/python"
elif [ -x "${HOME}/venvs/laika_framework/bin/python" ]; then
    _VENV_PY="${HOME}/venvs/laika_framework/bin/python"
else
    _VENV_PY="$(command -v python3 || command -v python)"
fi
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${_VENV_PY}}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_PYTHON}"

# Java: JRE local sin sudo (~/opt/jre o .local/jre) si el sistema no tiene.
if ! command -v java >/dev/null 2>&1; then
    for _jre in "${HOME}/opt/jre" "${LAIKA_WORKSPACE_DIR}/.local/jre" \
                "${LAIKA_WORKSPACE_DIR}/framework/.local/jre"; do
        if [ -x "${_jre}/bin/java" ]; then
            export JAVA_HOME="${_jre}"
            export PATH="${JAVA_HOME}/bin:${PATH}"
            break
        fi
    done
fi

# Harness local
export LAIKA_CONFIG_DIR="${LAIKA_CONFIG_DIR:-tests/local/config_dyn}"
export LAIKA_DISABLE_EMAIL="${LAIKA_DISABLE_EMAIL:-1}"

echo "APP_OS_MODE: ${APP_OS_MODE}"
echo "LAIKA_FS_MODE: ${LAIKA_FS_MODE}"
echo "PYSPARK_PYTHON: ${PYSPARK_PYTHON}"
echo "JAVA_HOME: ${JAVA_HOME:-<java del sistema>}"
echo "LAIKA_CONFIG_DIR: ${LAIKA_CONFIG_DIR}"
