#!/bin/sh

#######################################################
# RUN BASH TERMINAL
#######################################################
if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

#######################################################
# LOOKING FOR ENVIRONMENT VARIABLES DO NOT MODIFY THIS SECTION
#######################################################

# SEARCHING FO ENVIRONMENT VARIABLES

UNIX_CURRENT_SCRIPT_DIR=$(dirname "$(realpath "$0")")
UNIX_TESTING_DIR=$(realpath "$UNIX_CURRENT_SCRIPT_DIR/../../")
PANOPTO_UNIX_WORKSPACE_DIR=$(realpath "$UNIX_CURRENT_SCRIPT_DIR/..")
export PANOPTO_UNIX_ENVIRONMENT_VARS="${PANOPTO_UNIX_WORKSPACE_DIR}/config/environment_vars.sh"

source "${PANOPTO_UNIX_ENVIRONMENT_VARS}"

#######################################################
# VIRTUAL ENVIRONMENT
#######################################################

# Conda setup
$UNIX_CONDA init bash
eval "$($UNIX_CONDA shell.bash hook)"

# Activate virtual environment
conda activate $VENV_NAME

# ----------------------------------------------------------------------------
# Check if the virtual environment tar.gz file exists in HDFS
# ----------------------------------------------------------------------------
# hdfs dfs -rm -f -r -skipTrash "${PANOPTO_HIVE_VENV_TAR_GZ}"
if hdfs dfs -test -e "${PANOPTO_HIVE_VENV_TAR_GZ}"; then
    echo "Virtual environment already exists in HDFS: ${PANOPTO_HIVE_VENV_TAR_GZ}"
else
    echo "Virtual environment does not exist in HDFS. Uploading: ${PANOPTO_HIVE_VENV_TAR_GZ}"
    # Remove the existing tar.gz file if it exists
    rm -f "${PANOPTO_UNIX_VENV_TAR_GZ}"
    conda-pack -n $VENV_NAME -o "${PANOPTO_UNIX_VENV_TAR_GZ}"
    # Create the parent directory in HDFS if it doesn't exist
    hdfs dfs -mkdir -p "${PANOPTO_HIVE_VENV_TAR_GZ_PARENT_DIR}"
    hdfs dfs -put -f "${PANOPTO_UNIX_VENV_TAR_GZ}" "${PANOPTO_HIVE_VENV_TAR_GZ_PARENT_DIR}"
    # Changing permissions to make it readable by all users
    hdfs dfs -chmod -R o+rx "${PANOPTO_HIVE_VENV_TAR_GZ_PARENT_DIR}"
fi

#######################################################
# NOTEBOOK
#######################################################
cd $PANOPTO_UNIX_WORKSPACE_DIR
python
