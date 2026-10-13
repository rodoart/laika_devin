#!/bin/sh
#
# spark_exe.sh
#
# Usage: spark_exe.sh FID ENVIRONMENT_NAME job
#
# Example: sh spark_exe.sh gcgaacqmpysp PROD data_pull

#export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
#export PYSPARK_PYTHON=/opt/cloudera/parcels/citiconda3/bin/python
#export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/citiconda3/bin/python
#pyspark3
#export SPARK_HOME=/opt/cloudera/parcels/SPARK3/lib/spark3
#export PYSPARK_PYTHON=/opt/cloudera/parcels/citiconda39-9.9.34/bin/

export JAVA_HOME=/usr/java/default
export SPARK_HOME=/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7191000.12-1-1.p0.70675458/lib/spark3
export PYLIB=/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7191000.12-1-1.p0.70675458/lib/spark3/python/lib
export PYSPARK_PYTHON=/opt/cloudera/parcels/citiconda39-9.9.46/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/citiconda39-9.9.46/bin/python
export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/lib/hadoop/etc/hadoop
export HIVE_CONF_DIR=/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/lib/hive/conf

##Define routes to make the execution
export actual_route=$PWD
export main_script="main.py"
export LOGS_PATH=${actual_route}/logs
mkdir -p $LOGS_PATH

# r
# export LOGS_PATH=/data/1/gcgaacqmpysp/bb25173/Test_Dapull/DatapullFer/logs/
export spark_conf="/usr/bin/spark3-submit --conf spark.ui.port=12888 --queue root.datalabs"
export file_to_execute=${actual_route}/$main_script
# export file_to_execute="/data/1/gcgaacqmpysp/bb25173/Test_Dapull/DataPull/main.py"
# Selecting the impala setting to refresh the metadata


# Authenticate with Kerberos

# Set any needed environment variables
# Create a log file based on the time of day
export log_filename_err="DataPull_err_$(date '+%Y-%m-%d_%H-%M-%S')"
export log_filename_std="DataPull_std_$(date '+%Y-%m-%d_%H-%M-%S')"
echo saving stderr to ${LOGS_PATH}
echo saving stdout to ${LOGS_PATH}

# Spark Submit
# /usr/bin/spark-submit\
#   --queue root.datalabs\
#   --master yarn\
#   --deploy-mode client\
#   main.py\
#       >"$LOGS_PATH"/"$log_filename_err"\
#       2>"$LOGS_PATH"/"$log_filename_std"

echo $SPARK_HOME
nohup ${spark_conf} ${file_to_execute} ${python_parse}  > ${LOGS_PATH}/${log_filename_err}.out 2> ${LOGS_PATH}/${log_filename_std}.out &
