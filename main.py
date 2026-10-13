from datapull import DataPull
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import traceback
import json
import os
import sys

from pyspark.sql import SparkSession

with open("config/configuration.json") as a:
    config_rigid = json.load(a)
with open("config/config_custom.json") as a:
    config_custom = json.load(a)
##### Definimos nombre del datapull #####
app_name = "DataPull"
if type(config_custom["sources"]) is str:
    app_name += "_" + config_custom["sources"]
else:
    for source in config_custom["sources"]:
        app_name += "_" + source
app_name += "_" + config_custom["year_id"]
##### Por lo pronto la configuración es manual, luego cambiar por la clase #####

conf = SparkConf().\
setAppName(f'{app_name}').\
set('spark.dynamicAllocation.minExecutors','15').\
set('spark.dynamicAllocation.maxExecutors',"30").\
set('spark.default.parallelism','30').\
set("spark.sql.shuffle.partitions",'30').\
set("spark.executor.instances","30").\
set("spark.executor.cores","3").\
set("spark.executor.memory","12G").\
set("spark.executor.memoryOverhead","24G").\
set("spark.driver.memory","100G").\
set('spark.yarn.queue','root.datalabs').\
set("spark.sql.autoBroadcastJoinThreshold","-1").\
set("spark.sql.execution.arrow.enabled", "true").\
set("spark.driver.maxResultSize","0G").\
set("spark.kryoserializer.buffer.max", "1G").\
set("spark.speculation","true").\
set('spark.ui.port', '9812').\
set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/path/to/log4j.properties")


#sqlContext = HiveContext(sc)
os.environ['HADOOP_CONF_DIR'] = "/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/lib/hadoop/etc/hadoop"

spark_session_builder = lambda : SparkSession.builder.appName(app_name).config(conf=conf).enableHiveSupport().getOrCreate()

def main() -> None:
    data_pull = DataPull(config_rigid, config_custom, spark_session_builder)
    try:
        data_pull.run()
    except Exception as e:
        traceback_output = traceback.format_exc()
        print(traceback_output)

if __name__ == '__main__':
    main()
