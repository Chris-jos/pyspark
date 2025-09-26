import os
os.environ[
        "PYSPARK_PYTHON"
    ] = "/usr/local/bin/python"
from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, collect_list
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import time
from functools import reduce
import time
from datetime import datetime, timedelta
from pyspark.sql.types import *

d_config = {     
  "spark.executor.memory": "20g",
  "spark.executor.memoryOverhead": "11g",
  "spark.dynamicAllocation.initialExecutors": 2,
  "spark.driver.memory": "20g",
  "spark.driver.maxResultSize": "11g",
  "spark.kryoserializer.buffer.max": "1g",
  "spark.default.parallelism": 1350,
  "spark.sql.shuffle.partitions": 1350,
  "spark.driver.cores": 8,
  "spark.executor.cores": 4,
  "spark.app.name": "final_table",
  "spark.dynamicAllocation.maxExecutors": 40,
  #Set to False to supress the spark progress bar:
  "spark.ui.showConsoleProgress": False,
  "spark.debug.maxToStringFields": 300,
  #Setting below configs for network issues
  "spark.network.timeout": 300000,
  "spark.shuffle.sasl.timeout": 60000,
  "spark.max.fetch.failures.per.stage": 10,
  "spark.rpc.io.serverThreads": 64,
  "spark.shuffle.file.buffer": "1m",
  "spark.unsafe.sorter.spill.reader.buffer.size": "1m",
  "spark.io.compression.lz4.blockSize": "512k",
  "spark.executor.heartbeatInterval": 200000,
  "spark.scheduler.listenerbus.eventqueue.capacity": 20000,
  "spark.sql.autoBroadcastJoinThreshold": -1,
  "spark.sql.broadcastTimeout": 36000,
  "spark.sql.files.maxPartitionBytes": 120000000,
  "spark.sql.codegen.wholeStage": False,
}

def create_spark_session():
	conf = SparkConf().set("spark.yarn.queue", 'root.cpa')
	for key, value in d_config.items():
		conf = conf.set(key, value)
	spark = SparkSession \
		.builder \
		.enableHiveSupport() \
		.config(conf=conf) \
		.getOrCreate()

	return spark

