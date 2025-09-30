import os, sys

from py4j.compat import long
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, spark_partition_id
from pyspark.sql.types import StructType, StructField, LongType, DateType, StringType, IntegerType, TimestampType, \
    BooleanType

from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country
from helloSpark_lib.logger import Log4J

if __name__ == "__main__":
    """create managed tables in db"""
    conf = get_spark_app_config()
    log4j2_path = os.path.abspath("log4j2.properties")
    # # 👇 Add this line BEFORE enableHiveSupport()
    #   .config("spark.sql.warehouse.dir", "/absolute/path/to/warehouse")
    # The Spark depends on Hive Metastore: enableHiveSupport()
    spark = (SparkSession.builder
            .config(conf=conf)
            .enableHiveSupport()
            .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            .getOrCreate())
    
    logger = Log4J(spark)
    confInf = spark.sparkContext.getConf()

    # logger.info(confInf.toDebugString())
    logger.info("starting new spark")

    # read parquet file
    read_parquet = spark.read.format("parquet").load("data/Fire.parquet")

    # Set another DB as the current session, First, create a DB
    spark.sql("CREATE DATABASE IF NOT EXISTS Fire_DB")
    spark.catalog.setCurrentDatabase("Fire_DB")

    # Write data to table
    read_parquet.write.format("csv").mode("overwrite") \
        .bucketBy(4, "Call Type", "Call Date") \
        .sortBy("Call Type", "Call Date") \
        .saveAsTable("Fire_tbl") # 'save' does not support bucketBy right now.
    logger.info(spark.catalog.listTables("Fire_DB"))

