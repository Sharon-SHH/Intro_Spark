import os, sys

from py4j.compat import long
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, spark_partition_id
from pyspark.sql.types import StructType, StructField, LongType, DateType, StringType, IntegerType, TimestampType, \
    BooleanType

from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country
from helloSpark_lib.logger import Log4J

if __name__ == "__main__":
    """partitionBy(); maxRecordsPerFile:maximum 900 records"""
    conf = get_spark_app_config()
    log4j2_path = os.path.abspath("log4j2.properties")
    print(log4j2_path)
    spark = (SparkSession.builder
            .config(conf=conf)
            .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            .getOrCreate())
    
    logger = Log4J(spark)
    confInf = spark.sparkContext.getConf()

    # pycharm: configuration set parameter: data/Fire.csv

    # logger.info(confInf.toDebugString())
    logger.info("starting new spark")

    # read parquet file
    read_parquet = spark.read.format("parquet").load("data/Fire.parquet")
    read_parquet.show(5)
    logger.info("parquet schema :"+read_parquet.schema.simpleString())

    logger.info("num partitions :"+str(read_parquet.rdd.getNumPartitions()))
    read_parquet.groupBy(spark_partition_id()).count().show()
    partitionedDF = read_parquet.repartition(5)
    logger.info("num partitions after:" + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # partitionBy(); maxRecordsPerFile:maximum 900 records
    read_parquet.write.format("json").mode("overwrite").option("path", "data/json")\
        .partitionBy("Call Type", "Call Date")\
        .option("maxRecordsPerFile", 900) \
        .save()
    # read_parquet.write.format("json").mode("overwrite").option("path", "data/json1") \
    #     .bucketBy(4, "Call Type", "Call Date") \
    #     .option("maxRecordsPerFile", 900) \
    #     .save() # 'save' does not support bucketBy right now.

