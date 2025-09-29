import os, sys

from py4j.compat import long
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, LongType, DateType, StringType, IntegerType, TimestampType, \
    BooleanType

from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country
from helloSpark_lib.logger import Log4J

if __name__ == "__main__":
    """1. Read CSV, JSON, Parquet files
       2. Create Spark dataFrame Schema
       3. Write DataFrameWriter object"""
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

    # add schema in Programmatically
    manualSchema = StructType([
        StructField("Call Number", IntegerType(), True),
        StructField("Unit ID", StringType(), True),
        StructField("Incident Number", IntegerType(), True),
        StructField("Call Type", StringType(), True),
        StructField("Call Date", StringType(), True),
        StructField("Watch Date", StringType(), True),
        StructField("Received DtTm", StringType(), True),
        StructField("Entry DtTm", StringType(), True),
        StructField("Dispatch DtTm", StringType(), True),
        StructField("Response DtTm", StringType(), True),
        StructField("On Scene DtTm", StringType(), True),
        StructField("Transport DtTm", StringType(), True),
        StructField("Hospital DtTm", StringType(), True),
        StructField("Call Final Disposition", StringType(), True),
        StructField("Available DtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zipcode of Incident", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("Station Area", IntegerType(), True),
        StructField("Box", IntegerType(), True),
        StructField("Original Priority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("Final Priority", IntegerType(), True),
        StructField("ALS Unit", BooleanType(), True),
        StructField("Call Type Group", StringType(), True),
        StructField("Number of Alarms", IntegerType(), True),
        StructField("Unit Type", StringType(), True),
        StructField("Unit sequence in call dispatch", IntegerType(), True),
        StructField("Fire Prevention District", IntegerType(), True),
        StructField("Supervisor District", IntegerType(), True),
        StructField("Neighborhooods - Analysis Boundaries", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("case_location", StringType(), True),
        StructField("data_as_of", StringType(), True),
        StructField("data_loaded_at", StringType(), True),
])
    # read csv file;
    # !!! when error occurs, stop the code immediately
    csv_df = spark.read\
        .format("csv")\
        .option("header", "true")\
        .schema(manualSchema)\
        .option("mode","FAILFAST") \
        .load("data/Fire.csv")
    sp_df = (csv_df
          .withColumn("Call Date", to_date("Call Date", "MM/dd/yyyy"))
          .withColumn("Watch Date", to_date("Watch Date", "MM/dd/yyyy"))
          .withColumn("data_as_of", to_timestamp("data_as_of", "yyyy MMM dd hh:mm:ss a"))
          .withColumn("data_loaded_at", to_timestamp("data_loaded_at",  "yyyy MMM dd hh:mm:ss a"))
          )

    sp_df.show(5)
    logger.info("csv:"+sp_df.schema.simpleString())

    # write this csv file to a json file and parquet
    sp_df.write.mode("overwrite").json("data/Fire.json")
    sp_df.write.mode("overwrite").parquet("data/Fire.parquet")
    # DataFrame.write.format("json")
    #     .mode(saveMode)
    #     .option("path", "data")
    #     .save()

    # read json file
    read_Json = spark.read.format("json").load("data/Fire.json")
    read_Json.show(5)
    logger.info("json schema: " + read_Json.schema.simpleString())

    # read parquet file
    read_parquet = spark.read.format("parquet").load("data/Fire.parquet")
    read_parquet.show(5)
    logger.info("parquet schema :"+read_parquet.schema.simpleString())
