import os
from pyspark.sql import SparkSession

from lib.logger import Log4J
log4j2_path = os.path.abspath("log4j2.properties")

spark = (SparkSession.builder
         .appName("NewSpark")
         .master("local[3]")
         .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
         .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
         .getOrCreate())
logger = Log4J(spark)
logger.info("starting spark")
logger.info("finish spark")