import os
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helloSpark_lib.logger import Log4J
from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country



if __name__ == "__main__":
    """Pytest the function: we can observe the result, but we also need to do Unit Test"""
    conf = get_spark_app_config()
    log4j2_path = os.path.abspath("log4j2.properties")
    spark = (SparkSession.builder
             .config(conf=conf)
             .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
             .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
             .getOrCreate())

    logger = Log4J(spark)
    confInf = spark.sparkContext.getConf()
    logger.info("starting new spark")

    # read raw data
    df = spark.read.text("data/apache_logs.txt")
    df.printSchema()

    # IP, client, user, datetime, cmd, request, protocol, status, bytes, referrer, userAgent
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    #regexp()
    partial_df = df.select(regexp_extract("value", log_reg, 1).alias("ip"), \
              regexp_extract("value", log_reg,4).alias("datetime"), \
              regexp_extract("value", log_reg,6).alias("request"), \
              regexp_extract("value",log_reg,10).alias("referrer"))

    partial_df.printSchema()
    partial_df \
        .where("trim(referrer) != '-' ")\
        .withColumn("referrer", substring_index("referrer", "/",3))\
        .groupBy("referrer").count().show(100, truncate=False)
