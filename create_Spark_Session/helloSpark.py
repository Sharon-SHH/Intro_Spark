import os
from pyspark.sql import SparkSession

from lib.utils import get_spark_app_config
from lib.logger import Log4J

if __name__ == "__main__":
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
    logger.info(confInf.toDebugString())
    logger.info("starting new spark")
    logger.info("finish new spark")
