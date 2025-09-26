import os, sys
from pyspark.sql import SparkSession

from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country
from helloSpark_lib.logger import Log4J

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

    # pycharm: configuration set parameter: data/Fire.csv

    # logger.info(confInf.toDebugString())
    logger.info("starting new spark")
    sp_df = load_data(spark, sys.argv[1])
    re_df = sp_df.repartition(2)
    count_df = count_by_country(re_df)
    logger.info(count_df.collect())
    # print("Spark UI:", spark.sparkContext.uiWebUrl)
    input("Please inter")
    logger.info("finish new spark")
