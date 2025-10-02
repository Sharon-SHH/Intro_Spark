import configparser
from pyspark import SparkConf
from pyspark.sql.functions import col

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf

def load_data(spark, dataFile):
    return spark.read\
        .option("header", "true")\
            .option("inferSchema", "true")\
                .csv(dataFile)

def count_by_country(df):
    return (df.filter(col("Priority") == "3")          # 比较字符串而不是数字
              .select("Call Number", "Priority", "Station Area", "Call Date")
              .groupBy("Station Area")
              .count())
