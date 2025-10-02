import os, re
from xml.dom.minicompat import StringTypes
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helloSpark_lib.logger import Log4J
from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country

def standard_gender(gender):
    # Female, Male, Unknow
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == "__main__":
    """Standard Gender in survey.csv"""
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
    df = spark.read.format("csv").option("header","true")\
        .option("inferSchema","true").load("data/survey.csv")
    df.printSchema()
    df.show(5)
    
    # convert the function to a UDF (User Defined Function), = registered as a UDF, so that Spark knows how to apply it across all rows in the DataFrame.
    standard_gender_udf = udf(standard_gender, StringType())
    # check the standard_gender function is in catalog
    [logger.info(f) for f in spark.catalog.listFunctions() if "standard_gender" in f.name]
    survey_df = df.withColumn("gender", standard_gender_udf("gender"))
    survey_df.show(10)

    # register SQL expression: it also creates one entry for the catalog
    spark.udf.register("standardgender_udf", standard_gender, StringType())
    [logger.info(f.name) for f in spark.catalog.listFunctions() if "standardgender_udf" in f.name.lower()]
    survey_df1 = df.withColumn("gender", expr("standardgender_udf(gender)")) # make the object to be string,
    survey_df1.show(5)