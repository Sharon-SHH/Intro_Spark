import os
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from helloSpark_lib.logger import Log4J
from helloSpark_lib.utils import get_spark_app_config, load_data, count_by_country


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))


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

    # --------------- Can observe the result -------------
    # Create the schema and row data
    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])
    rdd = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(rdd, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.show(5)
    logger.info("new_df: " + str(new_df.schema))
