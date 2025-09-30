import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, Row


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder \
            .master("local[3]")\
            .appName("PyTest Spark")\
            .getOrCreate())
    yield spark


@pytest.fixture(scope="session")
def sample_create_df(spark):
    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])
    rdd = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(rdd, 2)
    return spark.createDataFrame(my_rdd, my_schema)