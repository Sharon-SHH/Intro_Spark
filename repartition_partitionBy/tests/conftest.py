import pytest
from pyspark.sql import SparkSession

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]   # .../HelloSpark
sys.path.insert(0, str(ROOT)) 

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder \
            .master("local[3]")\
            .appName("PyTest Spark")\
            .getOrCreate())
            # .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            # .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:{log4j2_path}")
            # .getOrCreate())
    yield spark
    # spark.stop()