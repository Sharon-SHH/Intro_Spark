from datetime import date

from pyspark.sql.types import DateType
from helloSpark import to_date_df


def test_type(sample_create_df):
    df = sample_create_df
    new_df = to_date_df(df, "M/d/y", "EventDate")

    assert isinstance(new_df.schema["EventDate"].dataType, DateType)

def test_data_val(sample_create_df):
    df = sample_create_df
    new_df = to_date_df(df, "M/d/y", "EventDate").collect()
    # collect 
    vals = [row["EventDate"] for row in new_df]
    expects = [date(2020, 4, 5), date(2020, 4, 5), date(2020, 4, 5), date(2020, 4, 5)]
    assert vals == expects
    # for row in new_df:
    #     assert row["EventDate"] == date(2020, 4, 5)
