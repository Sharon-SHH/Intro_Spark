from pathlib import Path
from helloSpark_lib.utils import load_data, count_by_country

#PROJECT_ROOT = Path(__file__).resolve().parents[1]
#print(PROJECT_ROOT)
#DATA = PROJECT_ROOT / "data" / "Fire.csv"


def test_load(spark):
    df = load_data(spark, "data/Fire.csv")
    assert df.count() == 44


def test_country(spark):
    df = load_data(spark, "data/Fire.csv")
    count_list = count_by_country(df).collect()
    cn_dict = {}
    for stationArea, num in count_list:
        cn_dict[stationArea] = num
    assert cn_dict[14] == 1
    assert cn_dict[23] == 5