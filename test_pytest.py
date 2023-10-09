import os

import pytest
from chispa import assert_df_equality
from datetime import datetime, date

from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row

from lib import DataLoader, Transformation, ConfigLoader
from lib.ConfigLoader import get_config
from lib.DataLoader import get_datasetOne_schema
from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.2.1"

def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["country.filter"] == "United Kingdom,Netherlands"

@pytest.fixture(scope='session')
def test_expected_final_df(spark):
    schema = "client_identifier int, mail_id string, bitcoin_address string, credit_card_type string"
    p = ConfigLoader.get_path("LOCAL", "finaldataset.path")
    return spark.read.format("csv").option("header","true").schema(schema).option("header", "true").load(p)



def test_finalDf(spark, test_expected_final_df):
    d1 = DataLoader.read_datasetOne(spark, "LOCAL")
    d2 = DataLoader.read_datasettwo(spark, "LOCAL")
    df12 = Transformation.join_datasetone_two(d1, d2)
    final_df = Transformation.get_finaldf(df12)
    assert_df_equality(final_df, test_expected_final_df, ignore_schema=True)






