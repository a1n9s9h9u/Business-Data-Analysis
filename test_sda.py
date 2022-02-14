import unittest
from sda import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("POC ABN AMRO TEST SDA").getOrCreate()

ans = [
    ("Asia", 1000),
    ("Africa", 1000),
    ("Asia", 1000),
    ("Africa", 1000),
    ("America", 1000)
]

ans_col = ["Region", "TotalRevenue"]

df1 = spark.createDataFrame(data=ans, schema=ans_col)

ans_test = [
    ("Africa", 2000),
    ("Asia", 2000),
    ("America", 1000)
]

ans_col_test = ["Region", "sumTotalRevenue"]

df2 = spark.createDataFrame(data=ans_test, schema=ans_col_test)

unit = [
    ("India", 200),
    ("USA", 600),
    ("Italy", 400),
    ("India", 200),
    ("USA", 600),
    ("Italy", 400)
]

unit_col = ["Country", "UnitsSold"]

df3 = spark.createDataFrame(data=unit, schema=unit_col)

unit_test = [
    ("USA", 1200),
    ("Italy", 800),
    ("India", 400)
]

unit_test_col = ["Country", "UnitsSold"]

df4 = spark.createDataFrame(data=unit_test, schema=unit_test_col)

sum_profit = [
    ("Asia", 1000, "1/1/2011"),
    ("Africa", 1000, "1/1/2012"),
    ("USA", 1000, "1/5/2014"),
    ("Asia", 1000, "10/1/2015"),
    ("Africa", 1000, "1/8/2016"),
    ("USA", 1000, "1/2/2018")
]

sum_profit_col = ["Region", "TotalProfit", "OrderDate"]

df5 = spark.createDataFrame(data=sum_profit, schema=sum_profit_col)

sum_profit_test = [
    ("Asia", 2000)
]

sum_profit_test_col = ["Region", "sumTotalProfit"]

df6 = spark.createDataFrame(data=sum_profit_test, schema=sum_profit_test_col)

sda_test_obj = SDA()


class TestSDA(unittest.TestCase):

    def test_getNoOfAcc(self):
        df = sda_test_obj.getSumTotalRevenue(df1)
        if df.schema != df2.schema:
            return False
        if df.collect() != df2.collect():
            return False
        return True

    def test_getTotalUnitsSold(self):
        df = sda_test_obj.getTotalUnitsSold(df3)
        if df.schema != df4.schema:
            return False
        if df.collect() != df4.collect():
            return False
        return True

    def test_getSumTotalProfit(self):
        df = sda_test_obj.getSumTotalProfit(df5)
        if df.schema != df6.schema:
            return False
        if df.collect() != df6.collect():
            return False
        return True

