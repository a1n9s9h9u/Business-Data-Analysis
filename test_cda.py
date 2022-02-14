import unittest
from cda import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("POC ABN AMRO TEST CDA").getOrCreate()

emp = [
    (1, "A"),
    (2, "B"),
    (1, "A"),
    (3, "D"),
    (2, "B")
]

emp_col = ["customerId", "name"]

df1 = spark.createDataFrame(data=emp, schema=emp_col)

cus = [
    (1, 2),
    (3, 1),
    (2, 2)
]

cus_col = ["customerId", "noOfAccounts"]

df2 = spark.createDataFrame(data=cus, schema=cus_col)

cus_two = [
    (1, 2),
    (2, 2)
]

cus_two_col = ["customerId", "noOfAccounts"]

df3 = spark.createDataFrame(data=cus_two, schema=cus_two_col)

cus_balance = [
    (1, 200),
    (3, 100),
    (2, 200),
    (3, 100),
]

cus_balance_col = ["customerId", "balance"]

df4 = spark.createDataFrame(data=cus_balance, schema=cus_balance_col)

cus_balance_test = [
    (1, 200),
    (3, 200),
    (2, 200),
]

cus_balance_col_test = ["customerId", "accountBalance"]

df5 = spark.createDataFrame(data=cus_balance_test, schema=cus_balance_col_test)

cda_test_obj = CDA()


class TestCDA(unittest.TestCase):

    def test_getNoOfAcc(self):
        df = cda_test_obj.getNoOfAcc(df1)
        if df.schema != df2.schema:
            return False
        if df.collect() != df2.collect():
            return False
        return True

    def test_getMoreThanTwoAcc(self):
        df = cda_test_obj.getMoreThanTwoAcc(df2)
        if df.schema != df3.schema:
            return False
        if df.collect() != df3.collect():
            return False
        return True

    def test_getAccBln(self):
        df = cda_test_obj.getAccBln(df4)
        if df.schema != df5.schema:
            return False
        if df.collect() != df5.collect():
            return False
        return True

