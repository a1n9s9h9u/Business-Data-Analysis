from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("UDF APP").getOrCreate()

data = [
    ("StuA", 8),
    ("StuB", 12),
    ("StuC", 21),
    ("StuD", 28),
    ("StuE", 35),
    ("StuF", 40),
    ("StuG", 45),
    ("StuH", 56),
    ("StuI", 62),
    ("StuJ", 68),
    ("StuK", 71),
    ("StuL", 78),
    ("StuM", 81),
    ("StuN", 91),
    ("StuO", 988)
]

schema = ["Name", "Marks"]

df = spark.createDataFrame(data=data, schema=schema)

df.show()
df.printSchema()

marksSlabA = 10
marksSlabB = 25
marksSlabC = 50
marksSlabD = 75
marksSlabE = 100

rankSlabA = '1'
rankSlabB = '2'
rankSlabC = '3'
rankSlabD = '5'
rankSlabE = '8'


def getRank(marks):

    if marks <= marksSlabA:
        rank = rankSlabA
    elif marks <= marksSlabB:
        rank = rankSlabB
    elif marks <= marksSlabC:
        rank = rankSlabC
    elif marks <= marksSlabD:
        rank = rankSlabD
    elif marks <= marksSlabE:
        rank = rankSlabE
    else:
        rank = "Invalid marks"

    return rank


udfFunc = udf(lambda z: getRank(z))

df = df.withColumn("Rank", udfFunc(df.Marks))

df.show()
df.printSchema()
