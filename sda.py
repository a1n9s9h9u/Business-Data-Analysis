from base import *
from pyspark.sql.functions import *

logging.warning("Starting the SDA class")


class SDA(Base):

    def __init__(self):
        super().__init__()

    logging.warning("Changing the column names")

    """
    changeColNames() method removes all the whitespaces from the 
    column names. It takes a dataframe as a parameter and returns a 
    dataframe with the updated column names.
    """
    def changeColNames(self, df):
        newCols = (column.replace(' ', '') for column in sal_df.columns)
        return df.toDF(*newCols)

    logging.warning("Changing the formats of the date rows")

    """
    formatDateRows() method substitute '-' by '/' in the date rows. 
    It takes a dataframe as a parameter and returns a 
    dataframe with the updated date rows.
    """
    def formatDateRows(self, df):
        return df. \
            withColumn("OrderDate", when(sal_df.OrderDate.contains('-'),
                                         regexp_replace(sal_df.OrderDate, '-', '/'))
                       .otherwise(sal_df.OrderDate))

    logging.warning("Calculating the sum total revenue")

    """
    getSumTotalRevenue() method calculates the sum of total revenue 
    grouped by Region. 
    It takes a dataframe as a parameter and returns a 
    dataframe with the sumTotalRevenue column.
    """
    def getSumTotalRevenue(self, df):
        return df.groupBy("Region").sum("TotalRevenue"). \
            withColumnRenamed("sum(TotalRevenue)", "sumTotalRevenue")

    logging.warning("Getting the top 5 countries with the highest no of units sold")

    """
    getTotalUnitsSold() method calculates the sum of total units 
    sold grouped by Country. 
    It takes a dataframe as a parameter and returns a 
    dataframe with the totalUnitsSold column. 
    Finally it creates a new dataframe with the top 5 
    countries with the highest totalUnitsSold.
    """
    def getTotalUnitsSold(self, df):
        df = df.groupBy("Country").sum("UnitsSold"). \
            withColumnRenamed("sum(UnitsSold)", "totalUnitsSold")
        df = df.sort(df.totalUnitsSold.desc()).limit(5)
        return df

    logging.warning("Calculating the sum total profit of Asia region from 2011 to 2015")

    """
    getSumTotalProfit() method calculates the sum of total profits 
    grouped by Region. 
    It takes a dataframe as a parameter and returns a 
    dataframe with the sumTotalProfit column. 
    It filters out the rows for the timeframe of 2011 to 2015, 
    then calculates the sumTotalProfit grouped by Region, finally 
    filters out the sumTotalProfit for only Asia Region.
    """
    def getSumTotalProfit(self, df):
        df = df.filter(df.OrderDate.between('1/1/2011', '12/31/2015'))
        df = df.groupBy("Region").sum("TotalProfit"). \
            withColumnRenamed("sum(TotalProfit)", "sumTotalProfit")
        df = df.filter(df.Region == 'Asia')
        return df


sda_obj = SDA()
sal_df = sda_obj.readCsv("./utilities/sales/salesData.csv", ';')
# sal_df.show(truncate=False)
# sal_df.printSchema()


sal_df = sda_obj.changeColNames(sal_df)
# sal_df.show(truncate=False)
# sal_df.printSchema()

sal_df = sal_df.withColumn("OrderID", sal_df.OrderID.cast('long')). \
    withColumn("UnitsSold", sal_df.UnitsSold.cast('double'))

sal_df = sda_obj.formatDateRows(sal_df)
# sal_df.show(truncate=False)
# sal_df.printSchema()

# sal_df = sal_df.withColumn("OrderDate", to_date(sal_df.OrderDate, 'dd-MM-yyyy'))
# sal_df.show(truncate=False)
# sal_df.printSchema()

sal_region_df = sda_obj.getSumTotalRevenue(sal_df)
# sal_region_df.show(truncate=False)
# sal_region_df.printSchema()

sal_units_sold_df = sda_obj.getTotalUnitsSold(sal_df)
# sal_units_sold_df.show(truncate=False)
# sal_units_sold_df.printSchema()

# sal_df.show(truncate=False)
# sal_df.printSchema()

"""
def changeDateFormat(col, formats=("MM/dd/yyyy", "dd-MM-yyyy")):
    return coalesce(*[to_date(col, f) for f in formats])

sal_df.withColumn("OrderDateNew", changeDateFormat("OrderDate")).show()

sal_df_date = sal_df.withColumn('OrderDate', date_format('OrderDate', 'dd-MM-yyyy'))
sal_df_date.show()
"""

sal_Asia_df = sda_obj.getSumTotalProfit(sal_df)
sal_Asia_df.show()

sda_obj.writeCsv(sal_Asia_df, 'profit_asia.csv')

