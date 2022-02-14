from base import *
from pyspark.sql.functions import to_date, coalesce, date_format
import pyspark.sql.functions as f
from datetime import datetime

base_obj = Base()

df = base_obj.readCsv("./utilities/sales/salesData.csv", ';')
# df.show()


def to_date_(col, formats=('dd-MM-yyyy', 'MM/dd/yyyy')):
    return coalesce(*[to_date(col, f) for f in formats])


df_new = df.withColumn('Order Date', to_date_('Order Date'))
# df_new.show()
# df_new.printSchema()

df_new = df_new.\
    withColumn('Order Date', date_format('Order Date', 'dd-MM-yyyy'))

# df_new.show()
# df_new.printSchema()

df_new_date = df_new.\
    select(f.col('Order Date'), to_date(f.col('Order Date'), 'dd-MM-yyyy').
           alias('OrderDate'), 'Order ID')

df_new_date = df_new_date.select('Order ID', 'OrderDate')

# df_new_date.show()
# df_new_date.printSchema()

df_new_final = df_new.\
    join(df_new_date, df_new['Order ID'] == df_new_date['Order ID'], 'inner').\
    drop(df_new['Order ID'])

# df_new_final.show()

df_new_fin = df_new_final.\
    select('Region','Country', 'Item Type', 'Sales Channel', 'Order Priority',
           'OrderDate', 'Order ID', 'Units Sold', 'Unit Price',
           'Total Revenue', 'Total Profit')

# df_new_fin.show()
# df_new_fin.printSchema()

