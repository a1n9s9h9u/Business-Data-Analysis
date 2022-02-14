from pyspark.sql import SparkSession

import logging

logging.\
    basicConfig(filename="logFile", filemode='w', format='%(asctime)s %(levelname)s-%(message)s')

logging.warning("Starting the utilities class")


class Base:

    def __init__(self):
        self.spark = SparkSession.builder. \
            appName("POC ABN AMRO").getOrCreate()
        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    """
    readCsv() method is to read the csv file. It takes two 
    parameter, one is the csv file name and another is the 
    delimiter that has been used to separate the texts of 
    the csv file.
    """
    def readCsv(self, file, deli):
        logging.warning("Trying to read csv file")
        try:
            logging.warning("Complete reading csv file")
            return self.spark.read. \
                options(header=True, inferSchema=True, delimiter=deli).csv(file)

        except Exception as e:
            print(e)
            logging.error("File not found")

    """
    writeCsv() method is to save a dataframe to a csv file. It 
    takes two parameter, one is the dataframe and another is the 
    file path where we want to save the csv file. It also 
    overwrites the file if it already exists.
    """
    def writeCsv(self, df, path):
        logging.warning("Trying to save dataframe to csv file")
        try:
            logging.warning("Saving dataframe to csv file")
            df.write.mode("overwrite").\
                options(header=True, delimiter=',').csv(path)

        except Exception as e:
            print(e)
            logging.error("Error saving dataframe to csv")

