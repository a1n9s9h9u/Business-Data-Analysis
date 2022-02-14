from base import *

logging.warning("Starting the CDA class")


class CDA(Base):

    def __init__(self):
        super().__init__()

    logging.warning("Joining two dataframes according to the customerId column")

    """
    joinDf() method is to perform the inner join between two dataframes. 
    It takes two dataframes as two parameters and return the joined dataframe.
    """
    def joinDf(self, df1, df2):
        return df1.join(df2, df1.customerId == df2.customerId, "inner")

    logging.warning("Getting the total no of accounts each customer has")

    """
    getNoOfAcc() method is to calculate the total no of accounts each customer has. 
    It takes one dataframe as parameter and return a dataframe with total 
    no of accounts for each customers.
    """
    def getNoOfAcc(self, df):
        return df.groupBy("customerId").count() \
            .withColumnRenamed("count", "noOfAccounts")

    logging.warning("Listing those customers who has 2 or more than 2 accounts")

    """
    getMoreThanTwoAcc() method is to filter out those customers who has 
    2 or more than 2 accounts. It takes a dataframe as parameter and return a 
    dataframe with customers having 2 or more than  accounts.
    """
    def getMoreThanTwoAcc(self, df):
        return df.filter(df.noOfAccounts >= 2)

    logging.warning("Getting the total account balance for each customers")

    """
    getAccBln() method is to calculate total account balance for 
    each customer. It takes a dataframe as parameter and return a 
    dataframe with total account balance for each customer.
    """
    def getAccBln(self, df):
        return df.groupBy("customerId").sum("balance"). \
            withColumnRenamed("sum(balance)", "accountBalance")

    logging.warning("Listing the top 5 customers with highest account balance")

    """
    getTopFiveAcc() method is to filter out top 5 customers with the 
    highest account balance. It takes a single dataframe as a parameter and 
    return a dataframe with the list of top  customers with the 
    highest account balance.
    """
    def getTopFiveAcc(self, df):
        return df. \
            select("customerId", "noOfAccounts", "accountBalance", "forename", "surname"). \
            sort(final_Acc_Bln_df.accountBalance.desc()).limit(5)


logging.warning("Creating dataframe for account_data.csv")
cda_obj = CDA()
acc_df = cda_obj.readCsv("./utilities/Account/account_data.csv", ',')
# acc_df.show()
# acc_df.printSchema()

logging.warning("Creating dataframe for customer_data.csv")
cus_df = cda_obj.readCsv("./utilities/Account/customer_data.csv", ',')
# cus_df.show()
# cus_df.printSchema()

logging.warning("Joining Account and Customer dataframe")
joined_df = cda_obj.joinDf(acc_df, cus_df).drop(cus_df.customerId)

# joined_df.show()
# joined_df.printSchema()

No_Of_Accs_df = cda_obj.getNoOfAcc(joined_df)

# No_Of_Accs_df.show()

final_df = cda_obj.joinDf(joined_df, No_Of_Accs_df). \
    drop(joined_df.accountId).drop(joined_df.customerId)

final_df = final_df.select("customerId", "noOfAccounts", "balance", "forename", "surname")
# final_df.show()

final_df_NOA_2 = cda_obj.getMoreThanTwoAcc(final_df). \
    select("customerId", "forename", "surname", "noOfAccounts")

# final_df_NOA_2.show()

# final_df.show()
# final_df.printSchema()

final_Acc_Bln_df = cda_obj.getAccBln(final_df)

# final_Acc_Bln_df.show()
# final_Acc_Bln_df.printSchema()

final_Acc_Bln_df = cda_obj.joinDf(final_df, final_Acc_Bln_df). \
    drop(final_df.customerId).dropDuplicates(["customerId"])

final_Acc_Bln_df = cda_obj.getTopFiveAcc(final_Acc_Bln_df)

# final_Acc_Bln_df.show()
# final_Acc_Bln_df.printSchema()

