
import os,pandas as pd
from traceback import print_tb
from petl import *
from json import dumps,loads
import datetime,logging

currenttime = datetime.datetime.now().strftime("%H-%M-%S")



class ETL:

    def __init__(self,paramArgs):
        self.args = paramArgs
        self.createLogs()
        self.result()
        #self.jsonFilesToDataframe()


    def createLogs(self):
        # Creating log file to implement logs fom the application 
        path = "logs"
        logFileName = "".join([path,'/loggingInfo-',currenttime,".log"])

        self.createDirectory(path)

        logging.basicConfig(filename=logFileName, filemode='w', format='%(name)s - %(levelname)s - %(message)s') 
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)  
        
    

    def createDirectory(self,path):
        try:

            # If log folder doesnot exist then create
            if not os.path.exists(path):
                os.makedirs(path)

        except os.error as error:
            print("File path is invalid or in accessible",error)



    def jsonFilesToDataframe(self):
        """ Reading the given json files and adding them into single dataframe"""
        try:

            transactionPath = self.args['transactions_location']
            transactionsData = pd.DataFrame(columns=['customer_id','basket','date_of_purchase'])
            
            for root,subdir,files in os.walk(transactionPath):
                for file in files:
                    filePath = os.path.join(root,file)
                   
                    with open(filePath,"r") as fileObj:
                        data = dumps(fileObj.readlines())
                        self.logger.info("%s file is being processed "%filePath)
                        data1 = loads(data)

                        for value in data1:
                            dictValue = loads(value)
                            transactionsData.loc[-1] = dictValue
                            transactionsData.index = transactionsData.index + 1
                            transactionsData = transactionsData.sort_index()
            
            transactionsData.to_csv("./input_data/starter/transactions.csv")
            self.logger.info("transaction file is in this location : ./input_data/starter/transactions.csv")

            return transactionsData
        except Exception as e:print(e)




    def result(self):
        """ Comparing transactions data with other datasets and fetching the required values from there"""
        try:
            transactionsData = self.jsonFilesToDataframe()            
            productsData = pd.read_csv(".\input_data\starter\products.csv",usecols=["product_id","product_category"])
            customersData = pd.read_csv(self.args['customers_location'])
            
            resuts = []
            self.logger.info(" In process of creating the ouput dataframe")

            for i, row in transactionsData.iterrows():
                temp = dict()
                temp['customer_id'] = row['customer_id']

                #extracting loyalty score for a specific customer id
                temp['loyalty_score'] = customersData[customersData["customer_id"] ==row['customer_id']].values[0][1]
                temp['product_id'] = []
                
                for prod in row['basket']:
                    temp['product_id'].append(prod['product_id'])

                #extracting product_category for a specific product_id
                temp['product_category'] = productsData[productsData["product_id"] ==prod['product_id']].values[0][1]
                temp['purchase_count'] = len(row['basket'])
                self.logger.info("data for %s is being updated"%temp['customer_id'])
                resuts.append(temp)
            
            newtransactionsData = pd.DataFrame(resuts)
            df=newtransactionsData.groupby("customer_id")
            df1=df.sum()
            df2=df.first()

            df4 = pd.concat([df1,df2],1).reset_index()
            finalData = df4.loc[:,~df4.columns.duplicated()]
            #print(finalData)
           
            finalData.to_csv("output.csv",index=False)

            # Giving some proper look for output data
            data = fromcsv("output.csv")
            tojson(data,"output.json")
            self.logger.info("please find the output data in output.json file")

        except Exception as e: print(e)
       
