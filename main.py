#Driver code to aggregate large data sets in csv form

import pymongo # API for MongoDb
import pandas
import sys
from datetime import datetime


from pymongo import MongoClient, errors 
from Constants import Database_
from Constants import Collection_
from Constants import Pipeline_
from Constants import Uri_
from Constants import Numberofdocs_
from Constants import Namefile_

print('connecting...')

try:
    client = pymongo.MongoClient(Uri_)
    client.server_info()
except errors.ServerSelectionTimeoutError as err:
    print(err)
    print('connection Failed')
    print('quitting')
    quit()


print('connected')

#Enter database and collection name here
db = client[Database_]
collection = db[Collection_]


#Define the number of required documents here per .csv file
numberofresutls = 1000 

print('Database:', Database_, ', Collection:', Collection_)


cursor = collection.aggregate(Pipeline_, allowDiskUse = True) 
#Max aggregation step size is 100MB, allowDiskUse allows to use HDD for larger data operations on the server side.





mongo_docs = list(cursor)
docs = pandas.DataFrame(columns=[])

print('Aggregating...')
for num, doc in enumerate(mongo_docs):
    print(num)
    doc["_id"] = str(doc["_id"])
    doc_id = doc["_id"] 
    series_obj = pandas.Series( doc, name=doc_id )
    docs = docs.append(series_obj) 
    if len(docs) > Numberofdocs_ :
        name = Namefile_ + str(num+1) +'.csv'
        print('writing :', name)
        docs.to_csv(name, sep=',')
        docs = docs[0:0]
        continue
if len(docs) > 0 :
        name = Namefile_ + str(num+1) +'.csv'
        print('writing :', name)
        docs.to_csv(name, sep=',')
        docs = docs[0:0]



    









