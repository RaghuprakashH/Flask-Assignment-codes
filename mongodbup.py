import csv
import sys
import json
import pandas
import pandas as pd
import pymongo


client_cloud = pymongo.MongoClient("mongodb+srv://test:test1@cluster0.mo8cp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client_cloud.test
db1 = client_cloud["OG701"]
collection = db1["BP752"]


present_data = {'Chiral_indice_n': '2'}
new_data = {"$set":{'Chiral_indice_n': '20'}}
collection.update_one(present_data, new_data)

