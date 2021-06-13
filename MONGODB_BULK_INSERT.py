import pymongo
import logging as lg
import os
import csv
from MONGODB_CONFIG import mongodb_conn_class

from  flask import Flask, render_template, request, jsonify
import MongoDB

lg.basicConfig(filename = 'testMongodb.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/mongodb_Bulk_ins_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_Bulk_ins_operation_via_post():
    if (request.method == 'POST'):
       DBNAME  = request.json["DBNAME"]
       COLLECTION_NAME = request.json["COLLECTION_NAME"]
       BULK_INSERT  = request.json["BULK_INSERT"]



    obj = mongodb_conn_class()
    obj.mongodb_conn()

    message = []


    try:
        client_cloud = pymongo.MongoClient(obj.USER)
        db = client_cloud.test
        message.append('MONGODB connected Successfully')
        db1 = client_cloud[DBNAME]

        #collection1 = db1[COLLECTION_NAME]
        collection1 = COLLECTION_NAME

      #  obj.checkExistence_DB(collection1=collection1, DB_NAME=DBNAME, db=db1, msg='x')
        obj.checkExistence_DB(DBNAME,client_cloud,'x')
        message.append(str(obj.msg))

        obj.checkExistence_COL(collection1,DBNAME,db1,'x')
        message.append(str(obj.msg))



        db1 = client_cloud[DBNAME]
        collection = db1[COLLECTION_NAME]
        try:
            with open("Mondodb_Load_file.csv") as f1:
                   records = csv.reader(f1, delimiter=",")
                   li = []
                   for record in records:
                       dict = {}
                       Chiral_indice_n = record[0]
                       Chiral_indice_m = record[1]
                       Initial_atomic_coordinate_u = record[2]
                       dict = {"Chiral_indice_n": Chiral_indice_n, "Chiral_indice_m": Chiral_indice_m,
                               "Initial_atomic_coordinate_u": Initial_atomic_coordinate_u}
                       li.append(dict)
                   try:
                      collection.insert_many(li)
                      message.append('Bulk insert Successfully')
                   except Exception as e:
                      message.append('Error in insert')
                      lg.error('Error has occurred')
                      lg.exception(str(e))
        except Exception as e:
               message.append('Error in file error')
               lg.error('Error has occurred')
               lg.exception(str(e))






    except Exception as e:
        message.append('Error in connection MONGODB')
        lg.error('Error has occurred')
        lg.exception(str(e))
    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)