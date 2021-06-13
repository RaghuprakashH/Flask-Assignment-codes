import pymongo
import logging as lg
import os
import csv
from MONGODB_CONFIG import mongodb_conn_class

from  flask import Flask, render_template, request, jsonify
import MongoDB

lg.basicConfig(filename = 'testMongodb.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/mongodb_insert_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_Update_operation_via_post():
    if (request.method == 'POST'):
       DBNAME  = request.json["DBNAME"]
       COLLECTION_NAME = request.json["COLLECTION_NAME"]
       UPDATE_FIELD1  = request.json["UPDATE_FIELD1"]
       UPDATE_FIELD2 = request.json["UPDATE_FIELD2"]
       UPDATE_FIELD3 = request.json["UPDATE_FIELD3"]
       newvalue1 = request.json["newvalue1"]
       newvalue2 = request.json["newvalue2"]
       newvalue3 = request.json["newvalue3"]

    obj = mongodb_conn_class()
    obj.mongodb_conn()

    message = []



    dict2 = {}

    dict2 = {"Chiral_indice_n": newvalue1,"Chiral_indice_m": newvalue2,"Initial_atomic_coordinate_u": newvalue3}



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


        try:
           db1 = client_cloud[DBNAME]
           collection = db1[COLLECTION_NAME]
           present_data = dict2

           collection.insert_one(present_data)
           message.append('Inserted Successfully')

        except Exception as e:
           message.append('Error in Updation')
           lg.error('Error has occurred')
           lg.exception(str(e))

    except Exception as e:
        message.append('Error in connection MONGODB')
        lg.error('Error has occurred')
        lg.exception(str(e))
    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)