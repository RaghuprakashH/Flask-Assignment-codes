import pymongo
import logging as lg
import os
import csv
import sys
from MONGODB_CONFIG import mongodb_conn_class

from  flask import Flask, render_template, request, jsonify
import MongoDB

lg.basicConfig(filename = 'testMongodb.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/mongodb_Download_File_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_Update_operation_via_post():
    if (request.method == 'POST'):
       DBNAME  = request.json["DBNAME"]
       COLLECTION_NAME = request.json["COLLECTION_NAME"]


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


        try:
           db1 = client_cloud[DBNAME]
           collection = db1[COLLECTION_NAME]
           all_record = collection.find()
           message.append('Fetched rows Successfully')
           try:
               file = open('mongodbdump.json', 'a')

               for idx in all_record:
                   file.write(str(idx))
           except Exception as e:
               message.append('Error in output file')
               lg.error('Error has occurred')
               lg.exception(str(e))


        except Exception as e:
           message.append('Error in Fetch')
           lg.error('Error has occurred')
           lg.exception(str(e))

    except Exception as e:
        message.append('Error in connection MONGODB')
        lg.error('Error has occurred')
        lg.exception(str(e))
    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)