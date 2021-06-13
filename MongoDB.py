import pymongo
import logging as lg
import os
import csv
from MONGODB_CONFIG import mongodb_conn_class

from  flask import Flask, render_template, request, jsonify

lg.basicConfig(filename = 'testMongodb.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/mongodb_table_creation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_table_cre_operation_via_post():
    if (request.method == 'POST'):
       TABLECREATION = request.json['TABLECREATION']
       DBNAME  = request.json['DBNAME']
       COLLECTION_NAME = request.json['COLLECTION_NAME']

    obj = mongodb_conn_class()
    obj.mongodb_conn()
    message = []
    obj.USER
    flagdb = 'N'
    flagcl = 'N'



    try:
        client_cloud = pymongo.MongoClient(obj.USER)
        db = client_cloud.test
        message.append('MONGODB connected Successfully')
        db1 = client_cloud[DBNAME]
        obj.checkExistence_DB(DBNAME, client_cloud, 'x')
        message.append(str(obj.msg))
        collection1 = COLLECTION_NAME
        obj.checkExistence_COL(collection1, DBNAME, db1, 'x')
        message.append(str(obj.msg))
        try:
              dataBase = client_cloud[DBNAME]
              try:
                  collection = dataBase[COLLECTION_NAME]


              except Exception as e:
                  message.append('Error in creating Collection')
                  lg.error('Error has occurred')
                  lg.exception(str(e))
        except Exception as e:
              message.append('Error in creating Database')
              lg.error('Error has occurred')
              lg.exception(str(e))


    except Exception as e:
        message.append('Error in connection MONGODB')
        lg.error('Error has occurred')
        lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)