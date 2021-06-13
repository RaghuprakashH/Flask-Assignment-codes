import pymongo
import logging as lg
import os
import csv
from MONGODB_CONFIG import mongodb_conn_class

from  flask import Flask, render_template, request, jsonify
import MongoDB

lg.basicConfig(filename = 'testMongodb.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/mongodb_Update_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_Update_operation_via_post():
    if (request.method == 'POST'):
       DBNAME  = request.json["DBNAME"]
       COLLECTION_NAME = request.json["COLLECTION_NAME"]
       UPDATE_FIELDS  = request.json["UPDATE_FIELDS"]
       OLD_VALUES = request.json["OLD_VALUES"]
       NEW_VALUES = request.json["NEW_VALUES"]

    obj = mongodb_conn_class()
    obj.mongodb_conn()

    message = []


    for oldvalue in OLD_VALUES:
        dict1 = {}

        if UPDATE_FIELDS == "Chiral_indice_n":
            dict1 = {"Chiral_indice_n": oldvalue}
        if UPDATE_FIELDS == "Chiral_indice_m":
            dict1 = {"Chiral_indice_m": oldvalue}
        if UPDATE_FIELDS == "Initial_atomic_coordinate_u":
                dict1 = {"Chiral_indice_m": oldvalue}
    for newvalue in NEW_VALUES:
        dict2 = {}

        if UPDATE_FIELDS ==  "Chiral_indice_n":
            dict2 = {"Chiral_indice_n": newvalue}
        if UPDATE_FIELDS ==  "Chiral_indice_m":
            dict2 = {"Chiral_indice_m": newvalue}
        if UPDATE_FIELDS ==  "Initial_atomic_coordinate_u" :
            dict2 = {"Chiral_indice_m": newvalue}


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
           present_data = dict1
           new_data = {"$set": dict2}
           collection.update_one(present_data, new_data)
           message.append('Updated Successfully')

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