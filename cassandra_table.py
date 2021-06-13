import logging as lg
import os
import csv
import mysql.connector as connection
import cassandra
from cassandra.cluster import Cluster
from  flask import Flask, render_template, request, jsonify
from cassandra_config import cassandra_connect
from cassandra_config import Cassandra_table_cre

import cassandra

lg.basicConfig(filename = 'test4.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/cassandra_creation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def cassandra_tab_operation_via_post():
    if (request.method == 'POST'):
       KEYSPACENAME = request.json['KEYSPACENAME']
       TABLENAME = request.json['TABLENAME']

       cloud_con = cassandra_connect()
       cloud_config = cloud_con.cloud_config
       auth_provider = cloud_con.auth_provider
       cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
       session = cluster.connect()
       connt = Cassandra_table_cre()
       fields = connt.cassa_config_read()
       message = []

       query = str((fields.create_QUERY+' '+KEYSPACENAME+'.'+TABLENAME+fields.columns)+';')
       try:
           row= session.execute(query).one()
           message.append('Table created Successfully')

           print(row)
       except Exception as e:
           message.append('Error in creating cassandra table')
           lg.error('Error has occurred')
           lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)