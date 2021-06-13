import logging as lg
import os
import csv
import mysql.connector as connection
import cassandra
from cassandra.cluster import Cluster
from  flask import Flask, render_template, request, jsonify
from cassandra_config import cassandra_connect
from cassandra_config import Cassandra_table_ins
from cassandra.query import SimpleStatement, BatchStatement
import cassandra

lg.basicConfig(filename = 'test4.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/cassandra_download_file', methods=['POST'])  # for calling the API from Postman/SOAPUI
def cassandra_ins_operation_via_post():
    if (request.method == 'POST'):
       KEYSPACENAME = request.json['KEYSPACENAME']
       TABLENAME = request.json['TABLENAME']
       FETCH    = request.json['FETCH']

       cloud_con = cassandra_connect()
       cloud_config = cloud_con.cloud_config
       auth_provider = cloud_con.auth_provider
       cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
       session = cluster.connect()
       connt = Cassandra_table_ins()
       field1 = connt.cassa_config_ins()
       message = []


       query = str(( FETCH +' '+KEYSPACENAME+'.'+TABLENAME+';'))


       try:
          rows=session.execute(query)
          message.append('Fetched table Successfully')
          try:
             with open('cass_down_dump.txt', 'w') as out_file:
               for row in rows:
                   out_file.write(str(row))
               message.append('Dowloaded file Successfully')
          except Exception as e:
                message.append('Error in output file errror')
                lg.error('Error has occurred')
                lg.exception(str(e))

       except Exception as e:
           message.append('Error in fetching cassandra table')
           lg.error('Error has occurred')
           lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)