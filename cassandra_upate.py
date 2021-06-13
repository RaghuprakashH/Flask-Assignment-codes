import logging as lg
import os
import csv
import mysql.connector as connection
import cassandra
from cassandra.cluster import Cluster
from  flask import Flask, render_template, request, jsonify
from cassandra_config import cassandra_connect
from cassandra_config import Cassandra_table_ins
from cassandra_config import Cassandra_table_update
from cassandra.query import SimpleStatement, BatchStatement
import cassandra

lg.basicConfig(filename = 'test4.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/cassandra_update', methods=['POST'])  # for calling the API from Postman/SOAPUI
def cassandra_update_operation_via_post():
    if (request.method == 'POST'):
       KEYSPACENAME = request.json['KEYSPACENAME']
       TABLENAME = request.json['TABLENAME']
       Initial_atomic_coordinate_u = request.json['Initial_atomic_coordinate_u']
       value2 = request.json['value2']
       Chiral_indice_n = request.json['Chiral_indice_n']
       condvalue1 = request.json['condvalue1']

       cloud_con = cassandra_connect()
       cloud_config = cloud_con.cloud_config
       auth_provider = cloud_con.auth_provider
       cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
       session = cluster.connect()
       connt =  Cassandra_table_update()
       field1 = connt.cassa_config_upd()
       message = []

       data =  (value2,condvalue1)
       prepare_st = str((field1.upd_QUERY+' '+KEYSPACENAME+'.'+TABLENAME+' '+field1.set1+' '+Initial_atomic_coordinate_u+' '+\
                         field1.cond1+' '+Chiral_indice_n+';'))




       try:
           prepare_ud = session.prepare(prepare_st)
           session.execute(prepare_ud,data)
           message.append('updated into table Successfully')


       except Exception as e:
           message.append('Error in updating cassandra table')
           lg.error('Error has occurred')
           lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)