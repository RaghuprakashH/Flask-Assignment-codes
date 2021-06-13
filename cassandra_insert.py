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
@app.route('/cassandra_insertion', methods=['POST'])  # for calling the API from Postman/SOAPUI
def cassandra_ins_operation_via_post():
    if (request.method == 'POST'):
       KEYSPACENAME = request.json['KEYSPACENAME']
       TABLENAME = request.json['TABLENAME']
       Chiral_indice_n = request.json['Chiral_indice_n']
       Chiral_indice_m = request.json['Chiral_indice_m']
       Initial_atomic_coordinate_u = request.json['Initial_atomic_coordinate_u']
       cloud_con = cassandra_connect()
       cloud_config = cloud_con.cloud_config
       auth_provider = cloud_con.auth_provider
       cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
       session = cluster.connect()
       connt = Cassandra_table_ins()
       field1 = connt.cassa_config_ins()
       message = []
       data = []
       data =  [(Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u)]
       qr1 = str((field1.ins_QUERY+' '+KEYSPACENAME+'.'+TABLENAME+field1.columns1)+' '+field1.values)


       try:
          # for i in data:
          # qr1 = 'INSERT INTO ineuron3.CARBON (Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u) VALUES(%s,%s,%s)'
           batch = BatchStatement()
           for i in data:
               data1 = i
               batch.add(qr1,data1)
           session.execute(batch)
           message.append('Inserted into table Successfully')


       except Exception as e:
           message.append('Error in inserting cassandra table')
           lg.error('Error has occurred')
           lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)