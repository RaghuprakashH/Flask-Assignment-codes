import logging as lg
import os
import csv
import mysql.connector as connection
import cassandra
from cassandra.cluster import Cluster
from  flask import Flask, render_template, request, jsonify
from cassandra_config import cassandra_connect
from cassandra_config import Cassandra_table_ins
from cassandra_config import Cassandra_table_cre
from cassandra_config import Cassandra_table_blk_ins
import cassandra
from cassandra.query import SimpleStatement, BatchStatement
from carbon_file import carbon_nano

lg.basicConfig(filename = 'test4.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/cassandra_bulk_insertion', methods=['POST'])  # for calling the API from Postman/SOAPUI
def cassandra_bulk_ins_operation_via_post():
    if (request.method == 'POST'):
       KEYSPACENAME = request.json['KEYSPACENAME']
       TABLENAME = request.json['TABLENAME']
       bulk_load = request.json['bulk_load']


       cloud_con = cassandra_connect()
       cloud_config = cloud_con.cloud_config
       auth_provider = cloud_con.auth_provider
       cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
       session = cluster.connect()
       connt = Cassandra_table_ins()
       field1 = connt.cassa_config_ins()
       message = []


       query = str((field1.ins_QUERY+' '+KEYSPACENAME+'.'+TABLENAME+field1.columns1)+' '+field1.values)
       file = carbon_nano()
       file.carbon_file()
       data2 = []
       A = []
       count0 = 0
       try:
          # for i in data:
           qr1 = 'INSERT INTO ineuron3.BP752 (Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u) VALUES(%s,%s,%s)'
           batch = BatchStatement()
           with open('new_file4.csv', newline='') as csvfile:
               data = csv.DictReader(csvfile)
               for row in data:
                    B = row['Chiral_indice_n']
                    C = row['Chiral_indice_m']
                    D = row['Initial_atomic_coordinate_u']
                    B1 = int(B)
                    C1 = int(C)
                    D1 = float(D)
                    A.append([B1,C1,D1])



               for j in A:
                   data2 = j
                   count0 = count0 + 1
                   print(count0)
                   batch.add(query, data2)

               session.execute(batch)
               message.append('Inserted into table Successfully')


       except Exception as e:
           message.append('Error in inserting cassandra table')
           lg.error('Error has occurred')
           lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)