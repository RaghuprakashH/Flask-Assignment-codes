import logging as lg
import csv
import mysql.connector as connection
import mysql.connector.errors
from configread import dbfile1
from configread import INSERT
from  flask import Flask, render_template, request, jsonify



lg.basicConfig(filename = 'test3.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/insert_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def insert_operation_via_post():
    if (request.method == 'POST'):
           DBNAME = request.json['DBNAME']
           TABLENAME = request.json['TABLENAME']
           Chiral_indice_n =int(request.json['Chiral_indice_n'])
           Chiral_indice_m = int(request.json['Chiral_indice_m'])
           Initial_atomic_coordinate_u = int(request.json['Initial_atomic_coordinate_u'])


           insertob = INSERT(DBNAME, TABLENAME,Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u)
           a = insertob.config_read()
           a.user
           a.root
           a.password

           insertts = insertob.config_insert()

           form1=(Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u)
           message = []


           use1 = str(insertts.FIELD2+' '+DBNAME+';')

           queryins = str(insertts.insert_QUERY+' '+TABLENAME+' '+insertts.columns)



           try:
                my_ins = connection.connect(host=a.user, user=a.root, passwd=a.password)
                cursor = my_ins.cursor()
                if my_ins.is_connected():
                    message.append('MYSQL is connected')
                    try:
                        cursor = my_ins.cursor()  # create a cursor to execute queries
                        cursor.execute(use1)
                        message.append('DATABASE USED')
                        try:
                            cursor  = my_ins.cursor()
                            cursor.execute(queryins,form1)
                            my_ins.commit()
                            message.append('Inserted successfully')
                        except Exception as e:
                            my_ins.close()
                            message.append('Error in insering')
                            lg.error('Error has occurred')
                            lg.exception(str(e))

                    except Exception as e:
                        my_ins.close()
                        message.append('Error in using Database')
                        lg.error('Error has occurred')
                        lg.exception(str(e))






           except mysql.connector.errors.Error as e:
                  message.append('error in connection')
                  lg.error('Error has occurred')
                  lg.exception(str(e))

    return jsonify(message)


if __name__ == '__main__':
  app.run(debug=True)