import logging as lg
import csv
import mysql.connector as connection
import mysql.connector.errors
from configread import dbfile1
from configread import INSERT
from configread import DELETE_QRY
from configread import UPDATE_QRY
from configread import SELECT_QRY
from  flask import Flask, render_template, request, jsonify
import pandas as pd



lg.basicConfig(filename = 'test3.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/SELECT_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def SELECT_operation_via_post():
    if (request.method == 'POST'):
           DBNAME = request.json['DBNAME']
           TABLENAME = request.json['TABLENAME']
           cond_fields = request.json['cond_fields']



           setob = SELECT_QRY('x',DBNAME,TABLENAME)
           a = setob.config_read()
           a.user
           a.root
           a.password

           insertts = setob.config_select()


           message = []


           use1 = str(insertts.Fielduse+' '+DBNAME+';')

           if cond_fields == ' ' :
              insertts.condition = ' '

           queryins = str(insertts.select_QUERY+' '+TABLENAME+' '+insertts.condition+' '+cond_fields+';')



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

                            with open('temp_file.csv', 'w') as out_file:
                                    cursor.execute(queryins)

                                    for result in cursor.fetchall():
                                         writer = csv.writer(out_file)
                                         writer.writerow(result)
                                #    print(list1)
                            data1 = pd.read_csv("temp_file.csv")
                            df = data1.dropna(axis=0, how='any')
                            df.to_csv('download_file.csv', index=False)

                            message.append('Downloaded successfully')
                        except Exception as e:
                            my_ins.close()
                            message.append('Error in deletion')
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