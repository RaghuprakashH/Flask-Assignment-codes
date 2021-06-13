import logging as lg
import csv
import mysql.connector as connection
import mysql.connector.errors
from carbon_file import carbon_nano
from configread import dbfile1
from configread import BULK_INSERT
from  flask import Flask, render_template, request, jsonify
from carbon_file import carbon_nano

lg.basicConfig(filename = 'test3.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)
@app.route('/Bulk_insert_operation', methods=['POST'])  # for calling the API from Postman/SOAPUI
def Bulk_insert_operation_via_post():
    if (request.method == 'POST'):
           DBNAME = request.json['DBNAME']
           TABLENAME = request.json['TABLENAME']
           BULK_INSRT = request.json['BULK_INSRT']



           insertob = BULK_INSERT('x', DBNAME,TABLENAME)
           a = insertob.config_read()
           a.user
           a.root
           a.password

           insertts = insertob.config_bulk_insert()

           message = []


           use1 = str(insertts.FIELD2+' '+DBNAME+';')

           queryins = str(insertts.insert_QUERY+' '+TABLENAME+' '+insertts.value+'' '({})'';')

           #Reformat the file

           file = carbon_nano()
           file.carbon_file()
           count1 = 0
           count2 = 0
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
                            with open('new_file4.csv','r') as data:
                                data_csv = csv.reader(data,delimiter=";")
                                next(data_csv)
                                for i in data_csv:
                                    query = queryins.format(','.join([value for value in i]))
                                    cursor  = my_ins.cursor()
                                    cursor.execute(query)
                                    count2 = count2 + 1

                                    count1 = count1 + 1
                                    if count1 >= 1000:
                                        message.append('Inserted successfully')
                                        my_ins.commit()

                                my_ins.commit()

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
