import logging as lg
import csv
import mysql.connector as connection
import mysql.connector.errors
from configread import dbfile1
from configread import INSERT
from  flask import Flask, render_template, request, jsonify


lg.basicConfig(filename = 'test3.log',level=lg.INFO,format = '%(asctime)s %(message)s')
app = Flask(__name__)

@app.route('/DB_CREATION', methods=['POST']) # for calling the API from Postman/SOAPUI
def DB_operation1():
    if (request.method=='POST'):
        mysql_parm = request.json['mysql_parm']
        DBNAME = request.json['DBNAME']
        TABLENAME = request.json['TABLENAME']
        test1 = dbfile1(mysql_parm,DBNAME,TABLENAME)
        test2 = test1.config_read()
        testdb = test1.config_read1()
        testtab =    test1.config_read_TAB()
        dbquery = testdb.DB_QUERY
        dbname = DBNAME
        delimit = testdb.DELIMIT
        field1 = testdb.FIELD1
        field2 = testdb.FIELD2
        db_query = str(dbquery+' '+dbname+' '+delimit)
        tabquery = testtab.TAB_QUERY
        tabnam  = TABLENAME
        cols   = testtab.COLUMNS
        tab_qry = str(tabquery+' '+tabnam+' '+cols )
        use1 = str(field2+' '+dbname+';')
        message = []
        try:
            my_db = connection.connect(host=test1.user, user=test1.root, passwd=test1.password)
            cursor = my_db.cursor()
            if my_db.is_connected():
               message.append('MYSQL is connected')
               try:
                   if my_db.is_connected():
                       cursor = my_db.cursor()
                       cursor.execute(db_query)
                       message.append('created Database successfully')
                       try:
                           query = field1
                           cursor = my_db.cursor()  # create a cursor to execute queries
                           cursor.execute(query)
                           msg = cursor.fetchall()
                           message.append(msg)
                           try:
                               usequery = use1
                               cursor = my_db.cursor()  # create a cursor to execute queries
                               cursor.execute(usequery)
                               message.append('DATABASE USED')
                               try:
                                   cursor = my_db.cursor()  # create a cursor to execute queries
                                   cursor.execute(tab_qry)
                                   message.append('CREATED SUCCESSFULLY')
                               except Exception as e:
                                   my_db.close()
                                   message.append('Error in creating table')
                                   lg.error('Error in creating table')
                                   lg.exception(str(e))

                           except Exception as e:
                               my_db.close()
                               message.append('Error in Database used')
                               lg.error('Error in creating table')
                               lg.exception(str(e))

                       except Exception as  e:
                           my_db.close()
                           message.append('Error in listing Database')
                           lg.error('Error has occurred')
                           lg.exception(str(e))

               except mysql.connector.errors.Error as e:
                   message.append('Error in creating Database')
                   lg.error('Error has occurred')
                   my_db.close()
                   lg.exception(str(e))


        except mysql.connector.errors.Error as e:
            message.append('error in connection')
            lg.error('Error has occurred')
            lg.exception(str(e))


    return jsonify(message)




if __name__ == '__main__':
  app.run(debug=True)