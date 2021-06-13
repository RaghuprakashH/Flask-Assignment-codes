import pymongo

class mongodb_conn_class():
    pass
    def mongodb_conn(self):
        filename = 'MONGO_USER'
        contents = open(filename).read()
        config = eval(contents)
        USER = config['USER']
        self.USER = USER
        return self

    def checkExistence_DB(self,DBNAME,client_cloud,msg):
        """It verifies the existence of DB"""
        self.DBNAME = DBNAME
        self.client = client_cloud
        self.msg = msg
        DBlist = self.client.list_database_names()
        if self.DBNAME in DBlist:
           self.msg = (f"DB: '{self.DBNAME}' exists")
        else:
           self.msg= (f"DB: '{self.DBNAME}' not yet present OR no collection is present in the DB")
        return self

    def checkExistence_COL(self,COLLECTION1,DBNAME,db1,msg):
        """It verifies the existence of collection name in a database"""
        self.COLLECTION_NAME = COLLECTION1
        self.DBNAME = DBNAME
        self.db = db1
        self.msg =msg
        collection_list = self.db.list_collection_names()
        if self.COLLECTION_NAME in collection_list:
            self.msg = (f"Collection:'{self.COLLECTION_NAME}' in Database:'{self.DBNAME}' exists")

        else:
            self.msg = (f"Collection:'{self.COLLECTION_NAME}' in Database:'{self.DBNAME}' does not exists OR \n\
            no documents are present in the collection")
        return  self


