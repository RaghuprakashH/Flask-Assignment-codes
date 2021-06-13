class dbfile1():
    def __init__(self ,parmdb ,DBNAME,TABLENAME):
        self.parmdb = parmdb
        self.DBNAME   = DBNAME
        self.TABLENAME  = TABLENAME

    def config_read(self):
        filename = 'externalconfig'
        contents = open(filename).read()
        config = eval(contents)
        user = config['user']
        password = config['password']
        root = config['root']
        print(user)
        self.user = user
        self.root = root
        self.password = password
        return self

    def config_read1(self):
        filename = 'DB_QUERY'
        contents = open(filename).read()
        config = eval(contents)
        DB_QUERY = config['DB_QUERY']
        DBNAME   = config['DBNAME']
        DELIMIT = config['DELIMIT']
        FIELD1  = config['FIELD1']
        FIELD2 = config['FIELD2']
        print(DB_QUERY)
        self.DB_QUERY = DB_QUERY
        self.DBNAME = DBNAME
        self.DELIMIT = DELIMIT
        self.FIELD1 = FIELD1
        self.FIELD2 = FIELD2
        return self

    def config_read_TAB(self):
        filename = 'TAB_QUERY'
        contents = open(filename).read()
        config = eval(contents)
        TAB_QUERY = config['TAB_QUERY']
        TABNAME   = config['TABNAME']
        COLUMNS  = config['COLUMNS']
        self.TAB_QUERY = TAB_QUERY
        self.TABNAME = TABNAME
        self.COLUMNS = COLUMNS
        return self

class INSERT(dbfile1):
        def __init__(self,DBNAME, TABLENAME,Chiral_indice_n,Chiral_indice_m,Initial_atomic_coordinate_u):
            self.DBNAME = DBNAME
            self.TABLENAME = TABLENAME
            self.Chiral_indice_n = Chiral_indice_n
            self.Chiral_indice_m = Chiral_indice_m
            self.Initial_atomic_coordinate_u = Initial_atomic_coordinate_u

        def config_insert(self):
            filename = 'INSERT'
            contents = open(filename).read()
            config = eval(contents)
            insert_QUERY = config['insert_QUERY']
            columns = config['columns']
            value = config['value']
            FIELD2  = config['FIELD2']
            self.insert_QUERY = insert_QUERY
            self.columns = columns
            self.value  = value
            self.FIELD2 = FIELD2
            return self
class BULK_INSERT(dbfile1):
    pass

    def config_bulk_insert(self):
            filename = 'INSERT'
            contents = open(filename).read()
            config = eval(contents)
            insert_QUERY = config['insert_QUERY']
            value = config['value']
            FIELD2 = config['FIELD2']
            self.insert_QUERY = insert_QUERY
            self.value = value
            self.FIELD2 = FIELD2
            return self

class DELETE_QRY(dbfile1):
    pass

    def config_delete(self):
            filename = 'DELETE'
            contents = open(filename).read()
            config = eval(contents)
            delete_QUERY = config['delete_QUERY']
            condition = config['condition']
            Fieldd = config['Fieldd']
            self.delete_QUERY = delete_QUERY
            self.condition = condition
            self.Fieldd = Fieldd
            return self

class UPDATE_QRY(dbfile1):
    pass

    def config_update(self):
            filename = 'UPDATE'
            contents = open(filename).read()
            config = eval(contents)
            update_QUERY = config['update_QUERY']
            set_values   = config['set_values']
            condition = config['condition']
            Fieldup  = config['Fieldup']
            self.update_QUERY = update_QUERY
            self.set_values = set_values
            self.condition = condition
            self.set_values = set_values
            self.Fieldup = Fieldup
            return self
class SELECT_QRY(dbfile1):
    pass

    def config_select(self):
            filename = 'SELECT'
            contents = open(filename).read()
            config = eval(contents)
            select_QUERY = config['select_QUERY']
            condition = config['condition']
            Fielduse  = config['Fielduse']
            self.select_QUERY = select_QUERY
            self.condition = condition
            self.Fielduse = Fielduse
            return self

