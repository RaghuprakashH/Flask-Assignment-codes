import cassandra
from cassandra.auth import PlainTextAuthProvider
class cassandra_connect():

    cloud_config = {
        'secure_connect_bundle': 'D:\secure-connect-test1.zip'
    }
    auth_provider = PlainTextAuthProvider('DzgSXTSzQgWNpYocAWPpQzAX',
                                          '27C9coC--crqmF0MiZldjv9Kg8NyhTzMP66SOPbHtiaNOWcidhyBz1FuOIuUp.,p2CajK266pu2QEhLkCNs4Zkt6qQaSce2cS_+10a9clpH6UhkdUkNtuBoTczw8sK_X')


class Cassandra_table_cre(cassandra_connect):
    pass
    def cassa_config_read(self):
        filename = 'CASS_TABLE_CRE'
        contents = open(filename).read()
        config = eval(contents)
        create_QUERY = config['create_QUERY']
        columns = config['columns']
        self.create_QUERY = create_QUERY
        self.columns = columns
        return self

class Cassandra_table_ins(cassandra_connect):
    pass
    def cassa_config_ins(self):
        filename = 'CASS_TABLE_INS'
        contents = open(filename).read()
        config = eval(contents)
        ins_QUERY = config['ins_QUERY']
        columns1 = config['columns1']
        values = config['values']
        self.ins_QUERY = ins_QUERY
        self.columns1 = columns1
        self.values = values
        return self


class Cassandra_table_blk_ins(cassandra_connect):
    pass
    def cassa_config_blk_ins(self):
        filename = 'CASS_TABLE_INS'
        contents = open(filename).read()
        config = eval(contents)
        ins_QUERY = config['ins_QUERY']
        columns1 = config['columns1']
        values = config['values']
        self.ins_QUERY = ins_QUERY
        self.columns1 = columns1
        self.values = values
        return self

class Cassandra_table_update(cassandra_connect):
    pass
    def cassa_config_upd(self):
        filename = 'CASS_UPD'
        contents = open(filename).read()
        config = eval(contents)
        upd_QUERY = config['upd_QUERY']
        set1 = config['set1']
        cond1 = config['cond1']
        self.upd_QUERY = upd_QUERY
        self.set1 = set1
        self.cond1 = cond1
        return self

class Cassandra_table_Delete(cassandra_connect):
    pass
    def cassa_config_del(self):
        filename = 'CASS_DEL'
        contents = open(filename).read()
        config = eval(contents)
        del_QUERY = config['del_QUERY']
        cond1 = config['cond1']
        self.upd_QUERY = del_QUERY
        self.cond1 = cond1
        return self
