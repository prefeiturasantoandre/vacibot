import cx_Oracle

class Database():
    def load(self):
        pass
    def execute(self, query):
        pass
    def fetch(self, table, where_col=None, where_value=None, where_op="="):
        pass
    def update(self, table, col, value, where_col, where_value, where_op="=", *next_where):
        pass
    def insert(self, table, cols, values):
        pass

class Database_Oracle(Database):
    constants = ["SYSDATE","SYSTIMESTAMP"]
    def __init__(self,connection_params=None, default_schema=None):
        self.connection_params = None
        self.default_schema = default_schema
        
        if connection_params:
            self.load(connection_params)

    def load(self,connection_params):
        self.connection_params = connection_params
        
    def execute(self, query):
        con = cx_Oracle.connect(self.connection_params)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()

    def fetch(self, table, where_col=None, where_value=None, where_op="="):
        if self.default_schema:
            table = self.default_schema + "." + table

        query = "SELECT * FROM "+ table

        if where_col and where_value:
            where_value = self.check_value(where_value)
            query = query + " WHERE " + where_col + where_op + str(where_value)
        
        con = cx_Oracle.connect(self.connection_params)
        cur = con.cursor()
        cur.execute(query)

        header = []
        rows = []
        for i in cur.description :
            header.append(i[0])
        for result in cur :
            rows.append(result)

        con.close()

        return header, rows

    def update(self, table, col, value, where_col, where_value, where_op="=", *next_where):
        value = self.check_value(value)
        where_value = self.check_value(where_value)

        if self.default_schema:
            table = self.default_schema + "." + table

        query = "UPDATE " + table + " SET " + col + "=" + str(value) + " WHERE " + where_col + where_op + str(where_value)

        i=0
        while len(next_where)-i >= 3:
            where_col2   = next_where[i+0]
            where_value2 = self.check_value( next_where[i+1] )
            where_op2    = next_where[i+2]
            query += " AND " + where_col2 + where_op2 + str(where_value2)
            i+=3

        self.execute(query)

    def insert(self, table, cols, values):
        for i in range(len(values)):
                values[i] = self.check_value(values[i])
        
        if self.default_schema:
            table = self.default_schema + "." + table

        query =  "INSERT INTO " + table
        query += " ("+ ','.join(cols) +")"
        query += " VALUES ("+ ','.join(values) +")"
        
        self.execute(query)

    def check_value(self, value):
        if isinstance(value ,str) and value not in self.constants:
            value = f"'{value}'"
        return value
