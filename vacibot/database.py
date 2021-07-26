import cx_Oracle

class Database():
    def load(self):
        pass
    def execute(self, query):
        pass
    def fetch(self, table, where_col=None, where_value=None, where_op="="):
        pass
    def update(self, table, col, value, where_col, where_value, where_op="="):
        pass

class Database_Oracle(Database):
    def __init__(self,connection_params=None):
        self.connection_params = None
        
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
        con = cx_Oracle.connect(self.connection_params)
        cur = con.cursor()

        query = "SELECT * FROM "+ table

        if where_col and where_value:
            if isinstance(where_value ,str):
                where_value = f"'{where_value}'"
            query = query + " WHERE " + where_col + where_op + where_value
        
        cur.execute(query)

        header = []
        rows = []
        for i in cur.description :
            header.append(i[0])
        for result in cur :
            rows.append(result)

        con.close()

        return header, rows

    def update(self, table, col, value, where_col, where_value, where_op="="):
        con = cx_Oracle.connect(self.connection_params)
        cur = con.cursor()

        if isinstance(value ,str):
                value = f"'{value}'"
        if isinstance(where_value ,str):
            where_value = f"'{where_value}'"

        query = "UPDATE " + table + " SET " + col + "=" + value + " WHERE " + where_col + where_op + where_value

        cur.execute(query)
        con.commit()
        con.close()
