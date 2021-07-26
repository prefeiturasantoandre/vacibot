from credentials import connection_params
import database

db = database.Database_Oracle(connection_params)
MAX_RETRY = 3