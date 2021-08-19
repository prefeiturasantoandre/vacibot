from credentials import connection_params
import database

db = database.Database_Oracle(connection_params, "BDM")
MAX_RETRY = 3
WORKING_QUEUE = 5
DISPATCHER_WAIT = 15
MAX_WORKERS = 10