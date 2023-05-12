from multiprocessing import connection
from unittest import result
import DB_Connection 
import pandas as pd

def Read_DB_SQLServer(query):
    conn = DB_Connection.Conn_SQLServer()
    pdf = pd.read_sql(query, conn)
    df = pd.DataFrame(pdf)

    conn.commit()

    return df

def Wirte_DB_SQLServer(query):
    if query!= None:
        conn = DB_Connection.Conn_SQLServer()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
    
            conn.commit()
            conn.close()
            return "successfull"
        except conn.DataError as err:
            return "error data"

    else:
        return "Error by query command!!!!!"