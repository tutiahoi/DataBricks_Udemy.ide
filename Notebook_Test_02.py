import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
import pyodbc
import pandas as pd

def Conn_SQLServer():
    appName = "PySpark SQL Server Example - via ODBC"
    master = "local"
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) 

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession

    database = "DE_Study"
    user = "adminde"
    password  = "Nguyenquangn01"

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=tcp:svdestudy.database.windows.net,1433;DATABASE={database};UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;')
    
    return conn

def Read_DB_SQLServer(query):
    conn = Conn_SQLServer()
    pdf = pd.read_sql(query, conn)
    df = pd.DataFrame(pdf)

    conn.commit()

    return df

def Wirte_DB_SQLServer(query):
    if query!= None:
        conn = Conn_SQLServer()
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
    
strsql ="Insert into dbo.Test(ID,Code,Description) values({},{},{})"
strsql = strsql.format(6,"'SSI'","'SSI'")
result = Wirte_DB_SQLServer(strsql)
print('qua')