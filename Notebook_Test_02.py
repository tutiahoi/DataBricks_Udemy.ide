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

    print(pyodbc.drivers())

    database = "DE_Study"
    user = "adminde"
    password  = "Nguyenquangn01"
    Driver='{ODBC Driver 18 for SQL Server};Server=tcp:svdestudy.database.windows.net,1433;Database=DE_Study;Uid=adminde;Pwd=Nguyenquangn01;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    #conn = pyodbc.connect(f'Driver={{ODBC Driver 18 for SQL Server}};Server=svdestudy.database.windows.net,1433;Database=DE_Study;Uid=adminde;Pwd=Nguyenquangn01;Trusted_Connection=yes;')
    conn = pyodbc.connect(Driver)
    print(conn)
    # database = "DE_Study"
    # user = "sa"
    # password  = "Nguyenquangn01"

    # conn = pyodbc.connect(f'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER=localhost,1433;DATABASE={database};UID={user};PWD={password}')
    
 
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
strsql = strsql.format(7,"'SSI'","'SSI'")
result = Wirte_DB_SQLServer(strsql)
print('qua')