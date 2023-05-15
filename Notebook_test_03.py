import pyodbc
 
 
def databaseConnection(driver, server, database, user, pwd):
    connectionString = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={user};PWD={pwd}"
    conn = pyodbc.connect(connectionString)
    sqlCMD = conn.cursor()
 
    sqlCMD.execute("SELECT  * FROM Test")
    row = sqlCMD.fetchone()
    while row:
        print(str(row[0]) + " " + str(row[1]))
        row = sqlCMD.fetchone()
 
 
def main():
    azureServer = "svdestudy.database.windows.net"
    azureDB = "DE_Study"
    userName = "adminde"
    password = "Nguyenquangn01"
    driver = "{ODBC Driver 18 for SQL Server}"
    databaseConnection(driver, azureServer, azureDB, userName, password)
 
 
if __name__ == '__main__':
    main()