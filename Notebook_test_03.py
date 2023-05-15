import pyodbc

conn = pyodbc.connect(f'Driver={{ODBC Driver 18 for SQL Server}};Server=svdestudy.database.windows.net;Database=DE_Study;Uid=adminde;Pwd=Nguyenquangn01;Trusted_Connection=yes;')
cursor = conn.cursor()

print(conn)