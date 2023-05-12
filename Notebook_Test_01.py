#read data from  azure DB
import pyspark

password = "Nguyenquangn01"
jdbc_url = "jdbc:sqlserver://svdestudy.database.windows.net:1433;database=DE_Study"

df_order = (spark.read
  .format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", "OrderTbl")
  .option("user", "adminde")
  .option("password", "Nguyenquangn01")
  .load()
)
df_order.show()