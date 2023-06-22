# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where TransactionID, StoreID, ProductID,  is null

# COMMAND ----------

#Write your code here
from datetime import datetime
def pos_dt():
  cd=datetime.now()
  fp=cd.strftime('%d-%m-%Y')
  return '/mnt/landing/POS/' + fp
pos_dt()

# COMMAND ----------

pp=pos_dt()
df2=spark.read.parquet(pp)
display(df2)

# COMMAND ----------

df2=df2.createOrReplaceTempView('pos_vw')
df2=spark.sql('select * from pos_vw where TransactionID is not null and StoreID is not null and  ProductID is not null')
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replace all the null price with default price 1000

# COMMAND ----------

#Write your code here
df2_2=df2.fillna(value='1000', subset=["Price"])
display(df2_2)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Round of all the price to integer number

# COMMAND ----------

#Write your code here
from pyspark.sql.functions import round
df2_2=df2_2.select("*",round("Price"))
df2_2.createOrReplaceTempView('dff')
df_pos=spark.sql('select TransactionID,StoreID,ProductID,Quantity,TotalSale,Timestamp,round(Price, 0) as Price from dff')
display(df_pos)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Store the data back into ADLS folder Staged

# COMMAND ----------

#Write your code here
def stg_path_pos():
  dt=datetime.now()
  fp=dt.strftime('%d-%m-%Y')
  return "/mnt/staged/POS/" + fp

stg_path_pos()


# COMMAND ----------

sp=stg_path_pos()
df_pos.write.parquet(sp,mode='Overwrite')

# COMMAND ----------


