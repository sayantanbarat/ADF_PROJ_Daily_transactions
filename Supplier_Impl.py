# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where ProductID   is null

# COMMAND ----------

#Write your code here
from datetime import datetime
def sup_path():
  dt=datetime.now()
  fp=dt.strftime('%d-%m-%Y')
  return "/mnt/landing/POS/" + fp
sup_path()

# COMMAND ----------

ppath=sup_path()
df1=spark.read.parquet(ppath)
#display(df1)
df1.createOrReplaceTempView("sup")
df2=spark.sql('select * from sup where ProductID is not null ')
display(df2)


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure QuantityInStock should be non-negative.

# COMMAND ----------

#Write your code here
ppath=sup_path()
df1=spark.read.parquet(ppath)
#display(df1)
df1.createOrReplaceTempView("sup")
df2=spark.sql('select * from sup where ProductID is not null and Quantity>0')
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Store the data back into ADLS folder Staged

# COMMAND ----------

#Write your code here
def stg_path():
  dt=datetime.now()
  fp=dt.strftime('%d-%m-%Y')
  return '/mnt/staged/SUP/' + fp
stg_path()

# COMMAND ----------

stpp=stg_path()
df2.write.parquet(stpp)

# COMMAND ----------


