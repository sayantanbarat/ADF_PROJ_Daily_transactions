# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where Sessionid, UserID, ProductID,   is null

# COMMAND ----------

from datetime import datetime
def current_datetime():
  cd=datetime.now()
  cp=cd.strftime('%d-%m-%Y')
  return '/mnt/landing/ECOM/' + cp

current_datetime()

# COMMAND ----------

#Write your code here
ppath=current_datetime()
df=spark.read.parquet(ppath)
#display(df)
df.createOrReplaceTempView("ecom")
df1=spark.sql("select * from ecom where Sessionid is not null and UserID is not null and ProductID is not null and Timestamp is not null ")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Replace all the null acton  with default action Login

# COMMAND ----------

#Write your code here
df1=df1.fillna(value="Login", subset=["Action"])
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Add the new column as Year and month and populate the value from timestamp column. And remove all the rows where u find null or empty timestamp column

# COMMAND ----------

#Write your code here
from pyspark.sql.functions import year,month
#from pyspark.sql.functions import current_timestamp
df1=df1.withColumn('Year', year("Timestamp")).withColumn('Month', month("Timestamp"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Store the data back into ADLS folder Staged

# COMMAND ----------

#Write your code here
def stgpath():
  cd=datetime.now()
  fm=cd.strftime('%d-%m-%Y')
  return "mnt/staged/ECOM/" + fm

stgpath()

# COMMAND ----------

stage_path=stgpath()
df1.write.parquet(path='mnt/staged/ECOM/21-06-2023', mode='Overwrite')

# COMMAND ----------


