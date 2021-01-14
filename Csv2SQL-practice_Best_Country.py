# Databricks notebook source
# CSV - SQL practices
# Task 2 - Best Country to Live
# Student name: Jussi Alho

from pyspark.sql.types import *

###### File 1 ######
file_locationSE = "/FileStore/tables/SchoolEnrollmentSecondary.csv"

schoolenrollment = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .option("delimiter", ";")\
  .load(file_locationSE)

#schoolenrollment.printSchema()
#schoolenrollment.show()

# Enables the SQL commands
schoolenrollment.registerTempTable("schoolT")

#res = sqlContext.sql("select * \
#                    from schoolT")

###### File 2 ######
file_locationER = "/FileStore/tables/AdjustedNetEnrollmentRatePrimaryFemale.csv"

enrollmentrate = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .option("delimiter", ";")\
  .load(file_locationER)

#enrollmentrate.printSchema()
#enrollmentrate.show()

# Enables the SQL commands
enrollmentrate.registerTempTable("enrollT")

#res = sqlContext.sql("select * \
#                    from enrollT")

###### File 3 ######
file_locationHI = "/FileStore/tables/humanindex-2.csv"

humanindex = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .option("delimiter", ";")\
  .load(file_locationHI)

#humanindex.printSchema()
#humanindex.show()

# Enables the SQL commands
humanindex.registerTempTable("humanT")

#res = sqlContext.sql("select * \
#                    from humanT")

##################################
# TESTING

#res = sqlContext.sql("select Country,ROUND(SchoolEnrollment2015,2) AS SchoolEnrollment2015 \
#                     from schoolT \
#                     order by SchoolEnrollment2015 desc \
#                     limit 10")

#res = sqlContext.sql("select Country,ROUND(AdjEnrollmentPrimaryFemale2014,2) AS AdjEnrollmentPrimaryFemale2014 \
#                     from enrollT \
#                     order by AdjEnrollmentPrimaryFemale2014 desc \
#                     limit 10")

#res = sqlContext.sql("select Country,HumanCapital \
#                     from humanT \
#                     order by HumanCapital desc \
#                     limit 10")

#ANALYZE
res = sqlContext.sql("select Country,ROUND((SchoolEnrollment2015/200*10+AdjEnrollmentPrimaryFemale2014+HumanCapital*10),2) AS Total \
                     from schoolT \
                     JOIN enrollT USING (Country) \
                     JOIN humanT USING (Country) \
                     ORDER BY Total Desc \
                     Limit 3")
res.show();


# COMMAND ----------



# COMMAND ----------


