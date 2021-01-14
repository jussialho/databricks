# Databricks notebook source
# CSV - SQL practices
# Student name: Jussi Alho

from pyspark.sql.types import *

# NYSE file format
#==================
# exchange stock_symbol date stock_price_open stock_price_high stock_price_low stock_price_close stock_volume stock_price_adj_close
# NYSE	ASP	2001-12-31	12.55	12.8	12.42	12.8	11300	6.91


inputFile = "/FileStore/tables/NYSE_2000_2001.tsv"

stock = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .option("delimiter", "\t")\
  .load(inputFile)

# You can try these
# stock.printSchema()
# stock.show()

# this enables the SQL commands
stock.registerTempTable("stockT")
# stockT works now as a database table

# res = sqlContext.sql("select * \
#                    from stockT")
##################################
# 1. the maximum price change within a day for company named MTA
res = sqlContext.sql("select max(stock_price_high-stock_price_low) \
                     from stockT \
                     where company='MTA'")

# Describe your results:
# Maximun price change within a dat for MTA is 2.75
##################################
# 2. the maximum price change within a day for all companys, sorted by company name,
# values rounded by 1 decimal and column renamed
#res = sqlContext.sql("select company,ROUND((max(stock_price_high-stock_price_low)),1) \
#                     from stockT \
#                     group by company \
#                     order by company asc")

#res = res.withColumnRenamed("round(max((stock_price_high - stock_price_low)), 1)", "Max price change").distinct()
##################################
# 3. the maximum price change within a day for all companys, sorted by biggest change,
# showing only 10 highest, column renamed and values rounded by 1 decimal
#res = sqlContext.sql("select company,ROUND((max(stock_price_high-stock_price_low)),1) AS MaxChange \
#                     from stockT \
#                     group by company \
#                     order by MaxChange desc \
#                     limit 10")
##################################
# 4. the maximum price change from open till close within a day for all companys, sorted by biggest change,
# showing only 10 highest, column renamed and values rounded by 1 decimal
#res = sqlContext.sql("select company,ROUND((max(stock_price_close-stock_price_open)),1) AS maxChangeOpen \
#                     from stockT \
#                     group by company \
#                     order by maxChangeOpen desc \
#                     limit 10")

# Analyze why the graphs from tasks 3 and 4 are so different.
# Because in task 3 values are highest and lowest in total and in task 4 they are open and close values only.

# How about prices downwards in a day?
# The maximum price change from close till open within a day for all companys, sorted by biggest change
#res = sqlContext.sql("select company,ROUND((max(stock_price_OPEN-stock_price_CLOSE)),1) AS maxChangeDown \
#                     from stockT \
#                     group by company \
#                     order by maxChangeDown asc \
#                     limit 10")

# Let's see the data what was wrong, look at BE sales in more detail:
#res = sqlContext.sql("select ROUND((max(stock_price_close-stock_price_open)),1) \
#                     from stockT \
#                     where company='BE'")

# You could see that the data was incomplete, since some of the prices were 0. We need to get rid of those lines (clean the data)
#
# the maximum price change within a day for all companys, sorted by biggest change and cleaned
#res = sqlContext.sql("select company,ROUND((max(stock_price_close-stock_price_open)),1) AS MaxChange \
#                     from stockT \
#                     where stock_price_close > 0 and stock_price_open > 0 \
#                     group by company \
#                     order by MaxChange desc")

# or you could permanently delete them with
#res = sqlContext.sql("delete from stockT \
#                      where stock_price_open = 0 or stock_price_close = 0")

# EGY seems to be the company with big changes in a day.
# Let's look deeper into the changes
#res = sqlContext.sql("select company,date,ROUND((stock_price_close-stock_price_open),1),stock_price_open,stock_price_low,stock_price_high,stock_price_close \
#                     from stockT \
#                     where company='EGY' \
#                     order by stock_price_close-stock_price_open desc")

# So you can see that during those days the prices only got higher, but also a lot higher.
# Since the whole August 2000 seemed to be strange, let's look even deeper.
#res = sqlContext.sql("select company,date,stock_volume,stock_price_close-stock_price_open,stock_price_open,stock_price_low,stock_price_high,stock_price_close \
#                     from stockT \
#                     where company='EGY' and (date between '2000-08-01' and '2000-08-31') \
#                     order by date")

# Now this is as far as we could go with simple SQL statements and this data.
# Perhaps someone could already from this see something.
##################################
# 5. But losing value is also something to look at, let's reverse the view
#res = sqlContext.sql("select company,ROUND((max(stock_price_open-stock_price_close)),1) \
#                     from stockT \
#                     where stock_price_open-stock_price_close > 0 \
#                     group by company \
#                     order by max(stock_price_open-stock_price_close) desc")

# Let's look MTA a bit deeper
#res = sqlContext.sql("select company,date,stock_volume,stock_price_open-stock_price_close,stock_price_open,stock_price_high,stock_price_low,stock_price_close \
#                     from stockT \
#                     where company='MTA' \
#                     order by stock_price_open-stock_price_close desc \
#                     limit 4")

# We see that there is a very strange day: 2000-05-26
# Analyse the data around that day deeper.
#res = sqlContext.sql("select company,date,stock_volume,ROUND((stock_price_open-stock_price_close),1),stock_price_open,stock_price_high,stock_price_low,stock_price_close \
#                     from stockT \
#                     where company='MTA' and (date between '2000-05-15' and '2000-06-10') \
#                     order by stock_price_open desc")
                     
# We see that 2000-05-26 was an exceptional date for this share. What is your analysis of this situation?
# Answer:
# Would it be some kind of an error because stock_price_open value for this day is way different from others within few weeks time. Or then something really weird happened that day.

# After doing all the task, answer to this question: what kind of professions would benefit from this kind of data processing?
# Answer:
# Bank workers, economists, analysts and all interested in stock markets.

# Create one more SQL command that you think would be good in that/those professions:
# Answer:
# The minimum price change from open till close within a day for all companys, sorted by biggest change,
# showing only top 10, column renamed and values rounded by 1 decimal
#res = sqlContext.sql("select company,ROUND((min(stock_price_close-stock_price_open)),1) AS minChangeOpen \
#                     from stockT \
#                     group by company \
#                     order by minChangeOpen asc \
#                     limit 10")

# What are the main problems of this data?
# Answer:
# Huge amount of values looking almost the same, long decimal fractions

res.show();
# or
# display(res)
