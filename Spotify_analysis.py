#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 21:53:47 2022

@author: csuftitan
"""
#import libraries

import pandas as pd 
#import numpy as np
#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import seaborn as sns

import matplotlib.pyplot as plt

#read csv
df1 = pd.read_csv('/Users/csuftitan/Downloads/charts.csv')
df1.head()


#create spark session to create an entry point
spark = SparkSession.builder.appName("spark_app").getOrCreate()
#read csv file in spark
df = spark.read.csv(path='/Users/csuftitan/Downloads/charts.csv', inferSchema=True, header=True)

#change the column type 
df = df.withColumn("rank", f.col("rank").cast(t.LongType())).withColumn("date", f.col("date").cast(t.DateType())).withColumn("streams", f.col("streams").cast(t.IntegerType()))
#drop all te values with N/A
df = df.na.drop()
df.count()
#register temp table to issue SQL queries via the sqlContext. sql( sqlQuery ) method
df.registerTempTable("charts")

##############################################################
#extract data of rank,title which is at rank 1 in argentina
query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = 'Argentina' GROUP BY title  ORDER BY count DESC;" 
reg = spark.sql(query).toPandas().head(10) 

#plot the extracted data
fig = plt.bar(reg['title'],reg['count'])
plt.xticks(rotation=90)
plt.title('Title Vs Count')
plt.xlabel('title')
plt.ylabel('count')
plt.show()
##############################################################

#extract artist and number of days they have had song at rank 1
truy = spark.sql('''
SELECT artist, count(*) as days
FROM charts 
WHERE rank = 1 
group by artist
order by days DESC
''').toPandas().head(20)

list1 = truy['days'].tolist()
list2 = truy['artist'].tolist()


fig1=plt.bar(list2,list1,color = 'green',width = 0.4)
plt.title('Artists Vs Days(at rank 1)')
plt.xticks(rotation=90)
plt.xlabel('artists')
plt.ylabel('Days')
plt.show()
    
##############################################################

#extract data of Taylor swift when she has had song in top 5
artist_1_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE 'Taylor Swift';")
artist1_q = spark.sql(artist_1_query).toPandas()
artist1_q

a1 = artist1_q['count'].tolist()

#extract data of Shakira when she has had song in top 5
artist_2_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE 'Shakira';") #number of titmes artist 2 has been in top 5
artist2_q = spark.sql(artist_2_query).toPandas()

a2 = artist2_q['count'].tolist()
#extract data of Ed Sheeran when she has had song in top 5
artist_3_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE 'Ed Sheeran';") #number of titmes artist 3 has been in top 5
artist3_q = spark.sql(artist_3_query).toPandas()

a3 = artist3_q['count'].tolist()
my_data = [a1[0],a2[0],a3[0]]

my_labels = ['Taylor Swift', 'Shakira', 'Ed Sheeran']
fig3 = plt.pie(my_data, labels=my_labels, autopct='%1.1f%%')
plt.title('Top 3 songs in 5 years period')
plt.axis('equal')
plt.show()

###################################################################
    
    
# extract the number of times song was streamd on spotify 
q = spark.sql('''
SELECT title, SUM(streams) streams 
FROM charts 
WHERE streams IS NOT NULL 
GROUP BY title 
ORDER BY streams DESC;''').toPandas()
q.head(10)
stm = q.streams.tolist()
stm = stm[:50]
tit = q.title.tolist()
tit = tit[:50]

#chart_data = pd.DataFrame(stm[:10]columns=[])

df = pd.DataFrame({
  'Streams': stm,
  'Song title': tit
})

plt.title('Title Vs Streams')
plt.xlabel('title')
plt.ylabel('Streams')
df
plt.plot(df['Song title'],df['Streams'])
plt.xticks(rotation=90)
    
    
###################################################################
    

#based on song number of times it has changed treand in this region

query2 = ("SELECT region,count(trend) as trend FROM charts WHERE title like 'Shape of You' and trend like '%MOVE_UP%' group by region;")

art = spark.sql(query2).toPandas()

reg = art.region.tolist()
reg = reg[:30] #only top 30 as the list is huge
move_up = art.trend.tolist()
move_up = move_up[:30]

#save the above in a dataframe
df1 = pd.DataFrame({
  'move_up': move_up,
  'region': reg
})
df1

plt.title('Region Vs Trend')
plt.xlabel('Region')
plt.ylabel('Trend')
plt.bar(df1['region'],df1['move_up'])
plt.xticks(rotation=90)


############################################################################################
#extract the number of times Shape of you moved up in trend
query = ("Select count(*) as count from charts where trend like '%MOVE_UP%' and title like 'Shape of You'")
tre2= spark.sql(query).toPandas()
tre2 = tre2['count'].tolist()
#extract the number of times Shape of you moved down in trend
query1 = ("Select (*) as count from charts where trend like '%MOVE_DOWN%' and title like '%Shape of You%'")
tre1 = spark.sql(query).toPandas()
tre1 = tre1['count'].tolist()

#extract the number of times Shape of you stayed at same position in trend
query = ("Select count(*) as count from charts where trend like '%SAME_POSITION%' and title like '%Shape of You%'")
tre = spark.sql(query).toPandas()
tre = tre['count'].tolist()


trend = [tre2[0],tre1[0],tre[0]]
trend_type = ['MOVE UP','MOVE DOWN', 'SAME POSITION']
df = pd.DataFrame({
  'Trend': trend,
  'Type': trend_type
})
plt.title('Type of trend Vs Trend')
plt.xlabel('Type of trend')
plt.ylabel('Trend')
plt.plot(df['Type'],df['Trend'])
sns.countplot(data=df,x='Trend',hue='Type')

###############################################################################

#extract title year date rank and region of given songs
q = spark.sql('''
SELECT title,YEAR(date) year ,date ,rank, region 
FROM charts 
WHERE title IN ('Shape of You', 'Delicate', 'Blank Space') 
AND chart='top200'
ORDER BY rank ASC;''').toPandas()
q.head(20)

fig, axes = plt.subplots(figsize=(20,7))
sns.lineplot('date', 'rank', data=q, hue='title', ci=None).set_title('Trends in Top 200')

###############################################################################
#By running the below query we will get the top songs of Ed Sheeran
p = spark.sql('''
SELECT title, MIN(rank) rank, count(rank) count 
FROM charts 
WHERE artist LIKE '%Ed Sheeran%' 
AND chart = 'top200' 
AND rank <=5 
GROUP BY title;
''').toPandas().head(5)

sns.histplot(data=p, x="rank",y = 'count',hue='title')


