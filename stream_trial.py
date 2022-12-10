#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 21:53:47 2022

@author: csuftitan
"""
import time
import streamlit as st
st.title('Spotify Data Explorer')
st.text('This is a web app to allow exploration of Spotify Charts Data')

import pandas as pd 
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import seaborn as sns

import matplotlib.pyplot as plt

add_selectbox = st.sidebar.selectbox(
    "What would you like to look at first",
    ("Big data analysis", "Songs","About")
)

#df1 = pd.read_csv('/Users/csuftitan/Downloads/charts.csv')
df1 = pd.read_csv('https://drive.google.com/file/d/1fUjXJI49cMAsYlnU_l78gKOM5eKqx0os/view?usp=share_link')
df1.head()
st.header('Header of Dataframe')
st.write(df1.head())

spark = SparkSession.builder.appName("spark_app").getOrCreate()

#df = spark.read.csv(path='/Users/csuftitan/Downloads/charts.csv', inferSchema=True, header=True)
df = spark.read.csv(path='https://drive.google.com/file/d/1fUjXJI49cMAsYlnU_l78gKOM5eKqx0os/view?usp=share_link', inferSchema=True, header=True)


df = df.withColumn("rank", f.col("rank").cast(t.LongType())).withColumn("date", f.col("date").cast(t.DateType())).withColumn("streams", f.col("streams").cast(t.IntegerType()))
df = df.na.drop()
df.count()
df.registerTempTable("charts")


if add_selectbox == 'Big data analysis':    
    n_country = st.selectbox(label='Please select a country', options=df1.region.unique()) 
    
    query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = '{}' GROUP BY title  ORDER BY count DESC;".format(n_country) 
     
    reg = spark.sql(query).toPandas().head(10) 
    st.subheader("Based on the country selected, below graph represents which song has been at rank 1 most number of times")
    check = st.checkbox('View query')
    if check:
        st.code('query = "SELECT title, count(title) AS count FROM charts WHERE rank = 1 and region = {} GROUP BY title  ORDER BY count DESC;".format(n_country)')
    fig = plt.bar(reg['title'],reg['count'])
    plt.xticks(rotation=90)
    plt.title('Title Vs Count')
    plt.xlabel('title')
    plt.ylabel('count')
    st.pyplot()
    st.set_option('deprecation.showPyplotGlobalUse', False)
    # Create a section for the dataframe statistics
    ##############################################################
    truy = spark.sql('''
    SELECT artist, count(*) as days
    FROM charts 
    WHERE rank = 1 
    group by artist
    order by days DESC
    ''').toPandas().head(20)
    
    list1 = truy['days'].tolist()
    list2 = truy['artist'].tolist()
    
    
    st.subheader("Below graph reprents the number of days an artist has had songs at rank 1")
    
    if check:
        st.code('''SELECT artist, count(*) as days FROM charts WHERE rank = 1 group by artist order by days DESC''')
    fig1=plt.bar(list2,list1,color = 'green',width = 0.4)
    plt.title('Artists Vs Days(at rank 1)')
    plt.xticks(rotation=90)
    plt.xlabel('artists')
    plt.ylabel('Days')
    st.pyplot()
    st.set_option('deprecation.showPyplotGlobalUse', False)
    
    ##############################################################
    options = st.multiselect(
        'Select 3 artists',
        df1.artist.unique())
    
    st.write('Graph will appear only after you select 3 options from the list')
    
    
    if st.button("Compare"):
        if len(options) == 3:
            st.write('Below is a comparison of number of times selected artists have had songs in top 5')
            art1 = options[0]
            art2 = options[1]
            art3 = options[2]
            artist_1_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE '{}';").format(art1)
            artist1_q = spark.sql(artist_1_query).toPandas()
            a1 = artist1_q['count'].tolist()
        
        
            artist_2_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE '{}';").format(art2) #number of titmes artist 1 has been in top 5
            artist2_q = spark.sql(artist_2_query).toPandas()
            a2 = artist2_q['count'].tolist()
        
            artist_3_query =("SELECT Count(*) as count FROM charts WHERE rank <= 5 and artist LIKE '{}';").format(art3) #number of titmes artist 1 has been in top 5
            artist3_q = spark.sql(artist_3_query).toPandas()
            a3 = artist3_q['count'].tolist()
        
        
            my_data = [a1[0],a2[0],a3[0]]
        
            my_labels = [art1, art2, art3]
            fig3 = plt.pie(my_data, labels=my_labels, autopct='%1.1f%%')
            plt.title('Top 5 songs in 5 years period')
            plt.axis('equal')
        
            st.pyplot()
            st.set_option('deprecation.showPyplotGlobalUse', False)
        else:
            st.warning('PLease select only three artists', icon="⚠️")
    ###################################################################
    
    
    
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
    
    st.subheader('Below graph shows the number of streams per songs')
    
    if check:
        st.code('''SELECT title, SUM(streams) streams FROM charts WHERE streams IS NOT NULL GROUP BY title ORDER BY streams DESC;''')
    df
    df = df.rename(columns={'Song title':'index'}).set_index('index')
    st.line_chart(df)
    
    
    ###################################################################
    #st.date_input('Date input')
    
    
    #based on song number of times it has changed treand in this region
    n_song = st.selectbox(label='Please select a song', options=df1.title.unique()) 
    
    query2 = ("SELECT region,count(trend) as trend FROM charts WHERE title like '{}' and trend like '%MOVE_UP%' group by region;").format(n_song)
    
    art = spark.sql(query2).toPandas()
    
    reg = art.region.tolist()
    reg = reg[:30]
    move_up = art.trend.tolist()
    move_up = move_up[:30]
    
    
    
    
    df1 = pd.DataFrame({
      'move_up': move_up,
      'region': reg
    })
    
    df1
    st.subheader('Below graph represents the number of times the selected song has moved up in position in perticular region')
    df1
    
    if check:
        st.code('''"SELECT region,count(trend) as trend FROM charts WHERE title like '{}' and trend like '%MOVE_UP%' group by region;").format(n_song)''')
    df1 = df1.rename(columns={'region':'index'}).set_index('index')    
    st.bar_chart(df1)
    
    
    
    #st.hist
    
    ##########################################################################################
    #rank vs artist vs year
    
    #query3 = spark.sql("SELECT title,YEAR(date) year ,date ,rank, region FROM charts WHERE title like '{}' AND chart='top200' ORDER BY rank ASC").format(n_song)
    #vis = spark.sql(query3).toPandas().head(10)
    
    
    query = ("Select Year(date) year, rank from charts where title = '{}'").format(n_song)
    runn = spark.sql(query).toPandas()
    
    year_list = runn['year'].tolist()
    rank_list = runn['rank'].tolist()
    
    st.subheader('Ranks vs Year of selected song')
    
    if check:
        st.code('''"Select Year(date) year, rank from charts where title = '{}'").format(n_song)''')
    
    fig_box = sns.boxplot(runn['year'],runn['rank'], palette="husl", data=runn)
    st.pyplot()
    st.set_option('deprecation.showPyplotGlobalUse', False)
    ############################################################################################
    query = ("Select count(*) as count from charts where trend like '%MOVE_UP%' and title like '{}'").format(n_song)
    tre2= spark.sql(query).toPandas()
    tre2 = tre2['count'].tolist()
    
    query1 = ("Select (*) as count from charts where trend like '%MOVE_DOWN%' and title like '%Shape of You%'")
    tre1 = spark.sql(query).toPandas()
    tre1 = tre1['count'].tolist()
    
    query = ("Select count(*) as count from charts where trend like '%SAME_POSITION%' and title like '%Shape of You%'")
    tre = spark.sql(query).toPandas()
    tre = tre['count'].tolist()
    
    st.subheader('Below graph represents the number to times a song has changed its position')
    
    if check:
        st.code('''"Select count(*) as count from charts where trend like '%MOVE_UP%' and title like '{}'").format(n_song)''')
    trend = [tre2[0],tre1[0],tre[0]]
    trend_type = ['MOVE UP','MOVE DOWN', 'SAME POSITION']
    df = pd.DataFrame({
      'Trend': trend,
      'Type': trend_type
    })
    df = df.rename(columns={'Type':'index'}).set_index('index')
    st.line_chart(df)
    
###############################################################################
    st.subheader("Below data is an extraction of highest lowest and average rank of all the songs by selected atrist")
    n_art = st.selectbox(label='Please select an artist', options=df1.artist.unique())
    avge = spark.sql("SELECT Title, MIN(rank) Highest, MAX(rank) Lowest, AVG(rank) Avg FROM charts WHERE artist like '{}' AND chart='top200' GROUP BY title ORDER BY Highest;").format(n_art)
    avge
    
    # Draw
    
    
    
elif add_selectbox == 'Song':
    n_artist = st.selectbox(label='Please select your favourite artist', options=df1.artist.unique()) 
    st.subheader("You can Select your favourite artist and get the list of songs and the urls to that song below")
    song = ("Select title,url from charts where artist like 'Taylor Swift'")
    song_panda = spark.sql(song).toPandas()
    st.write(song_panda)
elif add_selectbox == 'About':
    st.subheader("A little about us")
    st.write("Spotify Data Explorer is a web app that will help Spotify users to understand the trends in their favorite singer’s songs and their popularity. Artists can compare their successful songs with competitors and know where they stand in popularity among Spotify users. It will also suggest the region where the song is most streamed in and the region where the song had the highest and lowest rank in the form of graphs which will help the users understand the trends better than in a tabular format. Using the data, the user will also be able to predict if an artist will have a song in the ‘top 200’ category.")
    st.write('If you would like to contact us, please find our details below:')
    st.write("""Author: Manodnya Gaikwad'
    Email id: <manodnyagaikwad@csu.fullerton.edu>'
    'Github: '
    'LinkedIn: <https://www.linkedin.com/in/manodnya-gaikwad/>'
    """)
    
    
##streams per month graph

