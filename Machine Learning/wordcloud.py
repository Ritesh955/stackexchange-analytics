#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkConf, SparkContext
from pyspark.sql import functions, types,SparkSession, SQLContext
import math
from cassandra.cluster import Cluster,BatchStatement,ConsistencyLevel
import numpy as np
import pandas as pd
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
from PIL import Image

def cleanhtml(raw_html):
      a = "".join(raw_html)
      b = a.replace('<',"")
      c = b.replace('>'," ")
      return c

def main():
    posts = sqlc.read.format("org.apache.spark.sql.cassandra").options(table="posts", keyspace="stackexchange").load()
    data = posts.select('Tags','sites')
    main = data.groupBy("sites").agg(functions.collect_list("Tags").alias('Tags'))
    
    #Converts to pandas dataframe
    df = main.toPandas()
    
    #Converts the tags into required format
    df['Tags']=df['Tags'].apply(lambda x:cleanhtml(x))
    
    # Wordcloud for Sports Category
    sports = np.array(Image.open('/home/ashay/Downloads/tennis-player-and-ball.png'))
    
    text = df.Tags[6]
    # Create and generate a word cloud image:
    wordcloud = WordCloud(background_color="white", max_words=1000, mask=sports,
                   contour_width=6,colormap="magma").generate(text)
    # Display the generated image:
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.imsave('sport.png',wordcloud)
    plt.show()
    
    # WordCloud for Biology Category
    Biology = np.array(Image.open('/home/ashay/Downloads/pastedImage.png'))
    text = df.Tags[3]
    # Create and generate a word cloud image:
    wordcloud = WordCloud(background_color="white",max_words=2000, mask=Biology,
                   contour_width=6,colormap="viridis",mode="RGB").generate(text)
    # Display the generated image:
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.imsave('Biology.png',wordcloud)
    plt.show()

    # WordCloud for DataScience Category
    text = df.Tags[11]
    # Create and generate a word cloud image:
    wordcloud = WordCloud(background_color="Black",max_words=2000, mask=None,
                   contour_width=6,colormap="magma").generate(text)
    # Display the generated image:
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.imsave('Datascience.png',wordcloud)
    plt.show()


# In[ ]:
if __name__ == "__main__":
    cluster = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host',','.join(cluster)).getOrCreate()
    sc = spark.sparkContext
    sqlc = SQLContext(sc)
    main()

#Steps for running:
#Packages required : Matplotlib, WordCloud, PIL, Pandas, Numpy
#Connection with SFU Wifi as I am using cassandra connector with pyspark
#Commands to run
#spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 WordCloud.py
