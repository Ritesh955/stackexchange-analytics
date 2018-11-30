from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
import matplotlib.pyplot as plt
#from PIL import Image
from pyspark.sql.functions import udf
from textblob import TextBlob
assert sys.version_info >= (3, 5)
 # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext


@udf('double')
def polarity(v):
  b = TextBlob(v).sentiment.polarity
  return b

@udf('string')
def sentiment_score(v):
  if(v>=0.5):
    return 'Very Positive'
  elif(v>=0 and v<0.5):
    return 'Positive'
  elif(v<0 and v>=-0.5):
    return 'Negative'
  else:
    return 'Very Negative'

def main(inputs):
    comments = spark.read.parquet(inputs).repartition(96)
    #Average score per sites
    comments = comments.select('Id','sites','Score','Text').cache()
    average_score = comments.groupBy('sites').agg(functions.avg('Score').alias('Avg_score_per_site'))
    average_score.show()
    
    #Calculating sentiment type across sites
    comments = comments.withColumn('Polarity', polarity(comments.Text))
    comments.show()

    comments = comments.withColumn('sentiment', sentiment_score(comments.Polarity))
    comments.show()
    comments = comments.select('Id','sites','sentiment')
    
    #comments = comments.groupBy('sites','sentiment').agg(functions.count('sentiment').alias('sentiment_count'))
    #comments.show()
    positive_comments = comments.filter(comments['sentiment']=='Positive')
    negative_comments = comments.filter(comments['sentiment']=='Negative')
    very_positive_comments = comments.filter(comments['sentiment']=='Very Positive')
    very_negative_comments = comments.filter(comments['sentiment']=='Very Negative')
    
    # # PostiveComments
    positive_comments = positive_comments.groupBy('sites').agg(functions.count('sentiment').alias('positive_count'))
    positive_comments.show()
    
    # # NegativeComments
    negative_comments = negative_comments.groupBy('sites').agg(functions.count('sentiment').alias('negetive_count'))
    negative_comments.show()
    
    # # VeryPositiveComments
    very_positive_comments=very_positive_comments.groupBy('sites').agg(functions.count('sentiment').alias('very_positive_count'))
    very_positive_comments.show()
    
    # # VeryNegativeComments
    very_negative_comments=very_negative_comments.groupBy('sites').agg(functions.count('sentiment').alias('very_negative_count'))
    very_negative_comments.show()
    
    #df = sentiment.toPandas()
    #df.plot(x='sites',kind='bar')
    
if __name__ == "__main__":
    inputs = sys.argv[1]
    main(inputs)

#How to run this file?
# Packages required : textblob
# spark-submit comments_analytics.py /user/cmandava/Comments_parquet



