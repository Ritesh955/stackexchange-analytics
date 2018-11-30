from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext


def main(inputs):
    votes = spark.read.parquet(inputs).repartition(96)

    #Avg upvote and downvote per site
    upvote = votes.filter(votes['VoteTypeId']==1)
    downvote = votes.filter(votes['VoteTypeId']==2)
    upvote_per_site = upvote.groupBy('sites').agg(functions.count('id').alias('upvote'))
    downvote_per_site = downvote.groupBy('sites').agg(functions.count('id').alias('downvote'))
    downvote_per_site = downvote_per_site.join(upvote_per_site,['sites'])
    downvote_per_site.show()
    downvote_per_site.write.csv('downvote_per_site',mode="overwrite")

    
    #Number of upvotes and downvotes per post
    upvote_per_post = upvote.groupBy('PostId').agg(functions.count('VotetypeId').alias('upvote_Per_Post'))
    downvote_per_post = downvote.groupBy('PostId').agg(functions.count('VotetypeId').alias('downvote_Per_Post'))
    upvote_per_post.show()
    downvote_per_post.show()
    up_down_per_post = upvote_per_post.join(downvote_per_post,["PostId"]).sort('upvote_Per_Post',ascending=False)
    up_down_per_post.show()
    up_down_per_post.write.csv('up_down_per_post',mode="overwrite")

if __name__ == "__main__":
    inputs = sys.argv[1]
    main(inputs)

#How to run this file?
#spark-submit votes_analytics.py /user/cmandava/Votes_parquet
