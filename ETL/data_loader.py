import time
from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types,Row
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
cluster_seeds = ['199.60.17.188', '199.60.17.216']
# add more functions as necessary
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext


# Created KeySpace from CQLSH at node 199.60.17.188 using 
# CREATE  KEYSPACE IF NOT EXISTS StackExchange WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 } ;
#CREATE TABLE IF NOT EXISTS badges (id INT, userid TEXT, name TEXT, date TIMESTAMP, class INT, tagbased BOOLEAN, sites TEXT, PRIMARY KEY (id,userid,sites));
#CREATE TABLE IF NOT EXISTS tags (id INT, tagname TEXT, count INT, excerptpostid INT, wikipostid INT, sites TEXT, PRIMARY KEY (id,tagname,sites));
#CREATE TABLE IF NOT EXISTS comments (id INT, postId INT,score INT, text TEXT, creationDate TIMESTAMP, userId INT, sites TEXT, PRIMARY KEY (id,postid,sites));
#CREATE TABLE IF NOT EXISTS postlinks (id INT, postId INT,creationDate TIMESTAMP, relatedPostId INT, linkTypeId INT,sites TEXT, PRIMARY KEY (id,postid,sites));
#CREATE TABLE IF NOT EXISTS users (id INT, reputation INT,creationDate TIMESTAMP, displayName TEXT, lastAccessDate TIMESTAMP, websiteUrl TEXT,location TEXT,aboutme TEXT,views INT,upvotes INT,downvotes INT,accountid INT,sites TEXT, PRIMARY KEY (id,sites));
#CREATE TABLE IF NOT EXISTS votes (id INT, postId INT,voteTypeId INT, creationDate TIMESTAMP, sites TEXT, PRIMARY KEY (id,postid,sites));
#CREATE TABLE IF NOT EXISTS posts (id INT, postTypeId INT,ParentID INT,acceptedAnswerId INT,creationDate TIMESTAMP, score INT,viewCount INT, body TEXT, ownerUserId INT, lastEditorUserId INT, lastEditorDisplayName TEXT,
#lastEditDate TIMESTAMP,lastActivityDate TIMESTAMP, title TEXT, tags TEXT, answerCount INT, commentCount INT, favoriteCount INT, closedDate TIMESTAMP, sites TEXT, PRIMARY KEY (id,sites));
#CREATE TABLE IF NOT EXISTS posthistory (id INT, postId INT,score INT, text TEXT, creationDate TIMESTAMP, userId INT, sites TEXT, PRIMARY KEY (id,postid,userid,sites));

def main(inputs,keyspace,table_name):

    # load data
    df = spark.read.parquet(inputs)
    #Print number of rows in data
    print("No of rows in raw_dataset:",df.count())
    
    df.createOrReplaceTempView('raw_data')
    df = df.select([functions.col(x).alias(x.lower()) for x in df.columns]) 
    
    if table_name=='comments':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("postid").cast(types.IntegerType()),
                   functions.col("score").cast(types.IntegerType()),functions.col("text").cast(types.StringType()),
                   functions.col("creationdate").cast(types.TimestampType()),functions.col("userid").cast(types.IntegerType()),
                   functions.col("sites").cast(types.StringType()))
    if table_name=='votes':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("postid").cast(types.IntegerType()),
                   functions.col("creationdate").cast(types.TimestampType()),functions.col("votetypeid").cast(types.IntegerType()),
                   functions.col("sites").cast(types.StringType()))
    if table_name=='badges' :
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("userid").cast(types.IntegerType()),
                   functions.col("name").cast(types.StringType()),functions.col("date").cast(types.TimestampType()),
                   functions.col("class").cast(types.IntegerType()),functions.col("tagbased").cast(types.StringType()),
                   functions.col("sites").cast(types.StringType()))
    if table_name=='tags':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("tagname").cast(types.StringType()),
                   functions.col("count").cast(types.IntegerType()),functions.col("excerptpostid").cast(types.IntegerType()),
                   functions.col("wikipostid").cast(types.IntegerType()),functions.col("sites").cast(types.StringType()))
    if table_name=='users':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("reputation").cast(types.IntegerType()),
                   functions.col("creationdate").cast(types.TimestampType()),functions.col("displayname").cast(types.StringType()),
                   functions.col("lastaccessdate").cast(types.TimestampType()),functions.col("websiteurl").cast(types.StringType()),
                   functions.col("location").cast(types.StringType()),functions.col("aboutme").cast(types.StringType()),
                   functions.col("views").cast(types.IntegerType()),functions.col("upvotes").cast(types.IntegerType()),
                   functions.col("downvotes").cast(types.IntegerType()),functions.col("accountid").cast(types.IntegerType()),
                   functions.col("sites").cast(types.StringType()))    
    if table_name=='postlinks':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("postid").cast(types.IntegerType()),
                   functions.col("creationdate").cast(types.TimestampType()),functions.col("relatedpostid").cast(types.IntegerType()),
                   functions.col("linktypeid").cast(types.IntegerType()),functions.col("sites").cast(types.StringType())) 

    if table_name=='posts':
            df = df.select(functions.col("id").cast(types.IntegerType()),functions.col("posttypeid").cast(types.IntegerType()),
                   functions.col("parentid").cast(types.IntegerType()),functions.col("acceptedanswerid").cast(types.IntegerType()),
                   functions.col("creationdate").cast(types.TimestampType()),functions.col("score").cast(types.IntegerType()),
                   functions.col("viewcount").cast(types.IntegerType()),functions.col("body").cast(types.StringType()),
                   functions.col("owneruserid").cast(types.IntegerType()),functions.col("lasteditoruserid").cast(types.IntegerType()),
                   functions.col("lasteditordisplayname").cast(types.StringType()),functions.col("lasteditdate").cast(types.TimestampType()),
                   functions.col("lastactivitydate").cast(types.TimestampType()),functions.col("title").cast(types.StringType()),
                   functions.col("tags").cast(types.StringType()),functions.col("commentcount").cast(types.IntegerType()),
                   functions.col("answercount").cast(types.IntegerType()),functions.col("favoritecount").cast(types.IntegerType()),
                   functions.col("closeddate").cast(types.IntegerType()),functions.col("sites").cast(types.StringType()))
    
    df.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).save()
    print("Finished Loading Data to Cassandra")
    print("---Program ran for %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    start_time = time.time()
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs,keyspace,table_name)

##How to run this file?
#Sample command
#spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 data_loader.py /user/cmandava/Tags_parquet/ stackexchange tags

